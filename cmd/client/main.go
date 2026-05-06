package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"encoding/binary"
	"fmt"

	"Zephyr/internal/config"
	"Zephyr/internal/httpclient"
	"Zephyr/internal/storage"
	"Zephyr/internal/transport"
	"github.com/things-go/go-socks5"
)

func generateSessionID() string {
	b := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		log.Fatalf("Critical Error: Failed to generate random session ID: %v", err)
	}
	return hex.EncodeToString(b)
}

type StorageClient interface {
	Login(ctx context.Context) error
	FindFolder(ctx context.Context, name string) (string, error)
	CreateFolder(ctx context.Context, name string) (string, error)
	transport.Datastore
}

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google Service Account JSON")
	flag.Parse()

	log.Println("Starting Zephyr Client...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	var backend StorageClient
	if appCfg.StorageType == "google" {
		customHttpClient := httpclient.NewCustomClient(appCfg.Transport)
		backend = storage.NewGoogleBackend(customHttpClient, gcPath, appCfg.GoogleFolderID)
	} else {
		backend, err = storage.NewLocalBackend(appCfg.LocalDir)
		if err != nil {
			log.Fatalf("Failed to init local storage: %v", err)
		}
	}
	if err := backend.Login(ctx); err != nil {
		log.Fatalf("Backend login failed: %v", err)
	}

	if appCfg.StorageType == "google" && appCfg.GoogleFolderID == "" {
		log.Println("Zero-Config: Searching for existing Google Drive folder 'Zephyr-Data'...")
		folderID, err := backend.FindFolder(ctx, "Zephyr-Data")
		if err != nil {
			log.Fatalf("Failed to search for folder: %v", err)
		}

		if folderID == "" {
			log.Println("Zero-Config: 'Zephyr-Data' not found. Creating new folder...")
			folderID, err = backend.CreateFolder(ctx, "Zephyr-Data")
			if err != nil {
				log.Fatalf("Failed to auto-create folder: %v", err)
			}
		} else {
			log.Printf("Zero-Config: Found existing folder with ID %s", folderID)
		}

		appCfg.GoogleFolderID = folderID
		if err := appCfg.Save(configPath); err != nil {
			log.Printf("Warning: Failed to save folder ID to %s: %v", configPath, err)
		} else {
			log.Printf("Zero-Config: Config updated with folder ID %s", folderID)
		}
	}

	cid := appCfg.ClientID
	if cid == "" {
		cid = generateSessionID()[:8]
	}
	engine := transport.NewEngine(backend, true, cid)
	if appCfg.RefreshRateMs > 0 {
		engine.SetPollRate(appCfg.RefreshRateMs)
	}
	if appCfg.FlushRateMs > 0 {
		engine.SetFlushRate(appCfg.FlushRateMs)
	}
	engine.Start(ctx)

	listenAddr := appCfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	fdns := newFakeDNS()

	server := socks5.NewServer(
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				log.Printf("[SOCKS5] Error: invalid target address format '%s': %v", addr, err)
				return nil, fmt.Errorf("invalid address format: %w", err)
			}
			
			realHost, isFake := fdns.GetHostname(host)
			targetAddr := addr
			if isFake {
				targetAddr = net.JoinHostPort(realHost, port)
				log.Printf("[Dial] Un-faking: %s -> %s", host, realHost)
			}

			sessionID := generateSessionID()[:16]
			session := transport.NewSession(sessionID)
			session.TargetAddr = targetAddr
			engine.AddSession(session)

			return transport.NewVirtualConn(session, engine), nil
		}),

		socks5.WithResolver(rawResolver{fdns: fdns}),
	)

	log.Printf("Listening for SOCKS5 on %s...", listenAddr)

	go func() {
		if err := server.ListenAndServe("tcp", listenAddr); err != nil {
			log.Fatalf("SOCKS5 server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down client...")
	cancel()
}

type fakeDNS struct {
	mu       sync.RWMutex
	table    map[string]string
	revTable map[string]string
	nextIP   uint32
}

func newFakeDNS() *fakeDNS {
	return &fakeDNS{
		table:    make(map[string]string),
		revTable: make(map[string]string),
		nextIP:   0x0A000001,
	}
}

func (f *fakeDNS) GetIP(hostname string) net.IP {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ipStr, ok := f.revTable[hostname]; ok {
		return net.ParseIP(ipStr)
	}

	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, f.nextIP)
	ipStr := ip.String()

	f.table[ipStr] = hostname
	f.revTable[hostname] = ipStr
	f.nextIP++
	if f.nextIP > 0x0AFFFFFF {
		f.nextIP = 0x0A000001
	}

	return ip
}

func (f *fakeDNS) GetHostname(ip string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	host, ok := f.table[ip]
	return host, ok
}

type rawResolver struct {
	fdns *fakeDNS
}

func (r rawResolver) Resolve(ctx context.Context, name string) (context.Context, net.IP, error) {
	fakeIP := r.fdns.GetIP(name)
	log.Printf("[DNS] Fake-IP: %s -> %s", name, fakeIP.String())
	return ctx, fakeIP, nil
}