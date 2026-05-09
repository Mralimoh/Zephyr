package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"Zephyr/internal/config"
	"Zephyr/internal/httpclient"
	"Zephyr/internal/storage"
	"Zephyr/internal/transport"
)

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google Service Account JSON")
	flag.Parse()

	log.Println("Starting Zephyr Server...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	customHttpClient := httpclient.NewCustomClient(appCfg.Transport)
	backend := storage.NewGoogleBackend(customHttpClient, gcPath, appCfg.GoogleFolderID)

	if err := backend.Login(ctx); err != nil {
		log.Fatalf("Backend login failed: %v", err)
	}

	if appCfg.GoogleFolderID == "" {
		log.Println("Zero-Config: Searching for folder 'Zephyr-Data'...")
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
		}

		appCfg.GoogleFolderID = folderID
		if err := appCfg.Save(configPath); err != nil {
			log.Printf("Warning: Failed to save folder ID: %v", err)
		}
	}

	engine := transport.NewEngine(backend, false, "")

	engine.OnNewSession = func(sessionID, targetAddr string, session *transport.Session) {
		log.Printf("Server received new session %s destined for %s", sessionID, targetAddr)
		go handleServerConn(sessionID, targetAddr, session, engine)
	}

	engine.Start(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down server...")
	cancel()
}

func handleServerConn(sessionID, targetAddr string, session *transport.Session, engine *transport.Engine) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(session.Ctx, "tcp", targetAddr)
	if err != nil {
		log.Printf("Dial error to %s: %v", targetAddr, err)
		session.Close()
		return
	}
	defer conn.Close()

	vConn := transport.NewVirtualConn(session, engine)
	defer vConn.Close()

	errCh := make(chan error, 2)

	go func() {
		_, err := io.Copy(conn, vConn)
		errCh <- err
	}()

	go func() {
		_, err := io.Copy(vConn, conn)
		errCh <- err
	}()

	select {
	case <-errCh:
	case <-session.Ctx.Done():
	}
}