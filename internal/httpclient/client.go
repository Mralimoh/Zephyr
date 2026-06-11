package httpclient

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
	"strings"
)

type TransportConfig struct {
	EnableDomainFronting bool
	TargetIP             string
	SNI                  string
	HostHeader           string
	InsecureSkipVerify   bool
}

type hostRewriteTransport struct {
	Transport  http.RoundTripper
	HostHeader string
}

func (t *hostRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasPrefix(req.URL.Path, "/macros/") {
		req.Host = req.URL.Host
		return t.Transport.RoundTrip(req)
	}

	if t.HostHeader != "" && req.Host == "" {
		req.Host = t.HostHeader
	}
	
	return t.Transport.RoundTrip(req)
}

func NewCustomClient(cfg TransportConfig) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 60 * time.Second,
	}

	dialContext := dialer.DialContext
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.EnableDomainFronting {
		if cfg.SNI != "" {
			tlsConfig.ServerName = cfg.SNI
		}
		if cfg.TargetIP != "" {
			dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp", cfg.TargetIP)
			}
		}
	}

	transport := &http.Transport{
		DialContext:           dialContext,
		TLSClientConfig:       tlsConfig,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
	}

	var rt http.RoundTripper = transport
	
	if cfg.EnableDomainFronting && cfg.HostHeader != "" {
		rt = &hostRewriteTransport{
			Transport:  transport,
			HostHeader: cfg.HostHeader,
		}
	}

	return &http.Client{
		Transport: rt,
		Timeout:   60 * time.Second,
	}
}