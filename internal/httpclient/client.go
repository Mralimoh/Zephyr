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
	TargetIP string

	SNI string

	HostHeader string

	InsecureSkipVerify bool
}

type hostRewriteTransport struct {
	Transport  http.RoundTripper
	HostHeader string
}

func (t *hostRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasPrefix(req.URL.Path, "/macros/") || strings.Contains(req.URL.Host, "googleusercontent") {
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

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cfg.TargetIP != "" {
				return dialer.DialContext(ctx, "tcp", cfg.TargetIP)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ServerName:         cfg.SNI,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
	}

	var rt http.RoundTripper = transport
	if cfg.HostHeader != "" {
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