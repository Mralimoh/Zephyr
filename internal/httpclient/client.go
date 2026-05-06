package httpclient

import (
	"net/http"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

type TransportConfig struct {
	TargetIP           string
	SNI                string
	HostHeader         string
	InsecureSkipVerify bool
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type clientWrapper struct {
	client tlsclient.HttpClient
}

func (w *clientWrapper) Do(req *http.Request) (*http.Response, error) {
	fReq, err := fhttp.NewRequest(req.Method, req.URL.String(), req.Body)
	if err != nil {
		return nil, err
	}
	fReq.Header = make(fhttp.Header)
	for k, vv := range req.Header {
		for _, v := range vv {
			fReq.Header.Add(k, v)
		}
	}

	fResp, err := w.client.Do(fReq)
	if err != nil {
		return nil, err
	}

	resp := &http.Response{
		Status:     fResp.Status,
		StatusCode: fResp.StatusCode,
		Proto:      fResp.Proto,
		ProtoMajor: fResp.ProtoMajor,
		ProtoMinor: fResp.ProtoMinor,
		Header:     make(http.Header),
		Body:       fResp.Body,
		ContentLength: fResp.ContentLength,
	}

	for k, vv := range fResp.Header {
		for _, v := range vv {
			resp.Header.Add(k, v)
		}
	}

	return resp, nil
}

func NewCustomClient(cfg TransportConfig) HTTPClient {
	options := []tlsclient.HttpClientOption{
		tlsclient.WithClientProfile(profiles.Chrome_120),
		tlsclient.WithNotFollowRedirects(),
	}

	if cfg.InsecureSkipVerify {
		options = append(options, tlsclient.WithInsecureSkipVerify())
	}
	if cfg.SNI != "" {
		options = append(options, tlsclient.WithServerNameOverwrite(cfg.SNI))
	}

	client, err := tlsclient.NewHttpClient(tlsclient.NewNoopLogger(), options...)
	if err != nil {
		return &http.Client{}
	}

	return &clientWrapper{client: client}
}