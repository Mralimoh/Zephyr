package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"time"
)

type oauthClientJSON struct {
	Installed struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		AuthURI      string   `json:"auth_uri"`
		RedirectURIs []string `json:"redirect_uris"`
	} `json:"installed"`
}

type tokenCache struct {
	RefreshToken string `json:"refresh_token"`
}

type googleOpKind int

const (
	gOpGetToken googleOpKind = iota
	gOpGetFileID
	gOpUpdateFileID
	gOpDeleteFileID
)

type googleOp struct {
	kind     googleOpKind
	filename string
	fileID   string
	respStr  chan string
}

type GoogleBackend struct {
	httpClient *http.Client
	saPath     string
	folderID   string

	clientID     string
	clientSecret string
	tokenURI     string
	redirectURI  string

	token        string
	refreshToken string
	tokenEx      time.Time
	fileIDs      map[string]string

	managerChan chan googleOp
}

func NewGoogleBackend(client *http.Client, saPath, folderID string) *GoogleBackend {
	return &GoogleBackend{
		httpClient:  client,
		saPath:      saPath,
		folderID:    folderID,
		fileIDs:     make(map[string]string),
		managerChan: make(chan googleOp, 100),
	}
}

func (b *GoogleBackend) Login(ctx context.Context) error {
	data, err := os.ReadFile(b.saPath)
	if err != nil {
		return fmt.Errorf("failed to read Client Secret JSON %s: %w", b.saPath, err)
	}
	var oauthJSON oauthClientJSON
	if err := json.Unmarshal(data, &oauthJSON); err != nil {
		return fmt.Errorf("failed to unmarshal Client Secret JSON: %w", err)
	}

	b.clientID = oauthJSON.Installed.ClientID
	b.clientSecret = oauthJSON.Installed.ClientSecret
	b.tokenURI = "https://www.googleapis.com/oauth2/v4/token"
	authURI := oauthJSON.Installed.AuthURI
	if len(oauthJSON.Installed.RedirectURIs) > 0 {
		b.redirectURI = oauthJSON.Installed.RedirectURIs[0]
	} else {
		b.redirectURI = "http://localhost"
	}

	tokenCachePath := b.saPath + ".token"
	if cacheData, err := os.ReadFile(tokenCachePath); err == nil {
		var cache tokenCache
		if err := json.Unmarshal(cacheData, &cache); err == nil && cache.RefreshToken != "" {
			b.refreshToken = cache.RefreshToken
			if err := b.refreshAccessToken(ctx); err == nil {
				go b.runManager(ctx) // استارت مدیر پس از بازیابی توکن
				return nil
			}
		}
	}

	link := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=code&scope=https://www.googleapis.com/auth/drive.file&access_type=offline",
		authURI, url.QueryEscape(b.clientID), url.QueryEscape(b.redirectURI))

	fmt.Printf("\n==================== OAUTH AUTHENTICATION REQUIRED ====================\n")
	fmt.Printf("1. Please open this URL in your web browser:\n\n%s\n\n", link)
	fmt.Printf("4. Paste the FULL redirected URL or Code: ")

	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	code := input
	if strings.HasPrefix(input, "http") {
		u, _ := url.Parse(input)
		code = u.Query().Get("code")
	}

	if code == "" {
		return fmt.Errorf("invalid authorization code")
	}

	if err := b.exchangeCode(ctx, code); err != nil {
		return err
	}

	cache := tokenCache{RefreshToken: b.refreshToken}
	cacheBytes, _ := json.MarshalIndent(cache, "", "  ")
	_ = os.WriteFile(tokenCachePath, cacheBytes, 0600)

	fmt.Printf("OAuth Authentication Successful!\n=======================================================================\n\n")
	
	go b.runManager(ctx)
	return nil
}

func (b *GoogleBackend) exchangeCode(ctx context.Context, code string) error {
	v := url.Values{}
	v.Set("grant_type", "authorization_code")
	v.Set("code", code)
	v.Set("client_id", b.clientID)
	v.Set("client_secret", b.clientSecret)
	v.Set("redirect_uri", b.redirectURI)
	return b.executeTokenRequest(ctx, v)
}

func (b *GoogleBackend) refreshAccessToken(ctx context.Context) error {
	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", b.refreshToken)
	v.Set("client_id", b.clientID)
	v.Set("client_secret", b.clientSecret)
	return b.executeTokenRequest(ctx, v)
}

func (b *GoogleBackend) executeTokenRequest(ctx context.Context, v url.Values) error {
	req, err := http.NewRequestWithContext(ctx, "POST", b.tokenURI, strings.NewReader(v.Encode()))
	if err != nil { return err }
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.httpClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()

	var resData struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil { return err }

	b.token = resData.AccessToken
	if resData.RefreshToken != "" {
		b.refreshToken = resData.RefreshToken
	}
	b.tokenEx = time.Now().Add(time.Duration(resData.ExpiresIn-60) * time.Second)
	return nil
}

func (b *GoogleBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	resp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetToken, respStr: resp}
	tok := <-resp

	meta := map[string]interface{}{"name": filename}
	if b.folderID != "" {
		meta["parents"] = []string{b.folderID}
	}
	metaBytes, _ := json.Marshal(meta)

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		var goroutineErr error
		defer func() {
			mw.Close()
			pw.CloseWithError(goroutineErr)
		}()
		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/json; charset=UTF-8")
		part1, _ := mw.CreatePart(h)
		part1.Write(metaBytes)
		h = make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/octet-stream")
		part2, _ := mw.CreatePart(h)
		io.Copy(part2, data)
	}()

	req, _ := http.NewRequestWithContext(ctx, "POST", "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", pr)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	respHTTP, err := b.httpClient.Do(req)
	if err != nil { return err }
	defer respHTTP.Body.Close()
	if respHTTP.StatusCode < 200 || respHTTP.StatusCode >= 300 {
		return fmt.Errorf("upload rejected: %d", respHTTP.StatusCode)
	}
	return nil
}

func (b *GoogleBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	resp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetToken, respStr: resp}
	tok := <-resp

	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	q := fmt.Sprintf("name contains '%s' and trashed = false", prefix)
	if b.folderID != "" {
		q += fmt.Sprintf(" and '%s' in parents", b.folderID)
	}
	v := u.Query()
	v.Set("q", q)
	v.Set("fields", "files(id, name)")
	u.RawQuery = v.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	
	respHTTP, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer respHTTP.Body.Close()

	var rd struct {
		Files []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"files"`
	}
	json.NewDecoder(respHTTP.Body).Decode(&rd)

	var names []string
	for _, f := range rd.Files {
		if strings.HasPrefix(f.Name, prefix) {
			b.managerChan <- googleOp{kind: gOpUpdateFileID, filename: f.Name, fileID: f.ID}
			names = append(names, f.Name)
		}
	}
	return names, nil
}

func (b *GoogleBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	idResp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetFileID, filename: filename, respStr: idResp}
	fileID := <-idResp
	if fileID == "" { return nil, fmt.Errorf("file-id not found") }

	tokResp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetToken, respStr: tokResp}
	tok := <-tokResp

	req, _ := http.NewRequestWithContext(ctx, "GET", "https://www.googleapis.com/drive/v3/files/"+fileID+"?alt=media", nil)
	req.Header.Set("Authorization", "Bearer "+tok)

	respHTTP, err := b.httpClient.Do(req)
	if err != nil { return nil, err }
	if respHTTP.StatusCode != http.StatusOK {
		respHTTP.Body.Close()
		return nil, fmt.Errorf("download failed: %d", respHTTP.StatusCode)
	}
	return respHTTP.Body, nil
}

func (b *GoogleBackend) Delete(ctx context.Context, filename string) error {
	idResp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetFileID, filename: filename, respStr: idResp}
	fileID := <-idResp
	if fileID == "" { return nil }

	tokResp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetToken, respStr: tokResp}
	tok := <-tokResp

	req, _ := http.NewRequestWithContext(ctx, "DELETE", "https://www.googleapis.com/drive/v3/files/"+fileID, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	respHTTP, err := b.httpClient.Do(req)
	if err != nil { return err }
	respHTTP.Body.Close()

	b.managerChan <- googleOp{kind: gOpDeleteFileID, filename: filename}
	return nil
}

func (b *GoogleBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	tokResp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetToken, respStr: tokResp}
	tok := <-tokResp

	meta := map[string]interface{}{
		"name":     name,
		"mimeType": "application/vnd.google-apps.folder",
	}
	if b.folderID != "" { meta["parents"] = []string{b.folderID} }
	body, _ := json.Marshal(meta)

	req, _ := http.NewRequestWithContext(ctx, "POST", "https://www.googleapis.com/drive/v3/files?fields=id", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")

	respHTTP, err := b.httpClient.Do(req)
	if err != nil { return "", err }
	defer respHTTP.Body.Close()

	var resData struct { ID string `json:"id"` }
	json.NewDecoder(respHTTP.Body).Decode(&resData)
	return resData.ID, nil
}

func (b *GoogleBackend) FindFolder(ctx context.Context, name string) (string, error) {
	tokResp := make(chan string, 1)
	b.managerChan <- googleOp{kind: gOpGetToken, respStr: tokResp}
	tok := <-tokResp

	q := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and trashed = false", name)
	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	v := u.Query()
	v.Set("q", q)
	v.Set("fields", "files(id)")
	u.RawQuery = v.Encode()

	req, _ := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	req.Header.Set("Authorization", "Bearer "+tok)

	respHTTP, err := b.httpClient.Do(req)
	if err != nil { return "", err }
	defer respHTTP.Body.Close()

	var resData struct { Files []struct { ID string `json:"id"` } `json:"files"` }
	json.NewDecoder(respHTTP.Body).Decode(&resData)
	if len(resData.Files) > 0 { return resData.Files[0].ID, nil }
	return "", nil
}

func (b *GoogleBackend) runManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-b.managerChan:
			switch op.kind {
			case gOpGetToken:
				if time.Now().After(b.tokenEx) && b.refreshToken != "" {
					_ = b.refreshAccessToken(ctx)
				}
				op.respStr <- b.token

			case gOpGetFileID:
				op.respStr <- b.fileIDs[op.filename]

			case gOpUpdateFileID:
				b.fileIDs[op.filename] = op.fileID

			case gOpDeleteFileID:
				delete(b.fileIDs, op.filename)
			}
		}
	}
}