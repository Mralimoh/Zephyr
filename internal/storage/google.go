package storage

import (
	"bufio"
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
	"sync"
	"time"
)

type oauthClientJSON struct {
	Installed struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		AuthURI      string   `json:"auth_uri"`
		TokenURI     string   `json:"token_uri"`
		RedirectURIs []string `json:"redirect_uris"`
	} `json:"installed"`
}

type tokenCache struct {
	RefreshToken string `json:"refresh_token"`
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
	mu           sync.Mutex

	fileIDs     map[string]string
	fileIDsRing []string
	fileIDsIdx  int
	fileIdsMu   sync.RWMutex
}

func NewGoogleBackend(client *http.Client, saPath, folderID string) *GoogleBackend {
	return &GoogleBackend{
		httpClient:  client,
		saPath:      saPath,
		folderID:    folderID,
		fileIDs:     make(map[string]string),
		fileIDsRing: make([]string, 256),
	}
}

func (b *GoogleBackend) Login(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

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
			return b.refreshAccessToken(ctx)
		}
	}

	link := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=code&scope=https://www.googleapis.com/auth/drive.file&access_type=offline",
		authURI, url.QueryEscape(b.clientID), url.QueryEscape(b.redirectURI))

	fmt.Printf("\n==================== OAUTH AUTHENTICATION REQUIRED ====================\n")
	fmt.Printf("1. Please open this URL in your web browser:\n\n%s\n\n", link)
	fmt.Printf("2. Authenticate and accept the permissions.\n")
	fmt.Printf("3. The browser will redirect to something like %s/?code=4/1AX4X...\n", b.redirectURI)
	fmt.Printf("   (It's okay if the browser says 'Unable to connect' or 'Site can't be reached')\n")
	fmt.Printf("4. Please copy the FULL redirected URL from your browser's address bar and paste it below:\n")
	fmt.Printf("\nEnter URL or Code: ")

	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	code := input
	if strings.HasPrefix(input, "http") {
		u, err := url.Parse(input)
		if err == nil {
			qCode := u.Query().Get("code")
			if qCode != "" {
				code = qCode
			}
		}
	}

	if code == "" {
		return fmt.Errorf("invalid authorization code")
	}

	fmt.Printf("Trading code for tokens via domain-fronted proxy mapping...\n")
	if err := b.exchangeCode(ctx, code); err != nil {
		return err
	}

	cache := tokenCache{RefreshToken: b.refreshToken}
	cacheBytes, _ := json.MarshalIndent(cache, "", "  ")
	if err := os.WriteFile(tokenCachePath, cacheBytes, 0600); err != nil {
		fmt.Printf("WARNING: Failed to save refresh token to %s: %v\n", tokenCachePath, err)
	} else {
		fmt.Printf("Saved refresh token to %s. Future startups will be silent.\n", tokenCachePath)
	}

	fmt.Printf("OAuth Authentication Successful!\n=======================================================================\n\n")
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
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("token request returned %d: %s", resp.StatusCode, string(body))
	}

	var resData struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	b.token = resData.AccessToken
	if resData.RefreshToken != "" {
		b.refreshToken = resData.RefreshToken
	}
	b.tokenEx = time.Now().Add(time.Duration(resData.ExpiresIn-60) * time.Second)
	return nil
}

func (b *GoogleBackend) getValidToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if time.Now().After(b.tokenEx) {
		if err := b.refreshAccessToken(ctx); err != nil {
			b.tokenEx = time.Now().Add(5 * time.Second)
			return "", err
		}
	}
	return b.token, nil
}

func (b *GoogleBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return fmt.Errorf("getting valid token for upload: %w", err)
	}

	var metaStr string
	if b.folderID != "" {
		metaStr = `{"name":"` + filename + `","parents":["` + b.folderID + `"]}`
	} else {
		metaStr = `{"name":"` + filename + `"}`
	}
	metaBytes := []byte(metaStr)

	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)

	go func() {
		var goroutineErr error
		defer func() {
			mw.Close()
			if goroutineErr == nil {
				pw.Close()
			} else {
				pw.CloseWithError(goroutineErr)
			}
		}()

		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/json; charset=UTF-8")
		part1, err := mw.CreatePart(h)
		if err != nil {
			goroutineErr = fmt.Errorf("creating metadata part: %w", err)
			return
		}
		
		if _, err := part1.Write(metaBytes); err != nil {
			goroutineErr = fmt.Errorf("writing metadata part: %w", err)
			return
		}

		h = make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/octet-stream")
		part2, err := mw.CreatePart(h)
		if err != nil {
			goroutineErr = fmt.Errorf("creating data part: %w", err)
			return
		}
		
		if _, err := io.Copy(part2, data); err != nil {
			goroutineErr = fmt.Errorf("copying data to multipart: %w", err)
			return
		}
	}()

	reqURL := "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id"
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, pr)
	if err != nil {
		pr.Close()
		return fmt.Errorf("creating http request: %w", err)
	}
	
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("upload rejected with status %d: %s", resp.StatusCode, string(body))
}

func (b *GoogleBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return nil, err
	}

	q := "name contains '" + prefix + "' and trashed = false"
	if b.folderID != "" {
		q += " and '" + b.folderID + "' in parents"
	}

	reqURL := "https://www.googleapis.com/drive/v3/files?spaces=drive&orderBy=createdTime+asc&fields=files(id,name)&q=" + url.QueryEscape(q)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list returned %d: %s", resp.StatusCode, string(body))
	}

	var resData struct {
		Files []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return nil, err
	}

	b.fileIdsMu.Lock()
	var names []string
	for _, f := range resData.Files {
		if strings.HasPrefix(f.Name, prefix) {
			if _, exists := b.fileIDs[f.Name]; !exists {
				oldest := b.fileIDsRing[b.fileIDsIdx]
				if oldest != "" {
					delete(b.fileIDs, oldest)
				}
				b.fileIDs[f.Name] = f.ID
				b.fileIDsRing[b.fileIDsIdx] = f.Name
				b.fileIDsIdx = (b.fileIDsIdx + 1) % 256
			}
			names = append(names, f.Name)
		}
	}
	b.fileIdsMu.Unlock()

	return names, nil
}

func (b *GoogleBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	b.fileIdsMu.RLock()
	fileID, ok := b.fileIDs[filename]
	b.fileIdsMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("file-id mapping not found for %s", filename)
	}

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return nil, err
	}

	reqURL := "https://www.googleapis.com/drive/v3/files/" + fileID + "?alt=media"
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("download returned %d: %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}

func (b *GoogleBackend) Delete(ctx context.Context, filename string) error {
	b.fileIdsMu.RLock()
	fileID, ok := b.fileIDs[filename]
	b.fileIdsMu.RUnlock()

	if !ok {
		return nil 
	}

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}

	reqURL := "https://www.googleapis.com/drive/v3/files/" + fileID
	req, err := http.NewRequestWithContext(ctx, "DELETE", reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete returned %d: %s", resp.StatusCode, string(body))
	}

	io.Copy(io.Discard, resp.Body)

	b.fileIdsMu.Lock()
	delete(b.fileIDs, filename)
	b.fileIdsMu.Unlock()

	return nil
}

func (b *GoogleBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	existingID, err := b.FindFolder(ctx, name)
	if err == nil && existingID != "" {
		return existingID, nil
	}

	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}

	metaStr := `{"name":"` + name + `","mimeType":"application/vnd.google-apps.folder"}`
	if b.folderID != "" {
		metaStr = `{"name":"` + name + `","mimeType":"application/vnd.google-apps.folder","parents":["` + b.folderID + `"]}`
	}

	reqURL := "https://www.googleapis.com/drive/v3/files?fields=id"
	
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, strings.NewReader(metaStr))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		resBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create folder returned %d: %s", resp.StatusCode, string(resBody))
	}

	var resData struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return "", err
	}

	b.folderID = resData.ID
	return resData.ID, nil
}

func (b *GoogleBackend) FindFolder(ctx context.Context, name string) (string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}

	q := "name = '" + name + "' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
	
	reqURL := "https://www.googleapis.com/drive/v3/files?spaces=drive&fields=files(id)&q=" + url.QueryEscape(q)

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("find folder returned %d: %s", resp.StatusCode, string(body))
	}

	var resData struct {
		Files []struct {
			ID string `json:"id"`
		} `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&resData); err != nil {
		return "", err
	}

	if len(resData.Files) > 0 {
		b.folderID = resData.Files[0].ID
		return resData.Files[0].ID, nil
	}
	return "", nil
}

func (b *GoogleBackend) UploadViaGAS(ctx context.Context, gasURL, gasKey, clientID string, data io.Reader) error {
	reqURL := gasURL + "?key=" + gasKey + "&id=" + clientID

	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, data)
	if err != nil {
		return fmt.Errorf("creating GAS request: %w", err)
	}

	host := ""
	if strings.HasPrefix(gasURL, "https://") {
		host = gasURL[8:]
	} else if strings.HasPrefix(gasURL, "http://") {
		host = gasURL[7:]
	}
	if idx := strings.IndexByte(host, '/'); idx != -1 {
		host = host[:idx]
	}

	req.Host = host
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing GAS request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GAS status %d", resp.StatusCode)
	}

	io.Copy(io.Discard, resp.Body)
	return nil
}