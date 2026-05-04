package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LocalBackend struct {
	baseDir string
}

func NewLocalBackend(baseDir string) (*LocalBackend, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base dir %s: %w", baseDir, err)
	}
	return &LocalBackend{baseDir: baseDir}, nil
}

func (b *LocalBackend) Login(ctx context.Context) error {
	info, err := os.Stat(b.baseDir)
	if err != nil {
		return fmt.Errorf("local backend dir not found: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local backend path %s is not a directory", b.baseDir)
	}
	return nil
}

func (b *LocalBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	path := filepath.Join(b.baseDir, filename)
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	
	if _, err := io.Copy(f, data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write data: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}
	return nil
}

func (b *LocalBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	entries, err := os.ReadDir(b.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read dir: %w", err)
	}

	var results []string
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		if prefix == "" || strings.HasPrefix(entry.Name(), prefix) {
			results = append(results, entry.Name())
		}
	}
	return results, nil
}

func (b *LocalBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	path := filepath.Join(b.baseDir, filename)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", filename)
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return file, nil
}

func (b *LocalBackend) Delete(ctx context.Context, filename string) error {
	path := filepath.Join(b.baseDir, filename)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

func (b *LocalBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	path := filepath.Join(b.baseDir, name)
	if err := os.MkdirAll(path, 0755); err != nil {
		return "", err
	}
	return name, nil
}

func (b *LocalBackend) FindFolder(ctx context.Context, name string) (string, error) {
	path := filepath.Join(b.baseDir, name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	if info.IsDir() {
		return name, nil
	}
	return "", nil
}
