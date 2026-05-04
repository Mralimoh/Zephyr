package storage

import (
	"context"
	"io"
)

type Backend interface {
	Login(ctx context.Context) error
	Upload(ctx context.Context, filename string, data io.Reader) error
	ListQuery(ctx context.Context, prefix string) ([]string, error)
	Download(ctx context.Context, filename string) (io.ReadCloser, error)
	Delete(ctx context.Context, filename string) error
	CreateFolder(ctx context.Context, name string) (string, error)
	FindFolder(ctx context.Context, name string) (string, error)
}
