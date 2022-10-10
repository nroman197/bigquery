package driver

import (
	"cloud.google.com/go/bigquery"
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
)

type bigQueryDriver struct {
}

type bigQueryConfig struct {
	projectID string
	location  string
	dataSet   string
}

func (b bigQueryDriver) Open(uri string) (driver.Conn, error) {

	if uri == "scanner" {
		return &scannerConnection{}, nil
	}

	config, err := configFromUri(uri)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, config.projectID)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnection{
		ctx:    ctx,
		client: client,
		config: *config,
	}, nil
}

func configFromUri(uri string) (*bigQueryConfig, error) {
	if !strings.HasPrefix(uri, "bigquery://") {
		return nil, fmt.Errorf("invalid prefix, expected bigquery:// got: %s", uri)
	}

	uri = strings.ToLower(uri)
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %q", uri)
	}

	path := strings.TrimPrefix(u.Path, "/")
	if path == "" {
		return nil, fmt.Errorf("invalid connection string: %s", uri)
	}

	fields := strings.Split(path, "/")
	if len(fields) > 2 {
		return nil, fmt.Errorf("invalid connection string: %s", uri)
	}

	config := &bigQueryConfig{
		projectID: u.Hostname(),
		dataSet:   fields[len(fields)-1],
	}

	if len(fields) == 2 {
		config.location = fields[0]
	}

	return config, nil
}
