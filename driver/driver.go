package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type bigQueryDriver struct {
}

type bigQueryConfig struct {
	projectID string
	location  string
	dataSet   string
	scopes    []string
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

	client, err := bigquery.NewClient(ctx, config.projectID, option.WithScopes(config.scopes...))
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

	patternDatasetOnly := "/?*"
	matchDatasetOnly, err := path.Match(patternDatasetOnly, u.Path)
	if err != nil {
		return nil, err
	}

	patternLocationDataset := "/*/*"
	matchLocationDataset, err := path.Match(patternLocationDataset, u.Path)
	if err != nil {
		return nil, err
	}

	if !matchDatasetOnly && !matchLocationDataset {
		return nil, fmt.Errorf("invalid connection string: %s", uri)
	}

	location, dataset := path.Split(u.Path)
	location = strings.Trim(location, "/")
	scopes := strings.Split(u.Query().Get("scopes"), ",")

	return &bigQueryConfig{
		projectID: u.Hostname(),
		dataSet:   dataset,
		location:  location,
		scopes:    scopes,
	}, nil
}
