// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package hive

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"

	"iter"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive"
)

// HiveCatalog implements the Catalog interface backed by a Hive Metastore.
type HiveCatalog struct {
	// Address of the Hive Metastore service.
	metastoreURI string

	// Connection options for the metastore.
	options iceberg.Properties

	// gohive client used to communicate with the metastore.
	client *gohive.Connection

	// config keeps the connection details to allow automatic reconnection.
	cfg Config

	// mu guards client reconnection.
	mu sync.Mutex
}

// Config holds connection parameters for the Hive metastore.
// Only a subset of gohive.ConnectConfiguration options are exposed here for
// simplicity. Users needing more control can modify the returned configuration
// before calling NewHiveCatalog.
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	// Auth mechanism used when connecting to HiveServer2 (e.g. "KERBEROS", "NONE").
	Auth string
	// Service is used by Kerberos authentication.
	Service string
	// Database selected after connection. Defaults to "default" when empty.
	Database string
	// Optional TLS configuration for HTTPS transport.
	TLSConfig *tls.Config
}

func (c Config) connectConfiguration() *gohive.ConnectConfiguration {
	cfg := gohive.NewConnectConfiguration()
	cfg.Username = c.Username
	cfg.Password = c.Password
	cfg.Service = c.Service
	cfg.Database = c.Database
	cfg.TLSConfig = c.TLSConfig
	return cfg
}

// NewHiveCatalog creates a HiveCatalog and establishes a connection to the Hive
// metastore using the provided configuration. The active gohive client is stored
// in the catalog and automatically re-established if the connection is lost.
func NewHiveCatalog(conf Config, opts iceberg.Properties) (*HiveCatalog, error) {
	conn, err := gohive.Connect(conf.Host, conf.Port, conf.Auth, conf.connectConfiguration())
	if err != nil {
		return nil, fmt.Errorf("connect to hive metastore: %w", err)
	}

	return &HiveCatalog{
		metastoreURI: fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		options:      opts,
		client:       conn,
		cfg:          conf,
	}, nil
}

// ensureConnection checks if the client is initialized and attempts to
// reconnect when necessary.
func (h *HiveCatalog) ensureConnection() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.client != nil {
		return nil
	}

	conn, err := gohive.Connect(h.cfg.Host, h.cfg.Port, h.cfg.Auth, h.cfg.connectConfiguration())
	if err != nil {
		return err
	}
	h.client = conn
	return nil
}

// withReconnect executes fn using the active gohive client. If the operation
// fails, the connection is reset and the operation retried once.
func (h *HiveCatalog) withReconnect(fn func(*gohive.Connection) error) error {
	if err := h.ensureConnection(); err != nil {
		return err
	}

	if err := fn(h.client); err != nil {
		h.mu.Lock()
		if h.client != nil {
			_ = h.client.Close()
			h.client = nil
		}
		h.mu.Unlock()

		if errConn := h.ensureConnection(); errConn != nil {
			return errConn
		}

		return fn(h.client)
	}

	return nil
}

// Ping verifies the connection to Hive is still alive. If the connection was
// dropped, Ping attempts to automatically reconnect.
func (h *HiveCatalog) Ping(ctx context.Context) error {
	return h.withReconnect(func(conn *gohive.Connection) error {
		cursor := conn.Cursor()
		defer cursor.Close()
		cursor.Exec(ctx, "SELECT 1")
		return cursor.Err
	})
}

var _ catalog.Catalog = (*HiveCatalog)(nil)

// CatalogType returns the catalog type for Hive.
func (h *HiveCatalog) CatalogType() catalog.Type {
	return catalog.Hive
}

// CreateTable creates a new Iceberg table in the Hive catalog.
func (h *HiveCatalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, errors.New("not implemented")
}

// CommitTable commits table changes to the Hive catalog.
func (h *HiveCatalog) CommitTable(ctx context.Context, tbl *table.Table, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("not implemented")
}

// ListTables lists tables within a namespace.
func (h *HiveCatalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return nil
}

// LoadTable loads a table from the Hive catalog.
func (h *HiveCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	return nil, errors.New("not implemented")
}

// DropTable drops a table from the Hive catalog.
func (h *HiveCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	return errors.New("not implemented")
}

// RenameTable renames an existing table in the Hive catalog.
func (h *HiveCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	return nil, errors.New("not implemented")
}

// CheckTableExists checks whether a table exists in the Hive catalog.
func (h *HiveCatalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	return false, errors.New("not implemented")
}

// ListNamespaces lists namespaces, optionally filtered by a parent namespace.
func (h *HiveCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	return nil, errors.New("not implemented")
}

// CreateNamespace creates a new namespace in the Hive catalog.
func (h *HiveCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	return errors.New("not implemented")
}

// DropNamespace drops a namespace from the Hive catalog.
func (h *HiveCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return errors.New("not implemented")
}

// CheckNamespaceExists checks whether a namespace exists in the Hive catalog.
func (h *HiveCatalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	return false, errors.New("not implemented")
}

// LoadNamespaceProperties loads namespace properties from the Hive catalog.
func (h *HiveCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return nil, errors.New("not implemented")
}

// UpdateNamespaceProperties updates namespace properties in the Hive catalog.
func (h *HiveCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, errors.New("not implemented")
}
