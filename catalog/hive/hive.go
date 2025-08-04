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
	"regexp"
	"strings"
	"sync"

	"iter"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
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
	// Stage the table by creating metadata and writing it to the target
	// location.  We use the helper from the catalog/internal package to
	// build the initial metadata structure and choose a metadata location.
	staged, err := internal.CreateStagedTable(ctx, h.options, func(context.Context, table.Identifier) (iceberg.Properties, error) {
		// Hive has no namespace properties that affect location, so we
		// simply return an empty set.
		return iceberg.Properties{}, nil
	}, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	// Write the metadata file to the filesystem before attempting to
	// register the table in Hive.  If the connection fails we do not remove
	// the metadata file – this mirrors the behaviour of other catalog
	// implementations.
	fs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := fs.(io.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}
	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	// Build the CREATE TABLE statement.  Hive expects Iceberg tables to be
	// stored using the HiveIcebergStorageHandler and to include the
	// metadata location in table properties.
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	tblName := catalog.TableNameFromIdent(identifier)
	stmt := fmt.Sprintf("CREATE TABLE %s.%s STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' "+
		"LOCATION '%s' TBLPROPERTIES('table_type'='ICEBERG','metadata_location'='%s')",
		ns, tblName, staged.Metadata().Location(), staged.MetadataLocation())

	if err := h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, stmt)
		return cur.Err
	}); err != nil {
		return nil, err
	}

	return staged.Table, nil
}

// CommitTable commits table changes to the Hive catalog.
func (h *HiveCatalog) CommitTable(ctx context.Context, tbl *table.Table, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("not implemented")
}

// ListTables lists tables within a namespace.
func (h *HiveCatalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	// Retrieve all table names for the given namespace using Hive's
	// SHOW TABLES command.  The result is collected into a slice before
	// returning an iterator sequence over the identifiers.
	var tables []table.Identifier

	ns := strings.Join(namespace, ".")
	query := "SHOW TABLES"
	if ns != "" {
		query = fmt.Sprintf("SHOW TABLES IN %s", ns)
	}

	if err := h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, query)
		if cur.Err != nil {
			return cur.Err
		}
		for {
			row := cur.RowMap(ctx)
			if cur.Err != nil {
				if cur.Err.Error() == "No more rows are left" {
					cur.Err = nil
					break
				}
				return cur.Err
			}

			// SHOW TABLES returns a single column with the table
			// name.  RowMap returns a map with that column name
			// as the key.  We simply take the first string value.
			var name string
			for _, v := range row {
				if s, ok := v.(string); ok {
					name = s
					break
				}
			}
			if name != "" {
				if ns != "" {
					ident := append(append(table.Identifier{}, namespace...), name)
					tables = append(tables, ident)
				} else {
					tables = append(tables, table.Identifier{name})
				}
			}
		}
		return nil
	}); err != nil {
		return func(yield func(table.Identifier, error) bool) {
			yield(table.Identifier{}, err)
		}
	}

	return func(yield func(table.Identifier, error) bool) {
		for _, t := range tables {
			if !yield(t, nil) {
				return
			}
		}
	}
}

// LoadTable loads a table from the Hive catalog.
func (h *HiveCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	if props == nil {
		props = iceberg.Properties{}
	}

	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	tblName := catalog.TableNameFromIdent(identifier)

	var metadataLocation string

	// Extract the metadata location from the CREATE TABLE statement using
	// SHOW CREATE TABLE.  This avoids having to parse the verbose output of
	// DESCRIBE FORMATTED.
	err := h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, fmt.Sprintf("SHOW CREATE TABLE %s.%s", ns, tblName))
		if cur.Err != nil {
			return cur.Err
		}

		var ddl strings.Builder
		for {
			row := cur.RowMap(ctx)
			if cur.Err != nil {
				if cur.Err.Error() == "No more rows are left" {
					cur.Err = nil
					break
				}
				return cur.Err
			}
			for _, v := range row {
				if s, ok := v.(string); ok {
					ddl.WriteString(s)
					ddl.WriteByte('\n')
				}
			}
		}

		// Metadata location is stored in the table properties as
		// 'metadata_location'.  We use a regular expression to pull the
		// value from the CREATE TABLE statement.
		re := regexp.MustCompile(`'metadata_location'='([^']+)'`)
		matches := re.FindStringSubmatch(ddl.String())
		if len(matches) == 2 {
			metadataLocation = matches[1]
			return nil
		}

		// As a fallback, try to extract the LOCATION clause and assume
		// the metadata file resides under the table location.
		reLoc := regexp.MustCompile(`LOCATION\s+'([^']+)'`)
		locMatch := reLoc.FindStringSubmatch(ddl.String())
		if len(locMatch) != 2 {
			return fmt.Errorf("metadata location not found for %s.%s", ns, tblName)
		}
		metadataLocation = locMatch[1]
		return nil
	})
	if err != nil {
		return nil, err
	}

	tbl, err := table.NewFromLocation(ctx, identifier, metadataLocation, io.LoadFSFunc(props, metadataLocation), h)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

// DropTable drops a table from the Hive catalog.
func (h *HiveCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	tblName := catalog.TableNameFromIdent(identifier)
	stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ns, tblName)

	return h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, stmt)
		return cur.Err
	})
}

// RenameTable renames an existing table in the Hive catalog.
func (h *HiveCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	fromNs := strings.Join(catalog.NamespaceFromIdent(from), ".")
	fromTbl := catalog.TableNameFromIdent(from)
	toNs := strings.Join(catalog.NamespaceFromIdent(to), ".")
	toTbl := catalog.TableNameFromIdent(to)

	stmt := fmt.Sprintf("ALTER TABLE %s.%s RENAME TO %s.%s", fromNs, fromTbl, toNs, toTbl)

	if err := h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, stmt)
		return cur.Err
	}); err != nil {
		return nil, err
	}

	return h.LoadTable(ctx, to, nil)
}

// CheckTableExists checks whether a table exists in the Hive catalog.
func (h *HiveCatalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	return false, errors.New("not implemented")
}

// ListNamespaces lists namespaces, optionally filtered by a parent namespace.
func (h *HiveCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	if len(parent) > 0 {
		// Hive does not support nested namespaces.  If a parent is
		// provided we simply return an empty list.
		return []table.Identifier{}, nil
	}

	var namespaces []table.Identifier

	if err := h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, "SHOW DATABASES")
		if cur.Err != nil {
			return cur.Err
		}
		for {
			row := cur.RowMap(ctx)
			if cur.Err != nil {
				if cur.Err.Error() == "No more rows are left" {
					cur.Err = nil
					break
				}
				return cur.Err
			}
			for _, v := range row {
				if s, ok := v.(string); ok {
					namespaces = append(namespaces, table.Identifier{s})
					break
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return namespaces, nil
}

// CreateNamespace creates a new namespace in the Hive catalog.
func (h *HiveCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	ns := strings.Join(namespace, ".")
	stmt := fmt.Sprintf("CREATE DATABASE %s", ns)

	return h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, stmt)
		return cur.Err
	})
}

// DropNamespace drops a namespace from the Hive catalog.
func (h *HiveCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	ns := strings.Join(namespace, ".")
	stmt := fmt.Sprintf("DROP DATABASE IF EXISTS %s", ns)

	return h.withReconnect(func(conn *gohive.Connection) error {
		cur := conn.Cursor()
		defer cur.Close()
		cur.Exec(ctx, stmt)
		return cur.Err
	})
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
