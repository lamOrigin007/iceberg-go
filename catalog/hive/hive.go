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
	"errors"

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
