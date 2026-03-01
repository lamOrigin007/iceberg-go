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

package config

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGUCMappings_Basic(t *testing.T) {
	gucValue := "lineitem.l_orderkey=orders.o_orderkey,lineitem.l_partkey=part.p_partkey"

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "l_orderkey", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "l_partkey", Type: iceberg.PrimitiveTypes.Int64},
	)

	tableAliases := map[string]*iceberg.Schema{
		"lineitem": schema,
		"orders":   schema,
		"part":     schema,
	}

	config, err := ParseGUCMappings(gucValue, tableAliases)

	require.NoError(t, err)
	assert.True(t, config.Enabled)
	assert.Len(t, config.Mappings, 2)

	// Проверка первого маппинга
	m1 := config.Mappings[0]
	assert.Equal(t, "lineitem", m1.TargetAlias)
	assert.Equal(t, "l_orderkey", m1.TargetField)
	assert.Equal(t, "orders", m1.SourceAlias)
	assert.Equal(t, "o_orderkey", m1.SourceField)
}

func TestParseGUCMappings_Empty(t *testing.T) {
	config, err := ParseGUCMappings("", nil)

	require.NoError(t, err)
	assert.False(t, config.Enabled)
	assert.Empty(t, config.Mappings)
}

func TestParseGUCMappings_InvalidFormat(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"missing_equals", "target.field source.field"},
		{"missing_target_field", "=source.field"},
		{"missing_source_field", "target.field="},
		{"invalid_target", "target=source.field"},
		{"invalid_source", "target.field=source"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseGUCMappings(tt.value, nil)
			assert.Error(t, err)
		})
	}
}

func TestParseCoordinatorAddr(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
		hasError bool
	}{
		{"with_port", "localhost:9999", "localhost:9999", false},
		{"without_port", "localhost", "localhost:9999", false},
		{"with_ip", "192.168.1.1:8888", "192.168.1.1:8888", false},
		{"empty", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := ParseCoordinatorAddr(tt.value)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, addr)
			}
		})
	}
}

func TestParseWaitTimeout(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected int
		hasError bool
	}{
		{"valid", "30", 30, false},
		{"default", "", 30, false}, // DefaultWaitTimeout = 30s
		{"invalid", "abc", 0, true},
		{"negative", "-5", 0, true},
		{"zero", "0", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeout, err := ParseWaitTimeout(tt.value)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, timeout)
			}
		})
	}
}

func TestParseBatchSize(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected int
		hasError bool
	}{
		{"valid", "10000", 10000, false},
		{"default", "", 10000, false}, // DefaultBatchSize
		{"invalid", "abc", 0, true},
		{"negative", "-100", 0, true},
		{"zero", "0", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, err := ParseBatchSize(tt.value)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, size)
			}
		})
	}
}

func TestParseEnabled(t *testing.T) {
	tests := []struct {
		value    string
		expected bool
	}{
		{"on", true},
		{"On", true},
		{"ON", true},
		{"true", true},
		{"True", true},
		{"1", true},
		{"yes", true},
		{"off", false},
		{"false", false},
		{"0", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			assert.Equal(t, tt.expected, ParseEnabled(tt.value))
		})
	}
}

func TestDynamicFilterConfig_IsSourceTable(t *testing.T) {
	config := &DynamicFilterConfig{
		Enabled: true,
		Mappings: []dynamicfilter.FieldMapping{
			{TargetAlias: "lineitem", SourceAlias: "orders"},
			{TargetAlias: "lineitem", SourceAlias: "part"},
		},
	}

	assert.True(t, config.IsSourceTable("orders"))
	assert.True(t, config.IsSourceTable("part"))
	assert.False(t, config.IsSourceTable("lineitem"))
	assert.False(t, config.IsSourceTable("unknown"))
}

func TestDynamicFilterConfig_IsTargetTable(t *testing.T) {
	config := &DynamicFilterConfig{
		Enabled: true,
		Mappings: []dynamicfilter.FieldMapping{
			{TargetAlias: "lineitem", SourceAlias: "orders"},
			{TargetAlias: "lineitem", SourceAlias: "part"},
		},
	}

	assert.True(t, config.IsTargetTable("lineitem"))
	assert.False(t, config.IsTargetTable("orders"))
	assert.False(t, config.IsTargetTable("part"))
}

func TestDynamicFilterConfig_GetSourceAliases(t *testing.T) {
	config := &DynamicFilterConfig{
		Enabled: true,
		Mappings: []dynamicfilter.FieldMapping{
			{TargetAlias: "lineitem", SourceAlias: "orders"},
			{TargetAlias: "lineitem", SourceAlias: "part"},
			{TargetAlias: "orders", SourceAlias: "customer"},
		},
	}

	aliases := config.GetSourceAliases()
	assert.Len(t, aliases, 3)
	assert.Contains(t, aliases, "orders")
	assert.Contains(t, aliases, "part")
	assert.Contains(t, aliases, "customer")
}

func TestDynamicFilterConfig_GetTargetAliases(t *testing.T) {
	config := &DynamicFilterConfig{
		Enabled: true,
		Mappings: []dynamicfilter.FieldMapping{
			{TargetAlias: "lineitem", SourceAlias: "orders"},
			{TargetAlias: "lineitem", SourceAlias: "part"},
			{TargetAlias: "orders", SourceAlias: "customer"},
		},
	}

	aliases := config.GetTargetAliases()
	assert.Len(t, aliases, 2)
	assert.Contains(t, aliases, "lineitem")
	assert.Contains(t, aliases, "orders")
}

func TestDynamicFilterConfig_HasMappings(t *testing.T) {
	config := &DynamicFilterConfig{
		Enabled: true,
		Mappings: []dynamicfilter.FieldMapping{
			{TargetAlias: "lineitem", SourceAlias: "orders"},
		},
	}

	assert.True(t, config.HasMappings("orders", "lineitem"))
	assert.False(t, config.HasMappings("lineitem", "orders"))
	assert.False(t, config.HasMappings("unknown", "unknown"))
}
