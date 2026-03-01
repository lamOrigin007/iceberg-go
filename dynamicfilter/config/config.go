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

// Package config предоставляет конфигурацию для динамических фильтров
package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter/types"
)

// GUC параметр для маппинга полей динамических фильтров
// Формат: 'target_alias.field=source_alias.field,alias2.col2=alias3.col3'
const DynamicFilterMappingsGUC = "iceberg.dynamic_filter_mappings"

// GUC параметр для адреса координатора
const DynamicFilterCoordinatorAddrGUC = "iceberg.dynamic_filter_coordinator_addr"

// GUC параметр для включения динамических фильтров
const DynamicFilterEnabledGUC = "iceberg.enable_dynamic_filters"

// GUC параметр для таймаута ожидания фильтра (секунды)
const DynamicFilterWaitTimeoutGUC = "iceberg.dynamic_filter_wait_timeout"

// GUC параметр для размера пакета отправки значений
const DynamicFilterBatchSizeGUC = "iceberg.dynamic_filter_batch_size"

// DynamicFilterConfig конфигурация динамических фильтров
type DynamicFilterConfig struct {
	// Enabled включены ли динамические фильтры
	Enabled bool

	// CoordinatorAddr адрес координатора (host:port)
	CoordinatorAddr string

	// Mappings маппинг полей source->target
	Mappings []types.FieldMapping

	// WaitTimeout таймаут ожидания фильтра
	WaitTimeout int // секунды

	// BatchSize размер пакета для отправки значений
	BatchSize int
}

// ParseGUCMappings парсит GUC параметр с маппингом полей
//
// Формат: 'target_alias.field=source_alias.field,alias2.col2=alias3.col3'
//
// tableAliases - маппинг алиасов таблиц к их schema для определения типов полей
func ParseGUCMappings(gucValue string, tableAliases map[string]*iceberg.Schema) (*DynamicFilterConfig, error) {
	if gucValue == "" {
		return &DynamicFilterConfig{
			Enabled: false,
		}, nil
	}

	config := &DynamicFilterConfig{
		Enabled:   true,
		BatchSize: types.DefaultBatchSize,
		WaitTimeout: int(types.DefaultWaitTimeout.Seconds()),
	}

	// Разделение на отдельные маппинги
	pairs := strings.Split(gucValue, ",")
	mappings := make([]types.FieldMapping, 0, len(pairs))

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Разделение по '='
		parts := strings.Split(pair, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid mapping format %q: expected target.field=source.field", pair)
		}

		target := strings.TrimSpace(parts[0])
		source := strings.TrimSpace(parts[1])

		// Парсинг target.alias
		targetParts := strings.Split(target, ".")
		if len(targetParts) != 2 {
			return nil, fmt.Errorf("invalid target format %q: expected alias.field", target)
		}
		targetAlias := targetParts[0]
		targetField := targetParts[1]

		// Парсинг source.alias
		sourceParts := strings.Split(source, ".")
		if len(sourceParts) != 2 {
			return nil, fmt.Errorf("invalid source format %q: expected alias.field", source)
		}
		sourceAlias := sourceParts[0]
		sourceField := sourceParts[1]

		// Определение типов полей из schema
		var sourceFieldType iceberg.Type
		var sourceFieldID, targetFieldID int

		if schema, ok := tableAliases[targetAlias]; ok {
			f, found := schema.FindFieldByName(targetField)
			if !found {
				return nil, fmt.Errorf("field %q not found in target table %q", targetField, targetAlias)
			}
			targetFieldID = f.ID
		}

		if schema, ok := tableAliases[sourceAlias]; ok {
			f, found := schema.FindFieldByName(sourceField)
			if !found {
				return nil, fmt.Errorf("field %q not found in source table %q", sourceField, sourceAlias)
			}
			sourceFieldType = f.Type
			sourceFieldID = f.ID
		}

		mappings = append(mappings, types.FieldMapping{
			TargetAlias:   targetAlias,
			TargetField:   targetField,
			SourceAlias:   sourceAlias,
			SourceField:   sourceField,
			TargetFieldID: targetFieldID,
			SourceFieldID: sourceFieldID,
			FieldType:     sourceFieldType, // используем тип source для значений
		})
	}

	config.Mappings = mappings
	return config, nil
}

// ParseCoordinatorAddr парсит адрес координатора из GUC параметра
func ParseCoordinatorAddr(gucValue string) (string, error) {
	if gucValue == "" {
		return "", fmt.Errorf("coordinator address is required")
	}

	// Проверка формата host:port
	host, port, err := net.SplitHostPort(gucValue)
	if err != nil {
		// Если порт не указан, используем порт по умолчанию
		host = gucValue
		port = "9999"
	}

	return net.JoinHostPort(host, port), nil
}

// ParseWaitTimeout парсит таймаут ожидания из GUC параметра
func ParseWaitTimeout(gucValue string) (int, error) {
	if gucValue == "" {
		return int(types.DefaultWaitTimeout.Seconds()), nil
	}

	timeout, err := strconv.Atoi(gucValue)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout value %q: %w", gucValue, err)
	}

	if timeout <= 0 {
		return 0, fmt.Errorf("timeout must be positive, got %d", timeout)
	}

	return timeout, nil
}

// ParseBatchSize парсит размер пакета из GUC параметра
func ParseBatchSize(gucValue string) (int, error) {
	if gucValue == "" {
		return types.DefaultBatchSize, nil
	}

	size, err := strconv.Atoi(gucValue)
	if err != nil {
		return 0, fmt.Errorf("invalid batch size value %q: %w", gucValue, err)
	}

	if size <= 0 {
		return 0, fmt.Errorf("batch size must be positive, got %d", size)
	}

	return size, nil
}

// ParseEnabled парсит флаг включения динамических фильтров
func ParseEnabled(gucValue string) bool {
	if gucValue == "" {
		return false
	}

	gucValue = strings.ToLower(strings.TrimSpace(gucValue))
	return gucValue == "on" || gucValue == "true" || gucValue == "1" || gucValue == "yes"
}

// LoadFromGUC загружает конфигурацию из GUC параметров
//
// getGUCFunc - функция для получения значения GUC параметра
func LoadFromGUC(getGUCFunc func(string) string) (*DynamicFilterConfig, error) {
	// Получение значения флага включения
	enabledStr := getGUCFunc(DynamicFilterEnabledGUC)
	enabled := ParseEnabled(enabledStr)

	if !enabled {
		return &DynamicFilterConfig{
			Enabled: false,
		}, nil
	}

	// Получение адреса координатора
	coordinatorAddrStr := getGUCFunc(DynamicFilterCoordinatorAddrGUC)
	coordinatorAddr, err := ParseCoordinatorAddr(coordinatorAddrStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse coordinator address: %w", err)
	}

	// Получение маппингов
	mappingsStr := getGUCFunc(DynamicFilterMappingsGUC)
	config, err := ParseGUCMappings(mappingsStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse mappings: %w", err)
	}

	// Получение таймаута
	waitTimeoutStr := getGUCFunc(DynamicFilterWaitTimeoutGUC)
	waitTimeout, err := ParseWaitTimeout(waitTimeoutStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse wait timeout: %w", err)
	}

	// Получение размера пакета
	batchSizeStr := getGUCFunc(DynamicFilterBatchSizeGUC)
	batchSize, err := ParseBatchSize(batchSizeStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse batch size: %w", err)
	}

	config.Enabled = true
	config.CoordinatorAddr = coordinatorAddr
	config.WaitTimeout = waitTimeout
	config.BatchSize = batchSize

	return config, nil
}

// IsSourceTable проверяет, является ли таблица источником значений
func (c *DynamicFilterConfig) IsSourceTable(alias string) bool {
	for _, m := range c.Mappings {
		if m.SourceAlias == alias {
			return true
		}
	}
	return false
}

// IsTargetTable проверяет, является ли таблица целью для фильтра
func (c *DynamicFilterConfig) IsTargetTable(alias string) bool {
	for _, m := range c.Mappings {
		if m.TargetAlias == alias {
			return true
		}
	}
	return false
}

// GetSourceFieldsForTarget возвращает поля source для данного target
func (c *DynamicFilterConfig) GetSourceFieldsForTarget(targetAlias string) []types.FieldMapping {
	var result []types.FieldMapping
	for _, m := range c.Mappings {
		if m.TargetAlias == targetAlias {
			result = append(result, m)
		}
	}
	return result
}

// GetTargetFieldsForSource возвращает поля target для данного source
func (c *DynamicFilterConfig) GetTargetFieldsForSource(sourceAlias string) []types.FieldMapping {
	var result []types.FieldMapping
	for _, m := range c.Mappings {
		if m.SourceAlias == sourceAlias {
			result = append(result, m)
		}
	}
	return result
}

// GetSourceAliases возвращает все уникальные алиасы source таблиц
func (c *DynamicFilterConfig) GetSourceAliases() []string {
	seen := make(map[string]bool)
	var result []string

	for _, m := range c.Mappings {
		if !seen[m.SourceAlias] {
			seen[m.SourceAlias] = true
			result = append(result, m.SourceAlias)
		}
	}

	return result
}

// GetTargetAliases возвращает все уникальные алиасы target таблиц
func (c *DynamicFilterConfig) GetTargetAliases() []string {
	seen := make(map[string]bool)
	var result []string

	for _, m := range c.Mappings {
		if !seen[m.TargetAlias] {
			seen[m.TargetAlias] = true
			result = append(result, m.TargetAlias)
		}
	}

	return result
}

// HasMappings проверяет, есть ли маппинги для указанных алиасов
func (c *DynamicFilterConfig) HasMappings(sourceAlias, targetAlias string) bool {
	for _, m := range c.Mappings {
		if m.SourceAlias == sourceAlias && m.TargetAlias == targetAlias {
			return true
		}
	}
	return false
}
