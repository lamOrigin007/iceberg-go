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

package dynamicfilter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter/client"
	"github.com/apache/iceberg-go/dynamicfilter/types"
)

// ScanFilterApplier предоставляет функциональность для применения динамических фильтров
// к target таблицам во время сканирования.
//
// Этот тип используется в Postgres FDW для ожидания фильтров от координатора
// и построения BooleanExpression для применения к сканеру.
type ScanFilterApplier struct {
	mu sync.RWMutex

	queryID     string
	sessionID   string
	targetAlias string
	client      *client.Client

	// fieldIDs - список ID полей для применения фильтров
	fieldIDs []int
	// fieldTypes - типы полей
	fieldTypes map[int]iceberg.Type
	// timeout - таймаут ожидания фильтра
	timeout time.Duration

	// appliedFilters - примененные фильтры по fieldID
	appliedFilters map[int]*types.DynamicFilter
}

// ScanFilterApplierConfig конфигурация для ScanFilterApplier
type ScanFilterApplierConfig struct {
	QueryID     string
	SessionID   string
	TargetAlias string
	FieldIDs    []int
	FieldTypes  map[int]iceberg.Type
	Timeout     time.Duration
}

// NewScanFilterApplier создает новый ScanFilterApplier для использования с table.Scan
func NewScanFilterApplier(cfg ScanFilterApplierConfig, dfClient *client.Client) *ScanFilterApplier {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = types.DefaultWaitTimeout
	}

	return &ScanFilterApplier{
		queryID:        cfg.QueryID,
		sessionID:      cfg.SessionID,
		targetAlias:    cfg.TargetAlias,
		client:         dfClient,
		fieldIDs:       cfg.FieldIDs,
		fieldTypes:     cfg.FieldTypes,
		timeout:        timeout,
		appliedFilters: make(map[int]*types.DynamicFilter),
	}
}

// WaitForFilters ожидает готовности фильтров для всех полей.
// Должен вызываться перед началом сканирования target таблицы.
func (a *ScanFilterApplier) WaitForFilters(ctx context.Context) error {
	for _, fieldID := range a.fieldIDs {
		filter, err := a.WaitForFilter(ctx, fieldID)
		if err != nil {
			return fmt.Errorf("failed to wait for filter for field %d: %w", fieldID, err)
		}
		if filter != nil {
			a.appliedFilters[fieldID] = filter
		}
	}
	return nil
}

// WaitForFilter ожидает готовности фильтра для конкретного поля.
// Возвращает nil если фильтр не был получен (таймаут или не готов).
func (a *ScanFilterApplier) WaitForFilter(ctx context.Context, fieldID int) (*types.DynamicFilter, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Проверка кэша
	if filter, ok := a.appliedFilters[fieldID]; ok {
		return filter, nil
	}

	// Ожидание от координатора
	filter, err := a.client.WaitForFilter(ctx, a.targetAlias, fieldID, a.timeout)
	if err != nil {
		return nil, err
	}

	if filter != nil {
		a.appliedFilters[fieldID] = filter
	}

	return filter, nil
}

// GetFilter возвращает ранее полученный фильтр для поля (не блокируется).
func (a *ScanFilterApplier) GetFilter(fieldID int) *types.DynamicFilter {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.appliedFilters[fieldID]
}

// BuildFilterExpression строит BooleanExpression для поля.
// Возвращает AlwaysTrue если фильтр не установлен.
func (a *ScanFilterApplier) BuildFilterExpression(fieldID int, fieldRef iceberg.UnboundTerm) (iceberg.BooleanExpression, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	filter, ok := a.appliedFilters[fieldID]
	if !ok || filter == nil {
		return iceberg.AlwaysTrue{}, nil
	}

	return filter.BuildExpression(fieldRef), nil
}

// BuildCombinedFilterExpression строит комбинированный фильтр для всех полей.
// Поля объединяются через AND.
func (a *ScanFilterApplier) BuildCombinedFilterExpression(
	fieldRefs map[int]iceberg.UnboundTerm,
) (iceberg.BooleanExpression, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var predicates []iceberg.BooleanExpression

	for fieldID, filter := range a.appliedFilters {
		if filter == nil {
			continue
		}

		fieldRef, ok := fieldRefs[fieldID]
		if !ok {
			continue
		}

		expr := filter.BuildExpression(fieldRef)
		if expr != nil && !expr.Equals(iceberg.AlwaysTrue{}) {
			predicates = append(predicates, expr)
		}
	}

	if len(predicates) == 0 {
		return iceberg.AlwaysTrue{}, nil
	}

	if len(predicates) == 1 {
		return predicates[0], nil
	}

	// Комбинирование через AND
	result := predicates[0]
	for _, p := range predicates[1:] {
		result = iceberg.NewAnd(result, p)
	}

	return result, nil
}

// BuildRowFilter строит row filter для использования с table.Scan.
// Это удобный метод для получения комбинированного фильтра.
func (a *ScanFilterApplier) BuildRowFilter(fieldRefs map[int]string) (iceberg.BooleanExpression, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var predicates []iceberg.BooleanExpression

	for fieldID, filter := range a.appliedFilters {
		if filter == nil {
			continue
		}

		fieldName, ok := fieldRefs[fieldID]
		if !ok {
			continue
		}

		fieldRef := iceberg.Reference(fieldName)
		expr := filter.BuildExpression(fieldRef)
		if expr != nil && !expr.Equals(iceberg.AlwaysTrue{}) {
			predicates = append(predicates, expr)
		}
	}

	if len(predicates) == 0 {
		return iceberg.AlwaysTrue{}, nil
	}

	if len(predicates) == 1 {
		return predicates[0], nil
	}

	result := predicates[0]
	for _, p := range predicates[1:] {
		result = iceberg.NewAnd(result, p)
	}

	return result, nil
}

// GetAppliedFilters возвращает копию примененных фильтров.
func (a *ScanFilterApplier) GetAppliedFilters() map[int]*types.DynamicFilter {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[int]*types.DynamicFilter, len(a.appliedFilters))
	for k, v := range a.appliedFilters {
		result[k] = v
	}
	return result
}

// HasFilters проверяет, есть ли примененные фильтры.
func (a *ScanFilterApplier) HasFilters() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for _, filter := range a.appliedFilters {
		if filter != nil && filter.FilterType != types.FilterTypeUnspecified {
			return true
		}
	}
	return false
}

// GetFilterStats возвращает статистику примененных фильтров.
func (a *ScanFilterApplier) GetFilterStats() (count int, totalValues int64, filterTypes map[types.FilterType]int) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	count = len(a.appliedFilters)
	filterTypes = make(map[types.FilterType]int)

	for _, filter := range a.appliedFilters {
		if filter != nil {
			totalValues += filter.TotalValues
			filterTypes[filter.FilterType]++
		}
	}

	return
}

// ClearFilters очищает все примененные фильтры.
func (a *ScanFilterApplier) ClearFilters() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appliedFilters = make(map[int]*types.DynamicFilter)
}
