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

	// fieldNames - список имен полей для применения фильтров
	fieldNames []string
	// fieldTypes - типы полей
	fieldTypes map[string]iceberg.Type
	// timeout - таймаут ожидания фильтра
	timeout time.Duration

	// appliedFilters - примененные фильтры по fieldName
	appliedFilters map[string]*types.DynamicFilter

	// filtersWaited - флаг что фильтры уже ожиданы
	filtersWaited bool
}

// ScanFilterApplierConfig конфигурация для ScanFilterApplier
type ScanFilterApplierConfig struct {
	QueryID     string
	SessionID   string
	TargetAlias string
	FieldNames  []string
	FieldTypes  map[string]iceberg.Type
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
		fieldNames:     cfg.FieldNames,
		fieldTypes:     cfg.FieldTypes,
		timeout:        timeout,
		appliedFilters: make(map[string]*types.DynamicFilter),
	}
}

// WaitForFilters ожидает готовности фильтров для всех полей.
// Реализация table.FilterProvider интерфейса.
func (a *ScanFilterApplier) WaitForFilters(ctx context.Context, timeout time.Duration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Если уже ожидано - возвращаем true
	if a.filtersWaited {
		return true
	}

	// Создаем контекст с таймаутом
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Ожидаем фильтры для всех полей
	for _, fieldName := range a.fieldNames {
		filter, err := a.client.WaitForFilter(waitCtx, a.targetAlias, fieldName, timeout)
		if err != nil {
			// Таймаут или ошибка - возвращаем false
			a.filtersWaited = true
			return false
		}
		if filter != nil {
			a.appliedFilters[fieldName] = filter
		}
	}

	a.filtersWaited = true
	return true
}

// GetFilter возвращает построенный BooleanExpression из собранных фильтров.
// Реализация table.FilterProvider интерфейса.
func (a *ScanFilterApplier) GetFilter() iceberg.BooleanExpression {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var predicates []iceberg.BooleanExpression

	for fieldName, filter := range a.appliedFilters {
		if filter == nil {
			continue
		}

		fieldRef := iceberg.Reference(fieldName)
		expr := filter.BuildExpression(fieldRef)
		if expr != nil && !expr.Equals(iceberg.AlwaysTrue{}) {
			predicates = append(predicates, expr)
		}
	}

	if len(predicates) == 0 {
		return iceberg.AlwaysTrue{}
	}

	if len(predicates) == 1 {
		return predicates[0]
	}

	return iceberg.NewOr(predicates[0], predicates[1], predicates[2:]...)
}

// WaitForFiltersLegacy ожидает готовности фильтров для всех полей.
// Устаревший метод, используйте WaitForFilters(ctx, timeout) bool.
// Должен вызываться перед началом сканирования target таблицы.
func (a *ScanFilterApplier) WaitForFiltersLegacy(ctx context.Context) error {
	for _, fieldName := range a.fieldNames {
		filter, err := a.WaitForFilter(ctx, fieldName)
		if err != nil {
			return fmt.Errorf("failed to wait for filter for field %s: %w", fieldName, err)
		}
		if filter != nil {
			a.appliedFilters[fieldName] = filter
		}
	}
	return nil
}

// WaitForFilter ожидает готовности фильтра для конкретного поля.
// Возвращает nil если фильтр не был получен (таймаут или не готов).
func (a *ScanFilterApplier) WaitForFilter(ctx context.Context, fieldName string) (*types.DynamicFilter, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Проверка кэша
	if filter, ok := a.appliedFilters[fieldName]; ok {
		return filter, nil
	}

	// Ожидание от координатора
	filter, err := a.client.WaitForFilter(ctx, a.targetAlias, fieldName, a.timeout)
	if err != nil {
		return nil, err
	}

	if filter != nil {
		a.appliedFilters[fieldName] = filter
	}

	return filter, nil
}

// GetFilterForField возвращает ранее полученный фильтр для поля (не блокируется).
func (a *ScanFilterApplier) GetFilterForField(fieldName string) *types.DynamicFilter {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.appliedFilters[fieldName]
}

// BuildFilterExpression строит BooleanExpression для поля.
// Возвращает AlwaysTrue если фильтр не установлен.
func (a *ScanFilterApplier) BuildFilterExpression(fieldName string, fieldRef iceberg.UnboundTerm) (iceberg.BooleanExpression, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	filter, ok := a.appliedFilters[fieldName]
	if !ok || filter == nil {
		return iceberg.AlwaysTrue{}, nil
	}

	return filter.BuildExpression(fieldRef), nil
}

// BuildCombinedFilterExpression строит комбинированный фильтр для всех полей.
// Поля объединяются через AND.
func (a *ScanFilterApplier) BuildCombinedFilterExpression(
	fieldRefs map[string]iceberg.UnboundTerm,
) (iceberg.BooleanExpression, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var predicates []iceberg.BooleanExpression

	for fieldName, filter := range a.appliedFilters {
		if filter == nil {
			continue
		}

		fieldRef, ok := fieldRefs[fieldName]
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
func (a *ScanFilterApplier) BuildRowFilter(fieldNames map[string]string) (iceberg.BooleanExpression, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var predicates []iceberg.BooleanExpression

	for fieldName, filter := range a.appliedFilters {
		if filter == nil {
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
func (a *ScanFilterApplier) GetAppliedFilters() map[string]*types.DynamicFilter {
	a.mu.RLock()
	defer a.mu.RUnlock()

	result := make(map[string]*types.DynamicFilter, len(a.appliedFilters))
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
	a.appliedFilters = make(map[string]*types.DynamicFilter)
}
