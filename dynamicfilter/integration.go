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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter/client"
	"github.com/apache/iceberg-go/dynamicfilter/types"
)

// SourceValueCollector извлекает значения из source таблиц и отправляет их на координатор
type SourceValueCollector struct {
	mu          sync.RWMutex
	queryID     string
	sessionID   string
	sourceAlias string
	client      *client.Client
	fieldNames  []string
	fieldTypes  map[string]iceberg.Type
	bufferSize  int

	// Буферы значений по полям
	buffers map[string][]iceberg.Literal

	// Статистика
	valuesCollected int64
	valuesSent      int64
}

// NewSourceValueCollector создает новый коллектор значений
func NewSourceValueCollector(
	queryID, sessionID, sourceAlias string,
	dfClient *client.Client,
	fieldNames []string,
	fieldTypes map[string]iceberg.Type,
	bufferSize int,
) *SourceValueCollector {
	return &SourceValueCollector{
		queryID:     queryID,
		sessionID:   sessionID,
		sourceAlias: sourceAlias,
		client:      dfClient,
		fieldNames:  fieldNames,
		fieldTypes:  fieldTypes,
		bufferSize:  bufferSize,
		buffers:     make(map[string][]iceberg.Literal),
	}
}

// CollectFromBatch извлекает значения из RecordBatch и буферизует их
func (s *SourceValueCollector) CollectFromBatch(batch arrow.RecordBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, fieldName := range s.fieldNames {
		// Поиск индекса колонки по имени поля
		colIndex := s.findColumnIndexByName(batch, fieldName)
		if colIndex < 0 {
			continue // поле не найдено в этом батче
		}

		col := batch.Column(colIndex)
		values, err := s.extractValues(col, s.fieldTypes[fieldName])
		if err != nil {
			return fmt.Errorf("failed to extract values for field %s: %w", fieldName, err)
		}

		// Добавление в буфер
		s.buffers[fieldName] = append(s.buffers[fieldName], values...)
		s.valuesCollected += int64(len(values))

		// Отправка если буфер заполнен
		if len(s.buffers[fieldName]) >= s.bufferSize {
			if err := s.flushFieldUnsafe(fieldName); err != nil {
				return err
			}
		}
	}

	return nil
}

// findColumnIndexByName находит индекс колонки по имени поля
func (s *SourceValueCollector) findColumnIndexByName(batch arrow.RecordBatch, fieldName string) int {
	schema := batch.Schema()
	for i, field := range schema.Fields() {
		if field.Name == fieldName {
			return i
		}
	}
	return -1
}

// extractValues извлекает значения из Arrow колонки
func (s *SourceValueCollector) extractValues(col arrow.Array, fieldType iceberg.Type) ([]iceberg.Literal, error) {
	n := col.Len()
	values := make([]iceberg.Literal, 0, n)

	// Обработка в зависимости от типа
	switch arr := col.(type) {
	case *array.Boolean:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(arr.Value(i)))
			}
		}

	case *array.Int32:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(arr.Value(i)))
			}
		}

	case *array.Int64:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				switch fieldType {
				case iceberg.PrimitiveTypes.Date:
					values = append(values, iceberg.NewLiteral(iceberg.Date(arr.Value(i))))
				case iceberg.PrimitiveTypes.Time:
					values = append(values, iceberg.NewLiteral(iceberg.Time(arr.Value(i))))
				case iceberg.PrimitiveTypes.Timestamp, iceberg.PrimitiveTypes.TimestampTz:
					values = append(values, iceberg.NewLiteral(iceberg.Timestamp(arr.Value(i))))
				default:
					values = append(values, iceberg.NewLiteral(arr.Value(i)))
				}
			}
		}

	case *array.Float32:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(arr.Value(i)))
			}
		}

	case *array.Float64:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(arr.Value(i)))
			}
		}

	case *array.String:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(arr.Value(i)))
			}
		}

	case *array.Binary:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(arr.Value(i)))
			}
		}

	case *array.Date32:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(iceberg.Date(arr.Value(i))))
			}
		}

	case *array.Time64:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(iceberg.Time(arr.Value(i))))
			}
		}

	case *array.Timestamp:
		for i := 0; i < n; i++ {
			if col.IsValid(i) {
				values = append(values, iceberg.NewLiteral(iceberg.Timestamp(arr.Value(i))))
			}
		}

	default:
		// Для сложных типов пока не поддерживается извлечение
		// Можно расширить при необходимости
	}

	return values, nil
}

// FlushValues принудительно отправляет все буферизированные значения
func (s *SourceValueCollector) FlushValues(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for fieldName := range s.buffers {
		if len(s.buffers[fieldName]) > 0 {
			if err := s.flushFieldUnsafe(fieldName); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *SourceValueCollector) flushFieldUnsafe(fieldName string) error {
	values := s.buffers[fieldName]
	if len(values) == 0 {
		return nil
	}

	// Отправка значений
	if err := s.client.SendValues(context.Background(), s.sourceAlias, fieldName, values, s.fieldTypes[fieldName]); err != nil {
		return fmt.Errorf("failed to send values for field %s: %w", fieldName, err)
	}

	s.valuesSent += int64(len(values))
	s.buffers[fieldName] = s.buffers[fieldName][:0]

	return nil
}

// SignalComplete сигнализирует о завершении сбора значений
func (s *SourceValueCollector) SignalComplete(ctx context.Context) error {
	// Сначала отправляем оставшиеся значения
	if err := s.FlushValues(ctx); err != nil {
		return err
	}

	// Сигнал для каждого поля
	for _, fieldName := range s.fieldNames {
		if err := s.client.SignalSourceComplete(ctx, s.sourceAlias, fieldName); err != nil {
			return fmt.Errorf("failed to signal complete for field %s: %w", fieldName, err)
		}
	}

	return nil
}

// Stats возвращает статистику сбора значений
func (s *SourceValueCollector) Stats() (collected, sent int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.valuesCollected, s.valuesSent
}

// TargetFilterApplier применяет динамические фильтры к target таблицам
type TargetFilterApplier struct {
	mu          sync.RWMutex
	queryID     string
	sessionID   string
	targetAlias string
	client      *client.Client
	fieldNames  []string
	fieldTypes  map[string]iceberg.Type
	timeout     int // секунды

	// Примененные фильтры
	appliedFilters map[string]*types.DynamicFilter
}

// NewTargetFilterApplier создает новый аппликатор фильтров
func NewTargetFilterApplier(
	queryID, sessionID, targetAlias string,
	dfClient *client.Client,
	fieldNames []string,
	fieldTypes map[string]iceberg.Type,
	timeoutSec int,
) *TargetFilterApplier {
	return &TargetFilterApplier{
		queryID:        queryID,
		sessionID:      sessionID,
		targetAlias:    targetAlias,
		client:         dfClient,
		fieldNames:     fieldNames,
		fieldTypes:     fieldTypes,
		timeout:        timeoutSec,
		appliedFilters: make(map[string]*types.DynamicFilter),
	}
}

// WaitForFilters ожидает готовности фильтров для всех полей
func (t *TargetFilterApplier) WaitForFilters(ctx context.Context) error {
	for _, fieldName := range t.fieldNames {
		filter, err := t.WaitForFilter(ctx, fieldName)
		if err != nil {
			return fmt.Errorf("failed to wait for filter for field %s: %w", fieldName, err)
		}
		if filter != nil {
			t.appliedFilters[fieldName] = filter
		}
	}
	return nil
}

// WaitForFilter ожидает готовности фильтра для конкретного поля
func (t *TargetFilterApplier) WaitForFilter(ctx context.Context, fieldName string) (*types.DynamicFilter, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Проверка кэша
	if filter, ok := t.appliedFilters[fieldName]; ok {
		return filter, nil
	}

	// Ожидание от координатора
	timeout := t.timeout
	if timeout == 0 {
		timeout = int(types.DefaultWaitTimeout.Seconds())
	}

	filter, err := t.client.WaitForFilter(ctx, t.targetAlias, fieldName, 0)
	if err != nil {
		return nil, err
	}

	if filter != nil {
		t.appliedFilters[fieldName] = filter
	}

	return filter, nil
}

// BuildFilterExpression строит BooleanExpression для поля
func (t *TargetFilterApplier) BuildFilterExpression(fieldName string, fieldRef iceberg.UnboundTerm) (iceberg.BooleanExpression, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	filter, ok := t.appliedFilters[fieldName]
	if !ok || filter == nil {
		return iceberg.AlwaysTrue{}, nil
	}

	return filter.BuildExpression(fieldRef), nil
}

// BuildCombinedFilterExpression строит комбинированный фильтр для всех полей
func (t *TargetFilterApplier) BuildCombinedFilterExpression(
	fieldRefs map[string]iceberg.UnboundTerm,
) (iceberg.BooleanExpression, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var predicates []iceberg.BooleanExpression

	for fieldName, filter := range t.appliedFilters {
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

// GetAppliedFilters возвращает примененные фильтры
func (t *TargetFilterApplier) GetAppliedFilters() map[string]*types.DynamicFilter {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string]*types.DynamicFilter, len(t.appliedFilters))
	for k, v := range t.appliedFilters {
		result[k] = v
	}
	return result
}

// HasFilters проверяет, есть ли примененные фильтры
func (t *TargetFilterApplier) HasFilters() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, filter := range t.appliedFilters {
		if filter != nil && filter.FilterType != types.FilterTypeUnspecified {
			return true
		}
	}
	return false
}
