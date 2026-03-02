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

// ScanValueCollector реализует интерфейс table.ValueCollector для сбора значений
// из RecordBatch во время сканирования Iceberg таблиц.
//
// Этот коллектор используется в Postgres FDW для извлечения значений из source таблиц
// и отправки их на координатор динамических фильтров.
type ScanValueCollector struct {
	mu sync.RWMutex

	queryID     string
	sessionID   string
	sourceAlias string
	client      *client.Client

	// fieldNames - список имен полей для сбора значений
	fieldNames []string
	// fieldTypes - типы полей для корректной конвертации
	fieldTypes map[string]iceberg.Type
	// bufferSize - размер буфера перед отправкой
	bufferSize int

	// buffers хранит буферизированные значения по fieldName
	buffers map[string][]iceberg.Literal

	// Статистика
	valuesCollected int64
	valuesSent      int64
	batchesProcessed int64
}

// ScanValueCollectorConfig конфигурация для ScanValueCollector
type ScanValueCollectorConfig struct {
	QueryID     string
	SessionID   string
	SourceAlias string
	FieldNames  []string
	FieldTypes  map[string]iceberg.Type
	BufferSize  int
}

// NewScanValueCollector создает новый ScanValueCollector для использования с table.Scan
func NewScanValueCollector(cfg ScanValueCollectorConfig, dfClient *client.Client) *ScanValueCollector {
	bufferSize := cfg.BufferSize
	if bufferSize == 0 {
		bufferSize = types.DefaultBatchSize
	}

	return &ScanValueCollector{
		queryID:     cfg.QueryID,
		sessionID:   cfg.SessionID,
		sourceAlias: cfg.SourceAlias,
		client:      dfClient,
		fieldNames:  cfg.FieldNames,
		fieldTypes:  cfg.FieldTypes,
		bufferSize:  bufferSize,
		buffers:     make(map[string][]iceberg.Literal),
	}
}

// Collect вызывается для каждого RecordBatch во время сканирования.
// Извлекает значения из указанных полей и буферизует их для отправки.
func (c *ScanValueCollector) Collect(batch arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.batchesProcessed++

	// Для каждого поля извлекаем значения из батча
	for _, fieldName := range c.fieldNames {
		colIndex := c.findColumnIndexByName(batch, fieldName)
		if colIndex < 0 {
			// Поле не найдено в этом батче - пропускаем
			continue
		}

		col := batch.Column(colIndex)
		values, err := c.extractValues(col, c.fieldTypes[fieldName])
		if err != nil {
			return fmt.Errorf("failed to extract values for field %s: %w", fieldName, err)
		}

		// Добавляем в буфер
		c.buffers[fieldName] = append(c.buffers[fieldName], values...)
		c.valuesCollected += int64(len(values))

		// Если буфер заполнен - отправляем
		if len(c.buffers[fieldName]) >= c.bufferSize {
			if err := c.flushFieldUnsafe(fieldName); err != nil {
				return err
			}
		}
	}

	return nil
}

// Finalize вызывается после завершения сканирования.
// Отправляет оставшиеся буферизированные значения и сигнализирует о завершении.
func (c *ScanValueCollector) Finalize() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Отправляем все оставшиеся значения
	for fieldName := range c.buffers {
		if len(c.buffers[fieldName]) > 0 {
			if err := c.flushFieldUnsafe(fieldName); err != nil {
				return err
			}
		}

		// Сигнализируем о завершении сбора для этого поля
		if err := c.client.SignalSourceComplete(context.Background(), c.sourceAlias, fieldName); err != nil {
			return fmt.Errorf("failed to signal complete for field %s: %w", fieldName, err)
		}
	}

	return nil
}

// findColumnIndexByName находит индекс колонки в RecordBatch по имени поля
func (c *ScanValueCollector) findColumnIndexByName(batch arrow.RecordBatch, fieldName string) int {
	schema := batch.Schema()
	for i, field := range schema.Fields() {
		if field.Name == fieldName {
			return i
		}
	}
	return -1
}

// extractValues извлекает значения из Arrow колонки
func (c *ScanValueCollector) extractValues(col arrow.Array, fieldType iceberg.Type) ([]iceberg.Literal, error) {
	n := col.Len()
	values := make([]iceberg.Literal, 0, n)

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
		// Для сложных типов пока не поддерживается
	}

	return values, nil
}

// flushFieldUnsafe отправляет буферизированные значения для поля (должен быть захвачен lock)
func (c *ScanValueCollector) flushFieldUnsafe(fieldName string) error {
	values := c.buffers[fieldName]
	if len(values) == 0 {
		return nil
	}

	// Отправляем значения
	if err := c.client.SendValues(context.Background(), c.sourceAlias, fieldName, values, c.fieldTypes[fieldName]); err != nil {
		return fmt.Errorf("failed to send values for field %s: %w", fieldName, err)
	}

	c.valuesSent += int64(len(values))
	c.buffers[fieldName] = c.buffers[fieldName][:0]

	return nil
}

// Stats возвращает статистику сбора значений
func (c *ScanValueCollector) Stats() (collected, sent, batches int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.valuesCollected, c.valuesSent, c.batchesProcessed
}

// FlushValues принудительно отправляет все буферизированные значения для поля
func (c *ScanValueCollector) FlushValues(fieldName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.buffers[fieldName]) > 0 {
		return c.flushFieldUnsafe(fieldName)
	}
	return nil
}

// FlushAllValues принудительно отправляет все буферизированные значения для всех полей
func (c *ScanValueCollector) FlushAllValues() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for fieldID := range c.buffers {
		if len(c.buffers[fieldID]) > 0 {
			if err := c.flushFieldUnsafe(fieldID); err != nil {
				return err
			}
		}
	}
	return nil
}
