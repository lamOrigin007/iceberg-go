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
	"iter"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter/client"
	"github.com/apache/iceberg-go/dynamicfilter/types"
)

// FDWArrowStream обертка для Arrow RecordBatch итератора с поддержкой динамических фильтров
// для использования в Postgres FDW
type FDWArrowStream struct {
	mu sync.RWMutex

	// Внутренний итератор записей
	inner iter.Seq2[arrow.RecordBatch, error]

	// Коллектор для source таблиц (если таблица является источником)
	collector *SourceValueCollector

	// Аппликатор для target таблиц (если таблица является целью)
	filterApplier *TargetFilterApplier

	// Состояние
	queryID        string
	sessionID      string
	tableAlias     string
	isSource       bool
	isTarget       bool
	currentBatch   arrow.RecordBatch
	currentError   error
	done           bool
	batchesRead    int64
	rowsRead       int64
}

// FDWStreamConfig конфигурация для FDWArrowStream
type FDWStreamConfig struct {
	QueryID       string
	SessionID     string
	TableAlias    string
	IsSource      bool
	IsTarget      bool
	SourceFields  []int           // field IDs для извлечения значений
	SourceFieldTypes map[int]iceberg.Type
	TargetFields  []int           // field IDs для применения фильтров
	TargetFieldTypes map[int]iceberg.Type
	BufferSize    int
	WaitTimeout   int // секунды
}

// NewFDWArrowStream создает новый FDWArrowStream
func NewFDWArrowStream(
	ctx context.Context,
	inner iter.Seq2[arrow.RecordBatch, error],
	dfClient *client.Client,
	config FDWStreamConfig,
) (*FDWArrowStream, error) {

	stream := &FDWArrowStream{
		inner:       inner,
		queryID:     config.QueryID,
		sessionID:   config.SessionID,
		tableAlias:  config.TableAlias,
		isSource:    config.IsSource,
		isTarget:    config.IsTarget,
	}

	// Инициализация коллектора для source таблиц
	if config.IsSource && dfClient != nil {
		stream.collector = NewSourceValueCollector(
			config.QueryID,
			config.SessionID,
			config.TableAlias,
			dfClient,
			config.SourceFields,
			config.SourceFieldTypes,
			config.BufferSize,
		)
	}

	// Инициализация аппликатора для target таблиц
	if config.IsTarget && dfClient != nil {
		stream.filterApplier = NewTargetFilterApplier(
			config.QueryID,
			config.SessionID,
			config.TableAlias,
			dfClient,
			config.TargetFields,
			config.TargetFieldTypes,
			config.WaitTimeout,
		)

		// Ожидание фильтров перед началом чтения
		if err := stream.filterApplier.WaitForFilters(ctx); err != nil {
			return nil, fmt.Errorf("failed to wait for filters: %w", err)
		}
	}

	return stream, nil
}

// Next возвращает следующий RecordBatch из итератора
// Этот метод предназначен для использования в императивном стиле
func (f *FDWArrowStream) Next() (arrow.RecordBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return nil, nil
	}

	// Чтение следующего батча из внутреннего итератора
	// Поскольку iter.Seq2 не поддерживает итеративный стиль напрямую,
	// используем канал для коммуникации
	if f.currentBatch == nil && !f.done {
		// Первый вызов - запускаем итератор
		f.startIterator()
	}

	// Ожидание готовности батча
	batch := f.currentBatch
	err := f.currentError

	if err != nil || batch == nil {
		f.done = true
		return batch, err
	}

	// Сброс для следующего вызова
	f.currentBatch = nil
	f.currentError = nil

	return batch, nil
}

func (f *FDWArrowStream) startIterator() {
	// Запуск итератора в горутине с отправкой результатов в канал
	ch := make(chan struct {
		batch arrow.RecordBatch
		err   error
	}, 1)

	go func() {
		for batch, err := range f.inner {
			ch <- struct {
				batch arrow.RecordBatch
				err   error
			}{batch: batch, err: err}

			// Ожидание подтверждения обработки
			<-ch
		}
		close(ch)
	}()

	// Чтение первого батча
	result, ok := <-ch
	if !ok {
		f.done = true
		return
	}

	f.currentBatch = result.batch
	f.currentError = result.err

	// Сигнал продолжения
	ch <- struct {
		batch arrow.RecordBatch
		err   error
	}{}
}

// ForEach итерирует по всем RecordBatch с применением коллектора
func (f *FDWArrowStream) ForEach(
	ctx context.Context,
	fn func(arrow.RecordBatch) error,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for batch, err := range f.inner {
		if err != nil {
			return err
		}

		f.mu.Unlock()

		// Применение коллектора если это source таблица
		if f.collector != nil {
			if err := f.collector.CollectFromBatch(batch); err != nil {
				f.mu.Lock()
				return fmt.Errorf("collector error: %w", err)
			}
		}

		// Обновление статистики
		f.batchesRead++
		f.rowsRead += batch.NumRows()

		// Вызов пользовательской функции
		if err := fn(batch); err != nil {
			f.mu.Lock()
			return err
		}

		f.mu.Lock()
	}

	// Сигнал завершения для коллектора
	if f.collector != nil {
		f.mu.Unlock()
		if err := f.collector.SignalComplete(ctx); err != nil {
			f.mu.Lock()
			return fmt.Errorf("signal complete error: %w", err)
		}
		f.mu.Lock()
	}

	return nil
}

// GetIterator возвращает итератор с интегрированным коллектором
func (f *FDWArrowStream) GetIterator(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for batch, err := range f.inner {
			if err != nil {
				yield(nil, err)
				return
			}

			// Применение коллектора если это source таблица
			if f.collector != nil {
				if err := f.collector.CollectFromBatch(batch); err != nil {
					yield(nil, fmt.Errorf("collector error: %w", err))
					return
				}
			}

			// Обновление статистики
			f.mu.Lock()
			f.batchesRead++
			f.rowsRead += batch.NumRows()
			f.mu.Unlock()

			// Возврат батча
			if !yield(batch, nil) {
				return
			}
		}

		// Сигнал завершения для коллектора
		if f.collector != nil {
			if err := f.collector.SignalComplete(ctx); err != nil {
				// Логирование ошибки, но не прерывание итерации
				// так как основная операция уже завершена
			}
		}
	}
}

// GetFilterApplier возвращает аппликатор фильтров для target таблиц
func (f *FDWArrowStream) GetFilterApplier() *TargetFilterApplier {
	return f.filterApplier
}

// GetCollector возвращает коллектор значений для source таблиц
func (f *FDWArrowStream) GetCollector() *SourceValueCollector {
	return f.collector
}

// Stats возвращает статистику чтения
func (f *FDWArrowStream) Stats() (batches, rows int64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.batchesRead, f.rowsRead
}

// IsDone проверяет, завершен ли поток
func (f *FDWArrowStream) IsDone() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.done
}

// Close освобождает ресурсы
func (f *FDWArrowStream) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Освобождение текущего батча
	if f.currentBatch != nil {
		f.currentBatch.Release()
		f.currentBatch = nil
	}

	return nil
}

// FDWStreamManager управляет потоками для множественных таблиц в запросе
type FDWStreamManager struct {
	mu        sync.RWMutex
	client    *client.Client
	queryID   string
	sessionID string
	streams   map[string]*FDWArrowStream // table_alias -> stream
	configs   map[string]FDWStreamConfig
	started   bool
}

// NewFDWStreamManager создает новый менеджер потоков
func NewFDWStreamManager(
	queryID, sessionID string,
	dfClient *client.Client,
) *FDWStreamManager {
	return &FDWStreamManager{
		client:    dfClient,
		queryID:   queryID,
		sessionID: sessionID,
		streams:   make(map[string]*FDWArrowStream),
		configs:   make(map[string]FDWStreamConfig),
	}
}

// RegisterSource регистрирует source таблицу
func (m *FDWStreamManager) RegisterSource(
	tableAlias string,
	fieldIDs []int,
	fieldTypes map[int]iceberg.Type,
	bufferSize int,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.configs[tableAlias] = FDWStreamConfig{
		QueryID:        m.queryID,
		SessionID:      m.sessionID,
		TableAlias:     tableAlias,
		IsSource:       true,
		IsTarget:       false,
		SourceFields:   fieldIDs,
		SourceFieldTypes: fieldTypes,
		BufferSize:     bufferSize,
	}
}

// RegisterTarget регистрирует target таблицу
func (m *FDWStreamManager) RegisterTarget(
	tableAlias string,
	fieldIDs []int,
	fieldTypes map[int]iceberg.Type,
	waitTimeout int,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.configs[tableAlias] = FDWStreamConfig{
		QueryID:        m.queryID,
		SessionID:      m.sessionID,
		TableAlias:     tableAlias,
		IsSource:       false,
		IsTarget:       true,
		TargetFields:   fieldIDs,
		TargetFieldTypes: fieldTypes,
		WaitTimeout:    waitTimeout,
	}
}

// RegisterSourceTarget регистрирует таблицу которая является и source и target
func (m *FDWStreamManager) RegisterSourceTarget(
	tableAlias string,
	sourceFieldIDs []int,
	sourceFieldTypes map[int]iceberg.Type,
	targetFieldIDs []int,
	targetFieldTypes map[int]iceberg.Type,
	bufferSize, waitTimeout int,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.configs[tableAlias] = FDWStreamConfig{
		QueryID:          m.queryID,
		SessionID:        m.sessionID,
		TableAlias:       tableAlias,
		IsSource:         true,
		IsTarget:         true,
		SourceFields:     sourceFieldIDs,
		SourceFieldTypes: sourceFieldTypes,
		TargetFields:     targetFieldIDs,
		TargetFieldTypes: targetFieldTypes,
		BufferSize:       bufferSize,
		WaitTimeout:      waitTimeout,
	}
}

// CreateStream создает поток для таблицы
func (m *FDWStreamManager) CreateStream(
	ctx context.Context,
	tableAlias string,
	inner iter.Seq2[arrow.RecordBatch, error],
) (*FDWArrowStream, error) {
	m.mu.Lock()
	config, ok := m.configs[tableAlias]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("table %q not registered", tableAlias)
	}
	m.mu.Unlock()

	stream, err := NewFDWArrowStream(ctx, inner, m.client, config)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.streams[tableAlias] = stream
	m.mu.Unlock()

	return stream, nil
}

// Start начинает сессию на координаторе
func (m *FDWStreamManager) Start(ctx context.Context, mappings []types.FieldMapping, totalSegments int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return fmt.Errorf("session already started")
	}

	if err := m.client.StartQuery(ctx, m.queryID, m.sessionID, mappings, totalSegments); err != nil {
		return err
	}

	m.started = true
	return nil
}

// Stop завершает сессию на координаторе
func (m *FDWStreamManager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Закрытие всех потоков
	for alias, stream := range m.streams {
		if err := stream.Close(); err != nil {
			// Логирование ошибки
		}
		delete(m.streams, alias)
	}

	if !m.started {
		return nil
	}

	if err := m.client.StopQuery(ctx); err != nil {
		return err
	}

	m.started = false
	return nil
}

// GetStream возвращает поток для таблицы
func (m *FDWStreamManager) GetStream(tableAlias string) (*FDWArrowStream, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stream, ok := m.streams[tableAlias]
	if !ok {
		return nil, fmt.Errorf("stream for table %q not found", tableAlias)
	}

	return stream, nil
}

// WaitForAllTargets ожидает готовности всех target фильтров
func (m *FDWStreamManager) WaitForAllTargets(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for alias, stream := range m.streams {
		if stream.filterApplier != nil {
			if err := stream.filterApplier.WaitForFilters(ctx); err != nil {
				return fmt.Errorf("failed to wait for filters for table %q: %w", alias, err)
			}
		}
	}

	return nil
}

// SignalAllSourcesComplete сигнализирует о завершении всех source таблиц
func (m *FDWStreamManager) SignalAllSourcesComplete(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for alias, stream := range m.streams {
		if stream.collector != nil {
			if err := stream.collector.SignalComplete(ctx); err != nil {
				return fmt.Errorf("failed to signal complete for table %q: %w", alias, err)
			}
		}
	}

	return nil
}
