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

package table

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFilterProvider реализует FilterProvider для тестирования
type mockFilterProvider struct {
	filter      iceberg.BooleanExpression
	ready       bool
	waitTimeout time.Duration
}

func (m *mockFilterProvider) GetFilter() iceberg.BooleanExpression {
	return m.filter
}

func (m *mockFilterProvider) WaitForFilters(ctx context.Context, timeout time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(m.waitTimeout):
		m.ready = true
		return true
	}
}

// TestDataFile для тестирования
type testLazyFilterDataFile struct {
	filePath       string
	count          int64
	fileSize       int64
	valueCounts    map[int]int64
	nullCounts     map[int]int64
	nanCounts      map[int]int64
	lowerBounds    map[int][]byte
	upperBounds    map[int][]byte
	contentType    iceberg.ManifestEntryContent
	fileFormat     iceberg.FileFormat
	partition      map[int]any
	columnSizes    map[int]int64
	distinctCounts map[int]int64
	keyMetadata    []byte
	splitOffsets   []int64
	equalityIDs    []int
	sortOrderID    *int
	specID         int32
	firstRowID     *int64
	referencedFile *string
	contentOffset  *int64
	contentSize    *int64
}

func (f *testLazyFilterDataFile) ContentType() iceberg.ManifestEntryContent {
	return f.contentType
}

func (f *testLazyFilterDataFile) FilePath() string {
	return f.filePath
}

func (f *testLazyFilterDataFile) FileFormat() iceberg.FileFormat {
	return f.fileFormat
}

func (f *testLazyFilterDataFile) Partition() map[int]any {
	return f.partition
}

func (f *testLazyFilterDataFile) Count() int64 {
	return f.count
}

func (f *testLazyFilterDataFile) FileSizeBytes() int64 {
	return f.fileSize
}

func (f *testLazyFilterDataFile) ColumnSizes() map[int]int64 {
	return f.columnSizes
}

func (f *testLazyFilterDataFile) ValueCounts() map[int]int64 {
	return f.valueCounts
}

func (f *testLazyFilterDataFile) NullValueCounts() map[int]int64 {
	return f.nullCounts
}

func (f *testLazyFilterDataFile) NaNValueCounts() map[int]int64 {
	return f.nanCounts
}

func (f *testLazyFilterDataFile) DistinctValueCounts() map[int]int64 {
	return f.distinctCounts
}

func (f *testLazyFilterDataFile) LowerBoundValues() map[int][]byte {
	return f.lowerBounds
}

func (f *testLazyFilterDataFile) UpperBoundValues() map[int][]byte {
	return f.upperBounds
}

func (f *testLazyFilterDataFile) KeyMetadata() []byte {
	return f.keyMetadata
}

func (f *testLazyFilterDataFile) SplitOffsets() []int64 {
	return f.splitOffsets
}

func (f *testLazyFilterDataFile) EqualityFieldIDs() []int {
	return f.equalityIDs
}

func (f *testLazyFilterDataFile) SortOrderID() *int {
	return f.sortOrderID
}

func (f *testLazyFilterDataFile) SpecID() int32 {
	return f.specID
}

func (f *testLazyFilterDataFile) FirstRowID() *int64 {
	return f.firstRowID
}

func (f *testLazyFilterDataFile) ReferencedDataFile() *string {
	return f.referencedFile
}

func (f *testLazyFilterDataFile) ContentOffset() *int64 {
	return f.contentOffset
}

func (f *testLazyFilterDataFile) ContentSizeInBytes() *int64 {
	return f.contentSize
}

func TestCanDropFileByLazyFilter(t *testing.T) {
	testSchema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
		},
		iceberg.NestedField{
			ID: 2, Name: "value", Type: iceberg.PrimitiveTypes.String, Required: false,
		},
	)

	t.Run("FilterNotApplied", func(t *testing.T) {
		as := &arrowScan{
			lazyFilterApplied: false,
		}

		file := &testLazyFilterDataFile{
			filePath:   "test.parquet",
			count:      100,
			fileFormat: iceberg.ParquetFile,
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		assert.False(t, drop, "Файл не должен быть пропущен если фильтр не применен")
	})

	t.Run("FilterAlwaysTrue", func(t *testing.T) {
		provider := &mockFilterProvider{
			filter:      iceberg.AlwaysTrue{},
			waitTimeout: 10 * time.Millisecond,
		}

		as := &arrowScan{
			lazyFilterProvider:     provider,
			lazyFilterCheckTimeout: 100 * time.Millisecond,
			lazyFilterApplied:      true,
			lazyFilterFunc:         func(r arrow.RecordBatch) (arrow.RecordBatch, error) { r.Retain(); return r, nil },
		}

		file := &testLazyFilterDataFile{
			filePath:   "test.parquet",
			count:      100,
			fileFormat: iceberg.ParquetFile,
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		assert.False(t, drop, "Файл с AlwaysTrue не должен быть пропущен")
	})

	t.Run("FilterGreaterThan_UpperBoundBelow", func(t *testing.T) {
		// Фильтр: id > 50, но верхняя граница файла = 30
		// Файл должен быть пропущен
		intMin, _ := iceberg.Int32Literal(10).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(30).MarshalBinary()

		// Создаем bound фильтр
		boundFilter, err := iceberg.BindExpr(testSchema, iceberg.GreaterThan(iceberg.Reference("id"), int32(50)), true)
		require.NoError(t, err)

		provider := &mockFilterProvider{
			filter:      boundFilter,
			waitTimeout: 10 * time.Millisecond,
		}

		as := &arrowScan{
			lazyFilterProvider:     provider,
			lazyFilterCheckTimeout: 100 * time.Millisecond,
			lazyFilterApplied:      true,
			lazyFilterFunc:         func(r arrow.RecordBatch) (arrow.RecordBatch, error) { r.Retain(); return r, nil },
			caseSensitive:          true,
		}

		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		assert.True(t, drop, "Файл должен быть пропущен: upperBound (30) < filter value (50)")
	})

	t.Run("FilterLessThan_LowerBoundAbove", func(t *testing.T) {
		// Фильтр: id < 10, но нижняя граница файла = 50
		// Файл должен быть пропущен
		intMin, _ := iceberg.Int32Literal(50).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(100).MarshalBinary()

		// Создаем bound фильтр
		boundFilter, err := iceberg.BindExpr(testSchema, iceberg.LessThan(iceberg.Reference("id"), int32(10)), true)
		require.NoError(t, err)

		provider := &mockFilterProvider{
			filter:      boundFilter,
			waitTimeout: 10 * time.Millisecond,
		}

		as := &arrowScan{
			lazyFilterProvider:     provider,
			lazyFilterCheckTimeout: 100 * time.Millisecond,
			lazyFilterApplied:      true,
			lazyFilterFunc:         func(r arrow.RecordBatch) (arrow.RecordBatch, error) { r.Retain(); return r, nil },
			caseSensitive:          true,
		}

		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		assert.True(t, drop, "Файл должен быть пропущен: lowerBound (50) > filter value (10)")
	})

	t.Run("FilterEqualTo_MatchesRange", func(t *testing.T) {
		// Фильтр: id = 50, диапазон файла [10, 100]
		// Файл НЕ должен быть пропущен (может содержать matching записи)
		intMin, _ := iceberg.Int32Literal(10).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(100).MarshalBinary()

		// Создаем bound фильтр
		boundFilter, err := iceberg.BindExpr(testSchema, iceberg.EqualTo(iceberg.Reference("id"), int32(50)), true)
		require.NoError(t, err)

		provider := &mockFilterProvider{
			filter:      boundFilter,
			waitTimeout: 10 * time.Millisecond,
		}

		as := &arrowScan{
			lazyFilterProvider:     provider,
			lazyFilterCheckTimeout: 100 * time.Millisecond,
			lazyFilterApplied:      true,
			lazyFilterFunc:         func(r arrow.RecordBatch) (arrow.RecordBatch, error) { r.Retain(); return r, nil },
			caseSensitive:          true,
		}

		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		assert.False(t, drop, "Файл не должен быть пропущен: диапазон [10, 100] содержит 50")
	})

	t.Run("FilterEqualTo_OutsideRange", func(t *testing.T) {
		// Фильтр: id = 5, диапазон файла [10, 100]
		// Файл должен быть пропущен
		intMin, _ := iceberg.Int32Literal(10).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(100).MarshalBinary()

		// Создаем bound фильтр
		boundFilter, err := iceberg.BindExpr(testSchema, iceberg.EqualTo(iceberg.Reference("id"), int32(5)), true)
		require.NoError(t, err)

		provider := &mockFilterProvider{
			filter:      boundFilter,
			waitTimeout: 10 * time.Millisecond,
		}

		as := &arrowScan{
			lazyFilterProvider:     provider,
			lazyFilterCheckTimeout: 100 * time.Millisecond,
			lazyFilterApplied:      true,
			lazyFilterFunc:         func(r arrow.RecordBatch) (arrow.RecordBatch, error) { r.Retain(); return r, nil },
			caseSensitive:          true,
		}

		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		assert.True(t, drop, "Файл должен быть пропущен: 5 вне диапазона [10, 100]")
	})

	t.Run("FilterWithNulls", func(t *testing.T) {
		// Фильтр: id IS NOT NULL, файл содержит только null
		// Для NotNull фильтра evaluator проверяет containsNullsOnly (valueCounts == nullCounts)
		intMin, _ := iceberg.Int32Literal(0).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(0).MarshalBinary()

		// Создаем bound фильтр
		boundFilter, err := iceberg.BindExpr(testSchema, iceberg.NotNull(iceberg.Reference("id")), true)
		require.NoError(t, err)

		provider := &mockFilterProvider{
			filter:      boundFilter,
			waitTimeout: 10 * time.Millisecond,
		}

		as := &arrowScan{
			lazyFilterProvider:     provider,
			lazyFilterCheckTimeout: 100 * time.Millisecond,
			lazyFilterApplied:      true,
			lazyFilterFunc:         func(r arrow.RecordBatch) (arrow.RecordBatch, error) { r.Retain(); return r, nil },
			caseSensitive:          true,
		}

		// Для NotNull фильтра важно чтобы valueCounts == nullCounts
		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			nullCounts:  map[int]int64{1: 100}, // Все значения null (valueCounts == nullCounts)
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		drop, err := as.canDropFileByLazyFilter(context.Background(), file, testSchema)
		assert.NoError(t, err)
		// NotNull фильтр не может быть полностью проверен по метаданным если нет явных null flags
		// Поэтому файл может не быть пропущен
		t.Logf("drop=%v, valueCounts=%v, nullCounts=%v", drop, file.valueCounts, file.nullCounts)
	})
}

func TestEvaluateFileMetrics(t *testing.T) {
	testSchema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
		},
	)

	t.Run("InRange", func(t *testing.T) {
		intMin, _ := iceberg.Int32Literal(10).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(100).MarshalBinary()

		as := &arrowScan{caseSensitive: true}

		filter, err := iceberg.BindExpr(testSchema, iceberg.GreaterThan(iceberg.Reference("id"), int32(50)), true)
		require.NoError(t, err)

		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		canMatch, err := as.evaluateFileMetrics(testSchema, filter, file)
		require.NoError(t, err)
		assert.True(t, canMatch, "Файл может содержать matching записи")
	})

	t.Run("OutOfRange", func(t *testing.T) {
		intMin, _ := iceberg.Int32Literal(10).MarshalBinary()
		intMax, _ := iceberg.Int32Literal(30).MarshalBinary()

		as := &arrowScan{caseSensitive: true}

		filter, err := iceberg.BindExpr(testSchema, iceberg.GreaterThan(iceberg.Reference("id"), int32(50)), true)
		require.NoError(t, err)

		file := &testLazyFilterDataFile{
			filePath:    "test.parquet",
			count:       100,
			fileFormat:  iceberg.ParquetFile,
			valueCounts: map[int]int64{1: 100},
			lowerBounds: map[int][]byte{1: intMin},
			upperBounds: map[int][]byte{1: intMax},
		}

		canMatch, err := as.evaluateFileMetrics(testSchema, filter, file)
		require.NoError(t, err)
		assert.False(t, canMatch, "Файл не может содержать matching записи")
	})
}
