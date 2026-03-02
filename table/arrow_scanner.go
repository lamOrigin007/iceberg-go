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
	"io"
	"iter"
	"strconv"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/substrait-io/substrait-go/v4/expr"
	"golang.org/x/sync/errgroup"
)

const (
	ScanOptionArrowUseLargeTypes = "arrow.use_large_types"
)

type (
	positionDeletes   = []*arrow.Chunked
	perFilePosDeletes = map[string]positionDeletes
)

func readAllDeleteFiles(ctx context.Context, fs iceio.IO, tasks []FileScanTask, concurrency int) (perFilePosDeletes, error) {
	var (
		deletesPerFile = make(perFilePosDeletes)
		uniqueDeletes  = make(map[string]iceberg.DataFile)
		err            error
	)

	for _, t := range tasks {
		for _, d := range t.DeleteFiles {
			if d.ContentType() != iceberg.EntryContentPosDeletes {
				continue
			}

			if _, ok := uniqueDeletes[d.FilePath()]; !ok {
				uniqueDeletes[d.FilePath()] = d
			}
		}
	}

	if len(uniqueDeletes) == 0 {
		return deletesPerFile, nil
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	perFileChan := make(chan map[string]*arrow.Chunked, concurrency)
	go func() {
		defer close(perFileChan)
		for _, v := range uniqueDeletes {
			g.Go(func() error {
				deletes, err := readDeletes(ctx, fs, v)
				if deletes != nil {
					perFileChan <- deletes
				}

				return err
			})
		}

		err = g.Wait()
	}()

	for deletes := range perFileChan {
		for file, arr := range deletes {
			deletesPerFile[file] = append(deletesPerFile[file], arr)
		}
	}

	return deletesPerFile, err
}

func readDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (_ map[string]*arrow.Chunked, err error) {
	src, err := internal.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	tbl, err := rdr.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	tbl, err = array.UnifyTableDicts(compute.GetAllocator(ctx), tbl)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	filePathCol := tbl.Column(tbl.Schema().FieldIndices("file_path")[0]).Data()
	posCol := tbl.Column(tbl.Schema().FieldIndices("pos")[0]).Data()
	dict := filePathCol.Chunk(0).(*array.Dictionary).Dictionary().(*array.String)

	results := make(map[string]*arrow.Chunked)
	for i := 0; i < dict.Len(); i++ {
		v := dict.Value(i)

		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), compute.NewDatum(v))
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(posCol),
			mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		results[v] = filtered.(*compute.ChunkedDatum).Value
	}

	return results, nil
}

type set[T comparable] map[T]struct{}

func combinePositionalDeletes(mem memory.Allocator, deletes set[int64], start, end int64) arrow.Array {
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()

	for i := start; i < end; i++ {
		if _, ok := deletes[i]; !ok {
			bldr.Append(i)
		}
	}

	return bldr.NewArray()
}

type recProcessFn func(arrow.RecordBatch) (arrow.RecordBatch, error)

func processPositionalDeletes(ctx context.Context, deletes set[int64]) recProcessFn {
	nextIdx, mem := int64(0), compute.GetAllocator(ctx)

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		currentIdx := nextIdx
		nextIdx += r.NumRows()

		indices := combinePositionalDeletes(mem, deletes, currentIdx, nextIdx)
		defer indices.Release()

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(r), compute.NewDatumWithoutOwning(indices))
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

func filterRecords(ctx context.Context, recordFilter expr.Expression) recProcessFn {
	return func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer rec.Release()

		input := compute.NewDatumWithoutOwning(rec)
		mask, err := exprs.ExecuteScalarExpression(ctx, rec.Schema(), recordFilter, input)
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		result, err := compute.Filter(ctx, input, mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		return result.(*compute.RecordDatum).Value, nil
	}
}

type arrowScan struct {
	fs              iceio.IO
	metadata        Metadata
	projectedSchema *iceberg.Schema
	boundRowFilter  iceberg.BooleanExpression
	caseSensitive   bool
	rowLimit        int64
	options         iceberg.Properties

	useLargeTypes bool
	concurrency   int

	nameMapping iceberg.NameMapping
	
	// valueCollector для сбора значений из RecordBatch
	valueCollector ValueCollector
	
	// lazyFilterProvider для применения фильтров во время сканирования
	lazyFilterProvider     FilterProvider
	lazyFilterCheckTimeout time.Duration
	lazyFilterApplied      bool
	lazyFilterMu           sync.RWMutex
	lazyFilterFunc         recProcessFn
	lazyBatchesProcessed   int64
	lazyCheckInterval      int64
}

func (as *arrowScan) projectedFieldIDs() (set[int], error) {
	idset := set[int]{}
	for _, id := range as.projectedSchema.FieldIDs() {
		typ, _ := as.projectedSchema.FindTypeByID(id)
		switch typ.(type) {
		case *iceberg.MapType, *iceberg.ListType:
		default:
			idset[id] = struct{}{}
		}
	}

	if as.boundRowFilter != nil {
		extracted, err := iceberg.ExtractFieldIDs(as.boundRowFilter)
		if err != nil {
			return nil, err
		}

		for _, id := range extracted {
			idset[id] = struct{}{}
		}
	}

	return idset, nil
}

// checkAndUpdateFilter проверяет готовность динамического фильтра и применяет его
func (as *arrowScan) checkAndUpdateFilter(ctx context.Context) {
	if !as.lazyFilterApplied && as.lazyFilterProvider != nil {
		as.lazyFilterMu.Lock()
		defer as.lazyFilterMu.Unlock()

		if !as.lazyFilterApplied {
			waitCtx, cancel := context.WithTimeout(ctx, as.lazyFilterCheckTimeout)
			defer cancel()

			if as.lazyFilterProvider.WaitForFilters(waitCtx, as.lazyFilterCheckTimeout) {
				additionalFilter := as.lazyFilterProvider.GetFilter()
				if additionalFilter != nil && !additionalFilter.Equals(iceberg.AlwaysTrue{}) {
					// Создаем filterFunc для динамического фильтра
					as.lazyFilterFunc = as.buildLazyFilterFunc(ctx, additionalFilter)
				}
				as.lazyFilterApplied = true
			}
		}
	}
}

// buildLazyFilterFunc строит функцию фильтрации для динамического фильтра
func (as *arrowScan) buildLazyFilterFunc(ctx context.Context, filter iceberg.BooleanExpression) recProcessFn {
	// Конвертируем iceberg.BooleanExpression в substrait.Expression
	extSet, substraitFilter, err := substrait.ConvertExpr(as.projectedSchema, filter, as.caseSensitive)
	if err != nil {
		// Если конвертация не удалась, возвращаем функцию которая пропускает все записи
		return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
			r.Retain()
			return r, nil
		}
	}

	ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*extSet))

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		input := compute.NewDatumWithoutOwning(r)
		mask, err := exprs.ExecuteScalarExpression(ctx, r.Schema(), substraitFilter, input)
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		result, err := compute.Filter(ctx, input, mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		return result.(*compute.RecordDatum).Value, nil
	}
}

// canDropFileByLazyFilter проверяет можно ли пропустить файл целиком на основе метаданных
// и примененного lazyFilter. Использует статистику файла (min/max значения) для оценки.
func (as *arrowScan) canDropFileByLazyFilter(ctx context.Context, file iceberg.DataFile, fileSchema *iceberg.Schema) (bool, error) {
	if !as.lazyFilterApplied || as.lazyFilterFunc == nil {
		return false, nil
	}

	// Получаем фильтр из провайдера для оценки метаданных
	additionalFilter := as.lazyFilterProvider.GetFilter()
	if additionalFilter == nil || additionalFilter.Equals(iceberg.AlwaysTrue{}) {
		return false, nil
	}

	// Транслируем фильтр на схему файла
	translatedFilter, err := iceberg.TranslateColumnNames(additionalFilter, fileSchema)
	if err != nil {
		return false, err
	}

	if translatedFilter.Equals(iceberg.AlwaysFalse{}) {
		return true, nil
	}

	// Привязываем фильтр к схеме файла используя custom bind visitor
	// который не вызывает ошибку для уже bound predicates
	boundFilter, err := bindExprWithExistingBound(fileSchema, translatedFilter, as.caseSensitive)
	if err != nil {
		return false, err
	}

	if boundFilter.Equals(iceberg.AlwaysTrue{}) {
		return false, nil
	}

	// Создаем evaluator для проверки метаданных файла
	// rowsCannotMatch = false означает что файл можно пропустить
	canMatch, err := as.evaluateFileMetrics(fileSchema, boundFilter, file)
	if err != nil {
		return false, err
	}

	return !canMatch, nil
}

// bindExprWithExistingBound привязывает фильтр к схеме, сохраняя уже bound predicates
func bindExprWithExistingBound(schema *iceberg.Schema, filter iceberg.BooleanExpression, caseSensitive bool) (iceberg.BooleanExpression, error) {
	return iceberg.VisitExpr(filter, &lazyBindVisitor{schema: schema, caseSensitive: caseSensitive})
}

type lazyBindVisitor struct {
	schema        *iceberg.Schema
	caseSensitive bool
}

func (b *lazyBindVisitor) VisitTrue() iceberg.BooleanExpression  { return iceberg.AlwaysTrue{} }
func (b *lazyBindVisitor) VisitFalse() iceberg.BooleanExpression { return iceberg.AlwaysFalse{} }
func (b *lazyBindVisitor) VisitNot(child iceberg.BooleanExpression) iceberg.BooleanExpression {
	return iceberg.NewNot(child)
}
func (b *lazyBindVisitor) VisitAnd(left, right iceberg.BooleanExpression) iceberg.BooleanExpression {
	return iceberg.NewAnd(left, right)
}
func (b *lazyBindVisitor) VisitOr(left, right iceberg.BooleanExpression) iceberg.BooleanExpression {
	return iceberg.NewOr(left, right)
}
func (b *lazyBindVisitor) VisitUnbound(pred iceberg.UnboundPredicate) iceberg.BooleanExpression {
	expr, err := pred.Bind(b.schema, b.caseSensitive)
	if err != nil {
		panic(err)
	}
	return expr
}
func (b *lazyBindVisitor) VisitBound(pred iceberg.BoundPredicate) iceberg.BooleanExpression {
	// Возвращаем bound predicate как есть вместо panic
	return pred
}

// evaluateFileMetrics проверяет метаданные файла против фильтра
func (as *arrowScan) evaluateFileMetrics(fileSchema *iceberg.Schema, filter iceberg.BooleanExpression, file iceberg.DataFile) (bool, error) {
	// Используем existing inclusiveMetricsEval логику
	// Сначала делаем RewriteNotExpr для инверсии фильтра, затем bind с сохранением bound predicates
	rewritten, err := iceberg.RewriteNotExpr(filter)
	if err != nil {
		return false, err
	}

	// Привязываем переписанный фильтр к схеме (если он еще не привязан)
	boundRewritten, err := bindExprWithExistingBound(fileSchema, rewritten, as.caseSensitive)
	if err != nil {
		return false, err
	}

	eval := &inclusiveMetricsEval{
		st:                fileSchema.AsStruct(),
		includeEmptyFiles: false,
		expr:              boundRewritten,
	}

	return eval.Eval(file)
}

type enumeratedRecord struct {
	Record internal.Enumerated[arrow.RecordBatch]
	Task   internal.Enumerated[FileScanTask]
	Err    error
}

func (as *arrowScan) prepareToRead(ctx context.Context, file iceberg.DataFile) (*iceberg.Schema, []int, internal.FileReader, error) {
	ids, err := as.projectedFieldIDs()
	if err != nil {
		return nil, nil, nil, err
	}

	src, err := internal.GetFile(ctx, as.fs, file, false)
	if err != nil {
		return nil, nil, nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	fileSchema, colIndices, err := rdr.PrunedSchema(ids, as.nameMapping)
	if err != nil {
		rdr.Close()

		return nil, nil, nil, err
	}

	iceSchema, err := ArrowSchemaToIceberg(fileSchema, false, as.nameMapping)
	if err != nil {
		rdr.Close()

		return nil, nil, nil, err
	}

	return iceSchema, colIndices, rdr, nil
}

func (as *arrowScan) getRecordFilter(ctx context.Context, fileSchema *iceberg.Schema) (recProcessFn, bool, error) {
	if as.boundRowFilter == nil || as.boundRowFilter.Equals(iceberg.AlwaysTrue{}) {
		return nil, false, nil
	}

	translatedFilter, err := iceberg.TranslateColumnNames(as.boundRowFilter, fileSchema)
	if err != nil {
		return nil, false, err
	}

	if translatedFilter.Equals(iceberg.AlwaysFalse{}) {
		return nil, true, nil
	}

	translatedFilter, err = iceberg.BindExpr(fileSchema, translatedFilter, as.caseSensitive)
	if err != nil {
		return nil, false, err
	}

	if !translatedFilter.Equals(iceberg.AlwaysTrue{}) {
		extSet, recordFilter, err := substrait.ConvertExpr(fileSchema, translatedFilter, as.caseSensitive)
		if err != nil {
			return nil, false, err
		}

		ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*extSet))

		return filterRecords(ctx, recordFilter), false, nil
	}

	return nil, false, nil
}

func (as *arrowScan) processRecords(
	ctx context.Context,
	task internal.Enumerated[FileScanTask],
	fileSchema *iceberg.Schema,
	rdr internal.FileReader,
	columns []int,
	pipeline []recProcessFn,
	out chan<- enumeratedRecord,
) (err error) {
	var (
		testRowGroups any
		recRdr        array.RecordReader
	)

	switch task.Value.File.FileFormat() {
	case iceberg.ParquetFile:
		testRowGroups, err = newParquetRowGroupStatsEvaluator(fileSchema, as.boundRowFilter, false)
		if err != nil {
			return err
		}
	}

	recRdr, err = rdr.GetRecords(ctx, columns, testRowGroups)
	if err != nil {
		return err
	}
	defer recRdr.Release()

	var (
		idx  int
		prev arrow.RecordBatch
	)

	for recRdr.Next() {
		if prev != nil {
			out <- enumeratedRecord{Record: internal.Enumerated[arrow.RecordBatch]{
				Value: prev, Index: idx, Last: false,
			}, Task: task}
			idx++
		}

		prev = recRdr.RecordBatch()
		prev.Retain()

		// Вызов valueCollector для сбора значений (если установлен)
		if as.valueCollector != nil {
			if err := as.valueCollector.Collect(prev); err != nil {
				return err
			}
		}

		for _, f := range pipeline {
			prev, err = f(prev)
			if err != nil {
				return err
			}
		}
		
		// Применение динамического фильтра если есть
		as.lazyFilterMu.RLock()
		lazyFunc := as.lazyFilterFunc
		as.lazyFilterMu.RUnlock()
		
		if lazyFunc != nil {
			prev, err = lazyFunc(prev)
			if err != nil {
				return err
			}
		}
		
		// Периодическая проверка готовности фильтра
		as.lazyBatchesProcessed++
		if !as.lazyFilterApplied && as.lazyFilterProvider != nil &&
		   as.lazyBatchesProcessed % as.lazyCheckInterval == 0 {
			as.checkAndUpdateFilter(ctx)
		}
	}

	if prev != nil {
		out <- enumeratedRecord{Record: internal.Enumerated[arrow.RecordBatch]{
			Value: prev, Index: idx, Last: true,
		}, Task: task}
	}

	if recRdr.Err() != nil && recRdr.Err() != io.EOF {
		err = recRdr.Err()
	}

	return err
}

func (as *arrowScan) recordsFromTask(ctx context.Context, task internal.Enumerated[FileScanTask], out chan<- enumeratedRecord, positionalDeletes positionDeletes) (err error) {
	defer func() {
		if err != nil {
			out <- enumeratedRecord{Task: task, Err: err}
		}
	}()

	var (
		rdr        internal.FileReader
		iceSchema  *iceberg.Schema
		colIndices []int
		filterFunc recProcessFn
		dropFile   bool
	)

	iceSchema, colIndices, rdr, err = as.prepareToRead(ctx, task.Value.File)
	if err != nil {
		return err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	// Проверка готовности фильтра перед началом обработки файла
	as.checkAndUpdateFilter(ctx)

	// Проверка можно ли пропустить файл целиком на основе метаданных и lazyFilter
	if as.lazyFilterApplied {
		dropFile, err = as.canDropFileByLazyFilter(ctx, task.Value.File, iceSchema)
		if err != nil {
			return err
		}

		if dropFile {
			var emptySchema *arrow.Schema
			emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
			if err != nil {
				return err
			}
			out <- enumeratedRecord{Task: task, Record: internal.Enumerated[arrow.RecordBatch]{
				Value: array.NewRecordBatch(emptySchema, nil, 0), Index: 0, Last: true,
			}}

			return nil
		}
	}

	pipeline := make([]recProcessFn, 0, 2)
	if len(positionalDeletes) > 0 {
		deletes := set[int64]{}
		for _, chunk := range positionalDeletes {
			for _, a := range chunk.Chunks() {
				for _, v := range a.(*array.Int64).Int64Values() {
					deletes[v] = struct{}{}
				}
			}
		}

		pipeline = append(pipeline, processPositionalDeletes(ctx, deletes))
	}

	filterFunc, dropFile, err = as.getRecordFilter(ctx, iceSchema)
	if err != nil {
		return err
	}

	if dropFile {
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		out <- enumeratedRecord{Task: task, Record: internal.Enumerated[arrow.RecordBatch]{
			Value: array.NewRecordBatch(emptySchema, nil, 0), Index: 0, Last: true,
		}}

		return err
	}

	if filterFunc != nil {
		pipeline = append(pipeline, filterFunc)
	}

	pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		return ToRequestedSchema(ctx, as.projectedSchema, iceSchema, r, false, false, as.useLargeTypes)
	})

	err = as.processRecords(ctx, task, iceSchema, rdr, colIndices, pipeline, out)

	// Вызов Finalize после завершения обработки (если это последний task)
	if err == nil && as.valueCollector != nil && task.Last {
		if err := as.valueCollector.Finalize(); err != nil {
			return err
		}
	}

	return err
}

func createIterator(ctx context.Context, numWorkers uint, records <-chan enumeratedRecord, deletesPerFile perFilePosDeletes, cancel context.CancelCauseFunc, rowLimit int64) iter.Seq2[arrow.RecordBatch, error] {
	isBeforeAny := func(batch enumeratedRecord) bool {
		return batch.Task.Index < 0
	}

	sequenced := internal.MakeSequencedChan(uint(numWorkers), records,
		func(left, right *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*left):
				return true
			case isBeforeAny(*right):
				return false
			case left.Err != nil || right.Err != nil:
				return true
			case left.Task.Index == right.Task.Index:
				return left.Record.Index < right.Record.Index
			default:
				return left.Task.Index < right.Task.Index
			}
		}, func(prev, next *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*prev):
				return next.Task.Index == 0 && next.Record.Index == 0
			case next.Err != nil:
				return true
			case prev.Task.Index == next.Task.Index:
				return next.Record.Index == prev.Record.Index+1
			default:
				return next.Task.Index == prev.Task.Index+1 &&
					prev.Record.Last && next.Record.Index == 0
			}
		}, enumeratedRecord{Task: internal.Enumerated[FileScanTask]{Index: -1}})

	totalRowCount := int64(0)

	return func(yield func(arrow.RecordBatch, error) bool) {
		defer func() {
			for rec := range sequenced {
				if rec.Record.Value != nil {
					rec.Record.Value.Release()
				}
			}

			for _, v := range deletesPerFile {
				for _, chunk := range v {
					chunk.Release()
				}
			}
		}()

		defer cancel(nil)

		for {
			select {
			case <-ctx.Done():
				if err := context.Cause(ctx); err != nil {
					yield(nil, err)
				}

				return
			case enum, ok := <-sequenced:
				if !ok {
					return
				}

				if enum.Err != nil {
					yield(nil, enum.Err)

					return
				}

				rec := enum.Record.Value
				if rowLimit > 0 {
					if totalRowCount >= rowLimit {
						rec.Release()

						return
					} else if totalRowCount+rec.NumRows() > rowLimit {
						defer rec.Release()
						rec = rec.NewSlice(0, rowLimit-totalRowCount)
					}
				}

				if rec.NumRows() == 0 {
					// skip empty records
					continue
				}

				if !yield(rec, nil) {
					return
				}
				totalRowCount += rec.NumRows()
				if rowLimit > 0 && totalRowCount >= rowLimit {
					return
				}
			}
		}
	}
}

func (as *arrowScan) recordBatchesFromTasksAndDeletes(ctx context.Context, tasks []FileScanTask, deletesPerFile perFilePosDeletes) iter.Seq2[arrow.RecordBatch, error] {
	extSet := substrait.NewExtensionSet()
	as.nameMapping = as.metadata.NameMapping()

	ctx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan internal.Enumerated[FileScanTask], len(tasks))

	// numWorkers := 1
	numWorkers := min(as.concurrency, len(tasks))
	records := make(chan enumeratedRecord, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}

					if err := as.recordsFromTask(ctx, task, records,
						deletesPerFile[task.Value.File.FilePath()]); err != nil {
						cancel(err)

						return
					}
				}
			}
		}()
	}

	go func() {
		for i, t := range tasks {
			taskChan <- internal.Enumerated[FileScanTask]{
				Value: t, Index: i, Last: i == len(tasks)-1,
			}
		}
		close(taskChan)

		wg.Wait()
		close(records)
	}()

	return createIterator(ctx, uint(numWorkers), records, deletesPerFile,
		cancel, as.rowLimit)
}

func (as *arrowScan) GetRecords(ctx context.Context, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	var err error
	as.useLargeTypes, err = strconv.ParseBool(as.options.Get(ScanOptionArrowUseLargeTypes, "false"))
	if err != nil {
		as.useLargeTypes = false
	}

	resultSchema, err := SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
	if err != nil {
		return nil, nil, err
	}

	if as.rowLimit == 0 {
		return resultSchema, func(yield func(arrow.RecordBatch, error) bool) {}, nil
	}

	deletesPerFile, err := readAllDeleteFiles(ctx, as.fs, tasks, as.concurrency)
	if err != nil {
		return nil, nil, err
	}

	return resultSchema, as.recordBatchesFromTasksAndDeletes(ctx, tasks, deletesPerFile), nil
}
