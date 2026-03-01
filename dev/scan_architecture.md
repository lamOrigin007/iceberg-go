# Архитектура сканирования данных в iceberg-go

## Обзор

Библиотека реализует многоуровневую систему сканирования данных Apache Iceberg с поддержкой:
- Параллельного чтения файлов
- Projection pushdown (выборка только нужных колонок)
- Predicate pushdown (фильтрация на уровне manifest/partition/rowgroup)
- Обработки удалений (positional deletes)
- Конвертации в Arrow формат

---

## Ключевые компоненты

### 1. Scan (`table/scanner.go`)

Основной тип, управляющий процессом сканирования:

```go
type Scan struct {
    metadata       Metadata
    ioF            FSysF
    rowFilter      iceberg.BooleanExpression
    selectedFields []string
    caseSensitive  bool
    snapshotID     *int64
    asOfTimestamp  *int64
    options        iceberg.Properties
    limit          int64
    concurrency    int
}
```

#### Опции сканирования

| Функция | Описание |
|---------|----------|
| `WithSelectedFields(fields...string)` | Projection колонок |
| `WithRowFilter(e iceberg.BooleanExpression)` | Фильтр строк |
| `WithSnapshotID(n int64)` | Time travel по snapshot ID |
| `WithSnapshotAsOf(timeStampMs int64)` | Time travel по timestamp |
| `WithCaseSensitive(b bool)` | Чувствительность к регистру |
| `WithLimit(n int64)` | Лимит строк |
| `WitMaxConcurrency(n int)` | Параллелизм (по умолчанию `runtime.GOMAXPROCS`) |
| `WithOptions(opts iceberg.Properties)` | Дополнительные опции |

#### Константы

```go
const ScanNoLimit = -1  // Значение по умолчанию для лимита строк
```

---

### 2. Процесс сканирования (3 этапа)

#### Этап 1: `fetchPartitionSpecFilteredManifests(ctx)`

Получает отфильтрованные manifest файлы на уровне partition spec:

```go
func (scan *Scan) fetchPartitionSpecFilteredManifests(ctx context.Context) ([]iceberg.ManifestFile, error)
```

**Шаги:**
1. Получает текущий snapshot через `scan.Snapshot()`
2. Загружает manifest list из snapshot
3. Создает manifest evaluators для каждого partition spec
4. Фильтрует manifest файлы через partition statistics

**Используемые evaluators:**
- `manifestEvaluator` — оценка по partition statistics (LowerBound, UpperBound, ContainsNull, ContainsNaN)

#### Этап 2: `collectManifestEntries(ctx, manifestList)`

Параллельно читает manifest файлы и извлекает entries:

```go
func (scan *Scan) collectManifestEntries(
    ctx context.Context,
    manifestList []iceberg.ManifestFile,
) (*manifestEntries, error)
```

**Процесс:**
1. Создает `metricsEval` для фильтрации по column statistics
2. Запускает параллельную обработку manifest файлов через `errgroup.WithContext()`
3. Для каждого manifest:
   - Проверяет sequence number через `checkSequenceNumber()`
   - Применяет partition evaluator
   - Применяет metrics evaluator
   - Добавляет entry в соответствующую категорию:
     - `dataEntries` — данные (EntryContentData)
     - `positionalDeleteEntries` — удаления (EntryContentPosDeletes)
     - `EntryContentEqDeletes` — возвращает ошибку (не поддерживается)

**Параллелизм:**
```go
concurrencyLimit := min(scan.concurrency, len(manifestList))
g.SetLimit(concurrencyLimit)
```

#### Этап 3: `PlanFiles(ctx)`

Сопоставляет удаления с данными и создает FileScanTask:

```go
func (scan *Scan) PlanFiles(ctx context.Context) ([]FileScanTask, error)
```

**Шаги:**
1. Сортирует positional deletes по sequence number
2. Для каждого data entry вызывает `matchDeletesToData()`
3. Возвращает `[]FileScanTask`

**FileScanTask структура:**
```go
type FileScanTask struct {
    File          iceberg.DataFile
    DeleteFiles   []iceberg.DataFile  // Позиционные удаления для файла
    Start, Length int64               // Смещение и длина для partial reads
}
```

#### Функция matchDeletesToData

```go
func matchDeletesToData(entry iceberg.ManifestEntry, positionalDeletes []iceberg.ManifestEntry) ([]iceberg.DataFile, error)
```

Находит удаления, которые относятся к конкретному data файлу:
1. Binary search для нахождения релевантных deletes по sequence number
2. Создает evaluator для проверки `file_path`
3. Фильтрует delete files по matching file path

---

### 3. Система evaluators (`table/evaluators.go`)

#### Manifest Evaluator

Фильтрация на уровне manifest файлов на основе partition statistics:

```go
func newManifestEvaluator(
    spec iceberg.PartitionSpec,
    schema *iceberg.Schema,
    partitionFilter iceberg.BooleanExpression,
    caseSensitive bool,
) (func(iceberg.ManifestFile) (bool, error), error)
```

**manifestEvalVisitor** реализует visitor pattern для BooleanExpression:

| Метод | Описание |
|-------|----------|
| `VisitIn(term, literals)` | Проверка IN predicate по bounds |
| `VisitNotIn(term, literals)` | Всегда `rowsMightMatch` |
| `VisitIsNan(term)` | Проверка ContainsNaN |
| `VisitNotNan(term)` | Проверка на не-NaN значения |
| `VisitIsNull(term)` | Проверка ContainsNull |
| `VisitNotNull(term)` | Проверка на наличие non-null |
| `VisitEqual(term, lit)` | Проверка по lower/upper bounds |
| `VisitNotEqual(term, lit)` | Всегда `rowsMightMatch` |
| `VisitGreaterEqual(term, lit)` | Проверка по upper bound |
| `VisitGreater(term, lit)` | Проверка по upper bound |
| `VisitLessEqual(term, lit)` | Проверка по lower bound |
| `VisitLess(term, lit)` | Проверка по lower bound |
| `VisitStartsWith(term, lit)` | Проверка префикса по bounds |
| `VisitNotStartsWith(term, lit)` | Проверка NOT starts with |

**Возвращаемые значения:**
- `rowsMightMatch = true` — строки могут соответствовать
- `rowsCannotMatch = false` — строки точно не соответствуют

#### Partition Evaluator

Проекция filter выражений на partition spec:

```go
type inclusiveProjection struct {
    spec          iceberg.PartitionSpec
    schema        *iceberg.Schema
    caseSensitive bool
}
```

**Процесс:**
1. Rewrites NOT expressions
2. Bind expression к схеме
3. Для каждого partition field применяет transform projection:
   - bucket, truncate, year, month, day, hour transforms
4. Возвращает конъюнкцию всех partition predicates

#### Metrics Evaluator

Фильтрация на уровне файлов и row groups по column statistics:

```go
type inclusiveMetricsEval struct {
    st                iceberg.StructType
    includeEmptyFiles bool
    expr              iceberg.BooleanExpression
    valueCounts       map[int]int64
    nullCounts        map[int]int64
    nanCounts         map[int]int64
    lowerBounds       map[int][]byte
    upperBounds       map[int][]byte
}
```

**Методы:**

| Метод | Описание |
|-------|----------|
| `TestRowGroup(rgmeta, colIndices)` | Оценка row group по statistics |
| `Eval(file iceberg.DataFile)` | Оценка data file по statistics |
| `VisitIsNull(t)` | Проверка null counts |
| `VisitNotNull(t)` | Проверка на non-null значения |
| `VisitIsNan(t)` | Проверка nan counts |
| `VisitNotNan(t)` | Проверка на не-NaN |
| `VisitLess(t, lit)` | Проверка по lower bound |
| `VisitLessEqual(t, lit)` | Проверка по lower bound |
| `VisitGreaterEqual(t, lit)` | Проверка по upper bound |
| `VisitGreater(t, lit)` | Проверка по upper bound |
| `VisitEqual(t, lit)` | Проверка по lower/upper bounds |
| `VisitIn(t, literals)` | Проверка IN predicate |

**Parquet Row Group Evaluator:**

```go
func newParquetRowGroupStatsEvaluator(
    fileSchema *iceberg.Schema,
    expr iceberg.BooleanExpression,
    includeEmptyFiles bool,
) (func(*metadata.RowGroupMetaData, []int) (bool, error), error)
```

Используется для skip row groups при чтении Parquet файлов.

---

### 4. Arrow Scan (`table/arrow_scanner.go`)

Конвертация Iceberg данных в Arrow RecordBatch с применением фильтров и проекций.

#### arrowScan структура

```go
type arrowScan struct {
    fs              iceio.IO
    metadata        Metadata
    projectedSchema *iceberg.Schema
    boundRowFilter  iceberg.BooleanExpression
    caseSensitive   bool
    rowLimit        int64
    options         iceberg.Properties
    useLargeTypes   bool  // Arrow large_types опция
    concurrency     int
    nameMapping     iceberg.NameMapping
}
```

#### Ключевые методы

**GetRecords()** — основная точка входа:

```go
func (as *arrowScan) GetRecords(
    ctx context.Context,
    tasks []FileScanTask,
) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error)
```

Возвращает:
1. Arrow схему результата
2. Итератор `iter.Seq2[arrow.RecordBatch, error]` для потокового чтения

**recordBatchesFromTasksAndDeletes()** — координирует чтение:

```go
func (as *arrowScan) recordBatchesFromTasksAndDeletes(
    ctx context.Context,
    tasks []FileScanTask,
    deletesPerFile perFilePosDeletes,
) iter.Seq2[arrow.RecordBatch, error]
```

**Шаги:**
1. Запускает workers для параллельной обработки tasks
2. Создает sequenced channel для упорядоченного вывода
3. Применяет row limit при необходимости

**recordsFromTask()** — обработка单个 файла:

```go
func (as *arrowScan) recordsFromTask(
    ctx context.Context,
    task internal.Enumerated[FileScanTask],
    out chan<- enumeratedRecord,
    positionalDeletes positionDeletes,
) error
```

**Pipeline обработки:**
1. `prepareToRead()` — открывает файл, получает pruned schema
2. Построение pipeline обработчиков:
   - `processPositionalDeletes()` — применение deletions
   - `getRecordFilter()` — конвертация filter в Substrait
   - `ToRequestedSchema()` — конвертация в target schema
3. `processRecords()` — чтение и обработка записей

#### Обработка positional deletes

**readAllDeleteFiles()** — читает все delete файлы параллельно:

```go
func readAllDeleteFiles(
    ctx context.Context,
    fs iceio.IO,
    tasks []FileScanTask,
    concurrency int,
) (perFilePosDeletes, error)
```

**readDeletes()** — читает单个 delete файл:

```go
func readDeletes(
    ctx context.Context,
    fs iceio.IO,
    dataFile iceberg.DataFile,
) (map[string]*arrow.Chunked, error)
```

Возвращает map: `file_path -> chunked array of positions`

**processPositionalDeletes()** — создает функцию для применения deletions:

```go
func processPositionalDeletes(
    ctx context.Context,
    deletes set[int64],
) recProcessFn
```

Использует `compute.Take()` с генерацией индексов не-удаленных строк.

#### Фильтрация через Substrait

**filterRecords()** — применяет filter через Arrow compute:

```go
func filterRecords(
    ctx context.Context,
    recordFilter expr.Expression,
) recProcessFn
```

**Процесс:**
1. Конвертирует Iceberg BooleanExpression в Substrait expression
2. Использует `exprs.ExecuteScalarExpression()` для evaluation
3. Применяет `compute.Filter()` для фильтрации записей

#### Конвертация схемы

**ToRequestedSchema()** — конвертирует записи в target schema:

```go
func ToRequestedSchema(
    ctx context.Context,
    targetSchema, fileSchema *iceberg.Schema,
    batch arrow.RecordBatch,
    // ...
) (arrow.RecordBatch, error)
```

Поддерживает:
- Column reordering
- Type promotion
- Default values для missing columns

---

### 5. FileReader Interface (`table/internal/interfaces.go`)

Абстракция над форматами файлов:

```go
type FileReader interface {
    io.Closer
    Metadata() Metadata
    SourceFileSize() int64
    Schema() (*arrow.Schema, error)
    PrunedSchema(projectedIDs map[int]struct{}, mapping iceberg.NameMapping) (*arrow.Schema, []int, error)
    GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error)
    ReadTable(context.Context) (arrow.Table, error)
}
```

#### ParquetFileSource

Единственная реализованная реализация:

```go
type ParquetFileSource struct {
    mem  memory.Allocator
    fs   iceio.IO
    file iceberg.DataFile
}
```

**Методы:**

| Метод | Описание |
|-------|----------|
| `GetReader(ctx)` | Создает Parquet reader |
| `PrunedSchema(ids, mapping)` | Возвращает schema с pruning и column indices |
| `GetRecords(ctx, cols, tester)` | Читает записи с column pruning и row group skipping |
| `ReadTable(ctx)` | Читает весь файл в Arrow Table |

**Column Pruning:**
```go
func (p *parquetFormat) PrunedSchema(
    ids map[int]struct{},
    mapping iceberg.NameMapping,
) (*arrow.Schema, []int, error)
```

Возвращает:
1. Arrow schema только с projected полями
2. Column indices для чтения только нужных колонок

**Row Group Skipping:**
```go
func (p *parquetFormat) GetRecords(
    ctx context.Context,
    cols []int,
    tester func(*metadata.RowGroupMetaData, []int) (bool, error),
) (array.RecordReader, error)
```

Использует `tester` функцию (metrics evaluator) для skip row groups.

---

### 6. Вспомогательные структуры

#### manifestEntries

Хранит разделенные data и delete entries:

```go
type manifestEntries struct {
    dataEntries             []iceberg.ManifestEntry
    positionalDeleteEntries []iceberg.ManifestEntry
    mu                      sync.Mutex
}
```

#### keyDefaultMap

Thread-safe map с factory для default значений:

```go
type keyDefaultMap[K comparable, V any] struct {
    defaultFactory func(K) V
    data           map[K]V
    mx             sync.RWMutex
}
```

Используется для кэширования:
- Partition filters per spec ID
- Manifest evaluators per spec ID
- Partition evaluators per spec ID

#### enumeratedRecord

Внутренняя структура для упорядоченной передачи записей:

```go
type enumeratedRecord struct {
    Record internal.Enumerated[arrow.RecordBatch]
    Task   internal.Enumerated[FileScanTask]
    Err    error
}
```

---

## Поток данных

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Table.Scan(opts...)                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Scan.PlanFiles(ctx)                              │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ 1. fetchPartitionSpecFilteredManifests()                          │  │
│  │    - Get snapshot                                                 │  │
│  │    - Load manifest list                                           │  │
│  │    - manifestEvaluator (partition stats)                          │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                    │                                    │
│                                    ▼                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ 2. collectManifestEntries() [PARALLEL]                            │  │
│  │    - partitionEvaluator (partition values)                        │  │
│  │    - metricsEvaluator (column stats)                              │  │
│  │    - Separate: data / positional deletes / eq deletes             │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                    │                                    │
│                                    ▼                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ 3. matchDeletesToData()                                           │  │
│  │    - Sort deletes by sequence number                              │  │
│  │    - Binary search + file_path matching                           │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                    []FileScanTask (data files + deletes)
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Scan.ToArrowRecords(ctx)                            │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ arrowScan.GetRecords()                                            │  │
│  │                                                                   │  │
│  │  ┌─────────────────────────────────────────────────────────────┐  │  │
│  │  │ readAllDeleteFiles() [PARALLEL]                             │  │  │
│  │  │   - Read all positional delete files                        │  │  │
│  │  │   - Group by file_path                                      │  │  │
│  │  └─────────────────────────────────────────────────────────────┘  │  │
│  │                                    │                              │  │
│  │                                    ▼                              │  │
│  │  ┌─────────────────────────────────────────────────────────────┐  │  │
│  │  │ recordBatchesFromTasksAndDeletes()                          │  │  │
│  │  │   - Create workers (concurrency limit)                      │  │  │
│  │  │   - Sequenced channel for ordered output                    │  │  │
│  │  │                                                             │  │  │
│  │  │   ┌───────────────────────────────────────────────────────┐ │  │  │
│  │  │   │ recordsFromTask() [per worker]                        │ │  │  │
│  │  │   │   1. prepareToRead()                                  │ │  │  │
│  │  │   │      - Open file                                      │ │  │  │
│  │  │   │      - PrunedSchema() -> column indices               │ │  │  │
│  │  │   │   2. Build pipeline:                                  │ │  │  │
│  │  │   │      - processPositionalDeletes()                     │ │  │  │
│  │  │   │      - filterRecords() (Substrait)                    │ │  │  │
│  │  │   │      - ToRequestedSchema()                            │ │  │  │
│  │  │   │   3. processRecords()                                 │ │  │  │
│  │  │   │      - GetRecords() with row group skipping           │ │  │  │
│  │  │   │      - Apply pipeline functions                       │ │  │  │
│  │  │   └───────────────────────────────────────────────────────┘ │  │  │
│  │  └─────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
        (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error])
```

---

## Оптимизации

### 1. Partition Pruning

Фильтрация manifest файлов до их чтения на основе partition statistics:

```go
// В fetchPartitionSpecFilteredManifests()
manifestEvaluators := newKeyDefaultMapWrapErr(scan.buildManifestEvaluator)
manifestList = slices.DeleteFunc(manifestList, func(mf iceberg.ManifestFile) bool {
    eval := manifestEvaluators.Get(int(mf.PartitionSpecID()))
    use, err := eval(mf)
    return !use || err != nil
})
```

### 2. Metrics-based Pruning

Фильтрация на уровне файлов и row groups:

```go
// В collectManifestEntries()
metricsEval, err := newInclusiveMetricsEvaluator(
    scan.metadata.CurrentSchema(),
    scan.rowFilter,
    scan.caseSensitive,
    scan.options["include_empty_files"] == "true",
)
```

### 3. Column Pruning

Чтение только необходимых колонок:

```go
// В prepareToRead()
fileSchema, colIndices, err := rdr.PrunedSchema(ids, as.nameMapping)
```

### 4. Row Group Skipping

Skip Parquet row groups на основе statistics:

```go
// В processRecords()
testRowGroups, err = newParquetRowGroupStatsEvaluator(
    fileSchema, as.boundRowFilter, false,
)
recRdr, err = rdr.GetRecords(ctx, columns, testRowGroups)
```

### 5. Параллельное чтение

**Manifest reading:**
```go
g, _ := errgroup.WithContext(ctx)
g.SetLimit(concurrencyLimit)
for _, mf := range manifestList {
    g.Go(func() error { /* read manifest */ })
}
```

**Delete file reading:**
```go
g.SetLimit(concurrency)
for _, v := range uniqueDeletes {
    g.Go(func() error { /* read delete file */ })
}
```

**Task processing:**
```go
numWorkers := min(as.concurrency, len(tasks))
for i := 0; i < numWorkers; i++ {
    go func() {
        for task := range taskChan {
            as.recordsFromTask(ctx, task, records, deletesPerFile[...])
        }
    }()
}
```

### 6. Memory Management

- Использование `compute.GetAllocator(ctx)` для Arrow allocations
- Retain/Release для RecordBatch
- `defer iceinternal.CheckedClose()` для очистки ресурсов

---

## Ограничения

| Компонент | Ограничение |
|-----------|-------------|
| **Формат файлов** | Только Parquet поддерживается |
| **Equality Deletes** | Не поддерживаются (возвращает ошибку) |
| **Projection сложных типов** | Map/List типы не pushdown-ятся |
| **In Predicate** | Ограничен 200 элементами в manifest evaluator |
| **File Format** | `default:` case возвращает `nil` в `GetFileFormat()` |

---

## API использования

### Базовое сканирование

```go
// Получить scanner с фильтром
scan := table.Scan(
    table.WithRowFilter(iceberg.GreaterThanEqual(
        iceberg.Reference("column_name"), 
        iceberg.NewLiteral(100),
    )),
    table.WithSelectedFields("col1", "col2", "col3"),
    table.WithLimit(1000),
)

// Получить план файлов
tasks, err := scan.PlanFiles(ctx)

// Или сразу получить Arrow записи
schema, records, err := scan.ToArrowRecords(ctx)
for record, err := range records {
    if err != nil {
        // обработка ошибки
    }
    defer record.Release()
    // обработка записи
}

// Или получить всю таблицу
tbl, err := scan.ToArrowTable(ctx)
defer tbl.Release()
```

### Time Travel

```go
// По snapshot ID
scan := table.Scan(
    table.WithSnapshotID(1234567890),
)

// По timestamp
scan := table.Scan(
    table.WithSnapshotAsOf(1672531200000), // ms since epoch
)
```

### Параллелизм

```go
scan := table.Scan(
    table.WitMaxConcurrency(8),
)
```

### Опции Arrow

```go
scan := table.Scan(
    table.WithOptions(iceberg.Properties{
        table.ScanOptionArrowUseLargeTypes: "true",
    }),
)
```

---

## Диаграмма классов

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Scan                                       │
├─────────────────────────────────────────────────────────────────────────┤
│ - metadata: Metadata                                                    │
│ - ioF: FSysF                                                            │
│ - rowFilter: BooleanExpression                                          │
│ - selectedFields: []string                                              │
│ - caseSensitive: bool                                                   │
│ - snapshotID: *int64                                                    │
│ - asOfTimestamp: *int64                                                 │
│ - limit: int64                                                          │
│ - concurrency: int                                                      │
│ - partitionFilters: *keyDefaultMap[int, BooleanExpression]              │
├─────────────────────────────────────────────────────────────────────────┤
│ + UseRowLimit(n int64) *Scan                                            │
│ + UseRef(name string) (*Scan, error)                                    │
│ + Snapshot() *Snapshot                                                  │
│ + Projection() (*iceberg.Schema, error)                                 │
│ + PlanFiles(ctx) ([]FileScanTask, error)                                │
│ + ToArrowRecords(ctx) (*arrow.Schema, iter.Seq2[...], error)            │
│ + ToArrowTable(ctx) (arrow.Table, error)                                │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ создает
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           FileScanTask                                  │
├─────────────────────────────────────────────────────────────────────────┤
│ + File: iceberg.DataFile                                                │
│ + DeleteFiles: []iceberg.DataFile                                       │
│ + Start: int64                                                          │
│ + Length: int64                                                         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ передается в
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            arrowScan                                    │
├─────────────────────────────────────────────────────────────────────────┤
│ - fs: iceio.IO                                                          │
│ - metadata: Metadata                                                    │
│ - projectedSchema: *iceberg.Schema                                      │
│ - boundRowFilter: BooleanExpression                                     │
│ - caseSensitive: bool                                                   │
│ - rowLimit: int64                                                       │
│ - useLargeTypes: bool                                                   │
│ - concurrency: int                                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ + GetRecords(ctx, tasks) (*arrow.Schema, iter.Seq2[...], error)         │
│ + recordBatchesFromTasksAndDeletes(...) iter.Seq2[...]                  │
│ + recordsFromTask(...) error                                            │
│ + processRecords(...) error                                             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ использует
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          FileReader (interface)                         │
├─────────────────────────────────────────────────────────────────────────┤
│ + PrunedSchema(ids, mapping) (*arrow.Schema, []int, error)              │
│ + GetRecords(ctx, cols, tester) (array.RecordReader, error)             │
│ + ReadTable(ctx) (arrow.Table, error)                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ реализует
                                    │
┌─────────────────────────────────────────────────────────────────────────┐
│                        ParquetFileSource                                │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Ссылки на файлы

| Компонент | Файл |
|-----------|------|
| Scan структура и методы | `table/scanner.go` |
| Arrow Scan реализация | `table/arrow_scanner.go` |
| Evaluators (manifest, partition, metrics) | `table/evaluators.go` |
| FileReader интерфейс | `table/internal/interfaces.go` |
| Parquet реализация | `table/internal/parquet_files.go` |
| Table и Scan options | `table/table.go` |
| Predicates helpers | `predicates.go` |
| Тесты scanner | `table/scanner_test.go` |

---

## Версии зависимостей

```
go 1.24.0
github.com/apache/arrow-go/v18 v18.4.1
github.com/substrait-io/substrait-go/v4 v4.4.0
golang.org/x/sync v0.17.0
```
