# Архитектура динамических фильтров для Iceberg FDW

## Обзор

Динамические фильтры позволяют оптимизировать JOIN-запросы в Postgres FDW (Greenplum) путём:
1. Сбора значений из таблиц-источников (source) во время выполнения
2. Передачи этих значений через сетевой координатор
3. Применения фильтров к таблицам-целям (target) на основе собранных значений

## Архитектурные компоненты

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Greenplum Master                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Coordinator (GRPC Server)                        │    │
│  │  - Приём значений от source сегментов                               │    │
│  │  - Агрегация и дедупликация                                         │    │
│  │  - Предоставление фильтров target сегментам                         │    │
│  │  - Управление сессиями (start/stop/query)                           │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    ▲                                        │
│                                    │ GRPC                                   │
│                                    ▼                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            ▼                       ▼                       ▼
┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
│   Segment 1       │   │   Segment 2       │   │   Segment N       │
│  ┌─────────────┐  │   │  ┌─────────────┐  │   │  ┌─────────────┐  │
│  │ FDW Client  │  │   │  │ FDW Client  │  │   │  │ FDW Client  │  │
│  │ - Source    │  │   │  │ - Target    │  │   │  │ - Source    │  │
│  │   Collector │  │   │  │   Filter    │  │   │  │   Collector │  │
│  │ - Target    │  │   │  │ - Target    │  │   │  │ - Target    │  │
│  │   Filter    │  │   │  │   Filter    │  │   │  │   Filter    │  │
│  └─────────────┘  │   │  └─────────────┘  │   │  └─────────────┘  │
└───────────────────┘   └───────────────────┘   └───────────────────┘
```

## Компоненты

### 1. Coordinator (GRPC Server)

**Расположение:** `dynamicfilter/coordinator/`

**Функции:**
- `Start(queryID string)` - инициализация сессии для запроса
- `Stop(queryID string)` - завершение сессии, очистка ресурсов
- `CollectValues(queryID, sourceID string, values []Literal)` - приём значений от source
- `GetFilter(queryID, targetID string) (Filter, error)` - получение фильтра для target
- `IsReady(queryID, targetID string) bool` - готов ли фильтр для target

**Структуры данных:**
```go
type QuerySession struct {
    QueryID     string
    Sources     map[string]*SourceState    // source_alias -> state
    Targets     map[string]*TargetState    // target_alias -> state
    Mappings    []FieldMapping
    StartTime   time.Time
    Status      SessionStatus
}

type SourceState struct {
    Alias       string
    Fields      []string
    Collected   map[int]ValueSet         // field_id -> collected values
    IsComplete  bool                     // все ли сегменты завершили сбор
    SegmentCount int                     // количество сегментов-источников
}

type TargetState struct {
    Alias       string
    Fields      []string
    SourceAlias string                     // связанный source
    Filter      *DynamicFilter             // сгенерированный фильтр
    IsReady     bool                       // готов ли фильтр к применению
}

type FieldMapping struct {
    TargetAlias  string
    TargetField  string
    SourceAlias  string
    SourceField  string
}

type DynamicFilter struct {
    FieldID     int
    FieldType   iceberg.Type
    Values      ValueSet
    Ranges      []ValueRange
    FilterType  FilterType  // IN или BETWEEN
    Expression  iceberg.BooleanExpression
}
```

### 2. FDW Client Library

**Расположение:** `dynamicfilter/client/`

**Функции:**
- `Connect(coordinatorAddr string) (*Client, error)`
- `StartQuery(queryID string) error`
- `StopQuery(queryID string) error`
- `SendValues(queryID, sourceAlias string, fieldID int, values []iceberg.Literal) error`
- `WaitForFilter(queryID, targetAlias string, timeout time.Duration) (*DynamicFilter, error)`
- `BuildFilterExpression(filter *DynamicFilter, targetFieldID int) (iceberg.BooleanExpression, error)`

### 3. GRPC Protocol

**Расположение:** `dynamicfilter/proto/dynamic_filter.proto`

```protobuf
syntax = "proto3";

package dynamicfilter;

service DynamicFilterService {
    // Управление сессиями
    rpc StartQuery(StartQueryRequest) returns (StartQueryResponse);
    rpc StopQuery(StopQueryRequest) returns (StopQueryResponse);
    rpc QueryStatus(QueryStatusRequest) returns (QueryStatusResponse);
    
    // Сбор значений от source
    rpc CollectValues(stream CollectValueRequest) returns (CollectValueResponse);
    
    // Получение фильтров для target
    rpc GetFilter(GetFilterRequest) returns (GetFilterResponse);
    rpc WaitForFilter(WaitForFilterRequest) returns (WaitForFilterResponse);
    
    // Синхронизация сегментов
    rpc RegisterSegment(RegisterSegmentRequest) returns (RegisterSegmentResponse);
    rpc SignalSourceComplete(SignalSourceCompleteRequest) returns (SignalSourceCompleteResponse);
}

message StartQueryRequest {
    string query_id = 1;
    string session_id = 2;           // уникальный ID сегмента
    repeated FieldMapping mappings = 3;
    int32 total_segments = 4;         // общее количество сегментов
}

message FieldMapping {
    string target_alias = 1;
    string target_field = 2;
    string source_alias = 3;
    string source_field = 4;
    int32 source_field_id = 5;
    int32 target_field_id = 6;
    string field_type = 7;            // iceberg type name
}

message CollectValueRequest {
    string query_id = 1;
    string session_id = 2;
    string source_alias = 3;
    int32 field_id = 4;
    repeated Literal values = 5;
    bool is_final = 6;                // последний пакет значений
}

message Literal {
    oneof value {
        bool bool_value = 1;
        int32 int32_value = 2;
        int64 int64_value = 3;
        float float32_value = 4;
        double float64_value = 5;
        string string_value = 6;
        bytes bytes_value = 7;
        int64 date_value = 8;
        int64 time_value = 9;
        int64 timestamp_value = 10;
        string decimal_value = 11;    // string representation
        string uuid_value = 12;
    }
}

message GetFilterRequest {
    string query_id = 1;
    string session_id = 2;
    string target_alias = 3;
    int32 field_id = 4;
}

message GetFilterResponse {
    bool is_ready = 1;
    DynamicFilter filter = 2;
}

message DynamicFilter {
    int32 field_id = 1;
    string field_type = 2;
    FilterType filter_type = 3;
    repeated Literal in_values = 4;
    repeated ValueRange ranges = 5;
}

enum FilterType {
    FILTER_TYPE_UNSPECIFIED = 0;
    FILTER_TYPE_IN = 1;
    FILTER_TYPE_BETWEEN = 2;
}

message ValueRange {
    Literal lower = 1;
    Literal upper = 2;
    bool lower_inclusive = 3;
    bool upper_inclusive = 4;
}
```

### 4. GUC Parameter Parser

**Расположение:** `dynamicfilter/config/`

**Формат GUC параметра:**
```
iceberg.dynamic_filter_mappings = 'target_alias.field=source_alias.field,alias2.col2=alias3.col3'
```

**Парсинг:**
```go
type DynamicFilterConfig struct {
    Mappings []FieldMapping
    Enabled  bool
}

func ParseGUCMappings(gucValue string, tableAliases map[string]string) (*DynamicFilterConfig, error)
```

### 5. Integration Points

#### 5.1 Source Table Integration

**Место внедрения:** `table/arrow_scanner.go` -> `arrowScan.GetRecords()`

**Процесс:**
1. Проверка: является ли таблица источником значений
2. Если да - создание collector wrapper для RecordReader
3. При чтении каждого RecordBatch:
   - Извлечение значений из указанных полей
   - Буферизация и отправка пакетов на координатор
4. После завершения чтения - сигнал `SignalSourceComplete`

```go
type SourceValueCollector struct {
    queryID      string
    sourceAlias  string
    fieldIDs     []int
    client       *dfclient.Client
    batchSize    int  // отправка пакетов по N значений
}

func (s *SourceValueCollector) CollectFromBatch(batch arrow.RecordBatch) error
func (s *SourceValueCollector) Finalize() error
```

#### 5.2 Target Table Integration

**Место внедрения:** `table/scanner.go` -> `Scan.PlanFiles()` и `table/arrow_scanner.go`

**Процесс:**
1. Перед планированием файлов - проверка `WaitForFilter()`
2. Получение фильтра от координатора
3. Построение BooleanExpression из фильтра
4. Объединение с существующим rowFilter через AND
5. Продолжение сканирования с обновлённым фильтром

```go
func (scan *Scan) ApplyDynamicFilter(ctx context.Context, client *dfclient.Client, targetAlias string) error {
    filter, err := client.WaitForFilter(ctx, scan.queryID, targetAlias, 30*time.Second)
    if err != nil {
        return err
    }
    if filter != nil {
        expr, err := filter.BuildExpression()
        if err != nil {
            return err
        }
        scan.rowFilter = iceberg.NewAnd(scan.rowFilter, expr)
        // Пересоздать partition filters с новым фильтром
        scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)
    }
    return nil
}
```

### 6. Filter Generation Logic

**Расположение:** `dynamicfilter/filter_builder.go`

**Пороги:**
- `IN_PREDICATE_LIMIT = 200` - максимум значений для IN предиката
- `BETWEEN_RANGE_COUNT = 50` - максимум диапазонов для BETWEEN

**Алгоритм:**
```go
func BuildDynamicFilter(values ValueSet, fieldType iceberg.Type) *DynamicFilter {
    if values.Len() <= IN_PREDICATE_LIMIT {
        return &DynamicFilter{
            FilterType: FilterTypeIN,
            Values: values,
        }
    }
    
    // Конвертация в диапазоны
    ranges := buildRangesFromValues(values, fieldType, BETWEEN_RANGE_COUNT)
    return &DynamicFilter{
        FilterType: FilterTypeBETWEEN,
        Ranges: ranges,
    }
}

func buildRangesFromValues(values ValueSet, typ iceberg.Type, maxRanges int) []ValueRange {
    // 1. Сортировка значений
    // 2. Группировка в кластеры (gap detection)
    // 3. Ограничение количества диапазонов
    // 4. Слияние соседних диапазонов при необходимости
}
```

**Построение выражения:**
```go
func (f *DynamicFilter) BuildExpression(fieldRef iceberg.Reference) iceberg.BooleanExpression {
    switch f.FilterType {
    case FilterTypeIN:
        return iceberg.IsIn(fieldRef, f.Values.Slice()...)
    case FilterTypeBETWEEN:
        var predicates []iceberg.BooleanExpression
        for _, r := range f.Ranges {
            pred := iceberg.NewAnd(
                iceberg.GreaterThanEqual(fieldRef, r.Lower),
                iceberg.LessThanEqual(fieldRef, r.Upper),
            )
            predicates = append(predicates, pred)
        }
        return iceberg.Or(predicates...)
    }
}
```

### 7. ArrowArrayStream Integration

**Место внедрения:** FDW wrapper вокруг `iter.Seq2[arrow.RecordBatch, error]`

```go
type FDWArrowStream struct {
    inner     iter.Seq2[arrow.RecordBatch, error]
    collector *SourceValueCollector
    queryID   string
}

func (f *FDWArrowStream) Next() (arrow.RecordBatch, error) {
    batch, err := <-f.inner
    if err != nil {
        return nil, err
    }
    if f.collector != nil {
        f.collector.CollectFromBatch(batch)
    }
    return batch, nil
}
```

## Поток выполнения запроса

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Query Execution Flow                           │
└──────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐
  │ 1. Query Start  │
  │   (Master)      │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ 2. Coordinator.StartQuery(queryID, mappings, totalSegments)     │
  │    - Создание QuerySession                                      │
  │    - Инициализация Source/Target state                          │
  └────────┬────────────────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ 3. Segment Scan Execution (параллельно на всех сегментах)       │
  │                                                                 │
  │    ┌────────────────────┐         ┌────────────────────┐       │
  │    │ Source Table Scan  │         │ Target Table Scan  │       │
  │    │                    │         │                    │       │
  │    │ a) RegisterSegment │         │ a) RegisterSegment │       │
  │    │ b) Scan files      │         │ b) WaitForFilter   │◄──────┼─── Wait
  │    │ c) Extract values  │         │    (блокировка)    │       │
  │    │ d) Send to Coord   │─────────►c) GetFilter        │       │
  │    │ e) Continue scan   │         │ d) Apply filter    │       │
  │    │ f) SignalComplete  │         │ e) Continue scan   │       │
  │    └────────────────────┘         └────────────────────┘       │
  └─────────────────────────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ 4. Coordinator агрегирует значения от всех сегментов            │
  │    - Дедупликация                                               │
  │    - Построение фильтров для target                             │
  │    - Сигнал готовности target сегментам                         │
  └────────┬────────────────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────┐
  │ 5. Query Stop   │
  │   Coordinator   │
  │   .StopQuery()  │
  └─────────────────┘
```

## Конфигурация

### GUC Parameters

```sql
-- Включение динамических фильтров
SET iceberg.enable_dynamic_filters = on;

-- Маппинг полей для динамических фильтров
SET iceberg.dynamic_filter_mappings = '
    lineitem.l_orderkey=orders.o_orderkey,
    lineitem.l_partkey=part.p_partkey
';

-- Адрес координатора
SET iceberg.dynamic_filter_coordinator_addr = 'master-host:port';

-- Таймаут ожидания фильтра (секунды)
SET iceberg.dynamic_filter_wait_timeout = 30;

-- Размер пакета для отправки значений
SET iceberg.dynamic_filter_batch_size = 10000;
```

### Coordinator Configuration

```go
type CoordinatorConfig struct {
    ListenAddr         string        // адрес для прослушивания
    QueryTimeout       time.Duration // таймаут сессии запроса
    MaxValuesPerField  int           // максимум значений на поле
    MaxRangesPerFilter int           // максимум диапазонов в BETWEEN
    InPredicateLimit   int           // порог для IN vs BETWEEN
    CleanupInterval    time.Duration // интервал очистки старых сессий
}
```

## Обработка ошибок

### Coordinator Errors

| Ошибка | Описание | Действие |
|--------|----------|----------|
| `ErrQueryNotFound` | queryID не найден | Вернуть ошибку клиенту |
| `ErrSessionExpired` | сессия истекла по таймауту | Очистить сессию |
| `ErrDuplicateSegment` | дубликат регистрации сегмента | Игнорировать |
| `ErrValuesOverflow` | превышен лимит значений | Отбросить лишние, логировать |

### Client Errors

| Ошибка | Описание | Действие |
|--------|----------|----------|
| `ErrCoordinatorUnavailable` | координатор недоступен | Fallback на обычный scan |
| `ErrFilterTimeout` | таймаут ожидания фильтра | Продолжить без фильтра, логировать |
| `ErrConnectionLost` | потеря соединения | Попытка reconnect, fallback |

## Производительность

### Оптимизации

1. **Batching значений** - отправка пакетов по N значений вместо по одному
2. **Compression** - сжатие GRPC сообщений (gzip)
3. **Parallel Collection** - параллельная отправка от разных полей
4. **Early Filter Build** - построение фильтра до завершения всех source
5. **Incremental Filter Update** - обновление фильтра по мере поступления значений

### Метрики

```go
type FilterMetrics struct {
    QueryID           string
    SourceAlias       string
    ValuesCollected   int64
    ValuesDeduplicated int64
    FilterType        string  // IN/BETWEEN
    RangesCount       int
    BuildDurationMs   int64
    WaitDurationMs    int64
}
```

## Тестирование

### Unit Tests

- Парсинг GUC параметра
- Построение IN предиката
- Построение BETWEEN предиката
- Конвертация значений в диапазоны
- GRPC сериализация/десериализация

### Integration Tests

- Один source + один target на одном сегменте
- Один source +多个 target на разных сегментах
- Несколько source + несколько target
- Тест с таймаутами и ошибками
- Тест производительности с большими объёмами данных

### Benchmark Tests

- Замер накладных расходов на сбор значений
- Замер ускорения JOIN с динамическими фильтрами
- Замер масштабируемости с ростом числа сегментов
