# Dynamic Filters for Iceberg FDW

Пакет `dynamicfilter` предоставляет функциональность динамических фильтров для оптимизации JOIN-запросов в Postgres FDW / Greenplum.

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                    Coordinator (GRPC Server)                    │
│  - Приём значений от source сегментов                           │
│  - Агрегация и дедупликация                                     │
│  - Предоставление фильтров target сегментам                     │
└─────────────────────────────────────────────────────────────────┘
                                ▲
                                │ GRPC
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FDW Segments                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │   Source    │    │   Target    │    │   Source    │         │
│  │  Collector  │    │   Filter    │    │  Collector  │         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

## Компоненты

### Coordinator (`coordinator/`)

GRPC сервер для управления динамическими фильтрами:

```go
import "github.com/apache/iceberg-go/dynamicfilter/coordinator"

coord := coordinator.New(coordinator.Config{
    ListenAddr:         ":9999",
    QueryTimeout:       5 * time.Minute,
    InPredicateLimit:   200,
    MaxRangesPerFilter: 50,
})

// Запуск сервера
go coord.Serve(":9999")
defer coord.Stop()
```

### Client (`client/`)

GRPC клиент для подключения к координатору:

```go
import "github.com/apache/iceberg-go/dynamicfilter/client"

// Подключение
dfClient, err := client.Connect("localhost:9999",
    client.WithBufferSize(10000),
    client.WithDialTimeout(10*time.Second),
)
defer dfClient.Close()

// Начало сессии
err = dfClient.StartQuery(ctx, queryID, sessionID, mappings, totalSegments)
defer dfClient.StopQuery(ctx)
```

### Config (`config/`)

Парсинг GUC параметров Postgres:

```go
import "github.com/apache/iceberg-go/dynamicfilter/config"

// Загрузка из GUC параметров
cfg, err := config.LoadFromGUC(func(name string) string {
    // Получение значения GUC параметра
    return GetGUCValue(name)
})

// Проверка роли таблицы
if cfg.IsSourceTable("orders") {
    // Таблица является источником значений
}
if cfg.IsTargetTable("lineitem") {
    // Таблица является целью для фильтра
}
```

### Integration (`integration.go`)

Интеграция с Iceberg сканером:

```go
import "github.com/apache/iceberg-go/dynamicfilter"

// Для source таблицы
collector := dynamicfilter.NewSourceValueCollector(
    queryID, sessionID, "orders",
    dfClient,
    []int{1, 2}, // field IDs
    fieldTypes,
    10000, // buffer size
)

// Для target таблицы
applier := dynamicfilter.NewTargetFilterApplier(
    queryID, sessionID, "lineitem",
    dfClient,
    []int{1, 2}, // field IDs
    fieldTypes,
    30, // timeout seconds
)

// Ожидание фильтров
err := applier.WaitForFilters(ctx)

// Построение выражения
fieldRef := iceberg.Reference("l_orderkey")
expr, err := applier.BuildFilterExpression(fieldID, fieldRef)

// Применение к сканеру
scan := table.Scan(table.WithRowFilter(expr))
```

### FDW Stream (`fdw_stream.go`)

Обертка для ArrowArrayStream с поддержкой динамических фильтров:

```go
import "github.com/apache/iceberg-go/dynamicfilter"

// Создание менеджера потоков
manager := dynamicfilter.NewFDWStreamManager(queryID, sessionID, dfClient)

// Регистрация таблиц
manager.RegisterSource("orders", []int{1}, fieldTypes, 10000)
manager.RegisterTarget("lineitem", []int{1}, fieldTypes, 30)

// Начало сессии
err := manager.Start(ctx, mappings, totalSegments)
defer manager.Stop(ctx)

// Создание потока
stream, err := manager.CreateStream(ctx, "lineitem", innerIterator)

// Итерация с применением фильтров
for batch, err := range stream.GetIterator(ctx) {
    if err != nil {
        // обработка ошибки
    }
    // обработка батча
}
```

## GUC Параметры

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `iceberg.enable_dynamic_filters` | Включение динамических фильтров | off |
| `iceberg.dynamic_filter_mappings` | Маппинг полей (`target.field=source.field,...`) | - |
| `iceberg.dynamic_filter_coordinator_addr` | Адрес координатора (host:port) | localhost:9999 |
| `iceberg.dynamic_filter_wait_timeout` | Таймаут ожидания фильтра (сек) | 30 |
| `iceberg.dynamic_filter_batch_size` | Размер пакета отправки значений | 10000 |

### Пример настройки

```sql
SET iceberg.enable_dynamic_filters = on;
SET iceberg.dynamic_filter_coordinator_addr = 'master-host:9999';
SET iceberg.dynamic_filter_mappings = '
    lineitem.l_orderkey=orders.o_orderkey,
    lineitem.l_partkey=part.p_partkey,
    lineitem.l_suppkey=supplier.s_suppkey
';
SET iceberg.dynamic_filter_wait_timeout = 60;
SET iceberg.dynamic_filter_batch_size = 50000;
```

## Поток выполнения

1. **Начало запроса**
   - FDW создает coordinator client
   - Вызывается `StartQuery()` с маппингом полей

2. **Сканирование source таблиц**
   - Извлечение значений из указанных полей
   - Буферизация и отправка на координатор
   - Сигнал `SignalSourceComplete()` после завершения

3. **Координатор**
   - Агрегация значений от всех сегментов
   - Дедупликация
   - Построение фильтра (IN или BETWEEN)
   - Сигнал готовности target сегментам

4. **Сканирование target таблиц**
   - Ожидание фильтра через `WaitForFilter()`
   - Построение BooleanExpression
   - Применение к сканеру через `WithRowFilter()`
   - Продолжение сканирования с фильтром

5. **Завершение запроса**
   - Вызов `StopQuery()` для очистки ресурсов

## Алгоритм построения фильтров

### IN Предикат

Если количество уникальных значений ≤ 200:
```sql
WHERE column IN (val1, val2, ..., valN)
```

### BETWEEN Предикат

Если количество уникальных значений > 200:
1. Сортировка значений
2. Группировка в кластеры (gap detection)
3. Ограничение количества диапазонов (≤ 50)
4. Построение OR предиката:
```sql
WHERE (column BETWEEN low1 AND high1)
   OR (column BETWEEN low2 AND high2)
   OR ...
```

## Примеры

### Простой JOIN с динамическим фильтром

```go
// Настройка
mappings := []dynamicfilter.FieldMapping{
    {
        TargetAlias: "lineitem", TargetField: "l_orderkey",
        SourceAlias: "orders", SourceField: "o_orderkey",
        SourceFieldID: 1, TargetFieldID: 1,
        FieldType: iceberg.PrimitiveTypes.Int64,
    },
}

// Coordinator
coord := coordinator.New(coordinator.Config{ListenAddr: ":9999"})
go coord.Serve(":9999")
defer coord.Stop()

// Client
dfClient, _ := client.Connect("localhost:9999")
defer dfClient.Close()

// Source table (orders)
dfClient.StartQuery(ctx, "q1", "seg1", mappings, 1)
// ... во время сканирования ...
dfClient.SendValues(ctx, "orders", 1, values, iceberg.PrimitiveTypes.Int64)
dfClient.SignalSourceComplete(ctx, "orders", 1)

// Target table (lineitem)
filter, _ := dfClient.WaitForFilter(ctx, "lineitem", 1, 30*time.Second)
expr := filter.BuildExpression(iceberg.Reference("l_orderkey"))
scan := table.Scan(table.WithRowFilter(expr))
```

### Использование с Iceberg FDW

```go
// В FDW exec_simple_query
func execScan(ctx context.Context, table *table.Table, alias string) error {
    // Загрузка конфигурации
    cfg, _ := config.LoadFromGUC(GetGUCValue)
    
    // Подключение к координатору
    dfClient, _ := client.Connect(cfg.CoordinatorAddr)
    defer dfClient.Close()
    
    // Создание менеджера
    manager := dynamicfilter.NewFDWStreamManager(queryID, sessionID, dfClient)
    
    // Регистрация таблиц
    if cfg.IsSourceTable(alias) {
        manager.RegisterSource(alias, sourceFields, fieldTypes, cfg.BatchSize)
    }
    if cfg.IsTargetTable(alias) {
        manager.RegisterTarget(alias, targetFields, fieldTypes, cfg.WaitTimeout)
    }
    
    // Начало сессии
    manager.Start(ctx, cfg.Mappings, totalSegments)
    defer manager.Stop(ctx)
    
    // Сканирование с фильтрами
    scan := table.Scan()
    if cfg.IsTargetTable(alias) {
        stream, _ := manager.CreateStream(ctx, alias, scan.ToArrowRecords(ctx))
        return stream.ForEach(ctx, processBatch)
    }
    
    // Для source таблиц
    stream, _ := manager.CreateStream(ctx, alias, scan.ToArrowRecords(ctx))
    return stream.ForEach(ctx, processBatch)
}
```

## Тестирование

```bash
# Запуск тестов
go test ./dynamicfilter/...

# Тесты с покрытием
go test -cover ./dynamicfilter/...

# Benchmark тесты
go test -bench=. ./dynamicfilter/...
```

## Метрики

Для мониторинга производительности:

```go
// Статистика коллектора
collected, sent := collector.Stats()

// Статистика потока
batches, rows := stream.Stats()

// Статус запроса
status, _ := dfClient.QueryStatus(ctx)
```

## Ограничения

1. **Формат файлов**: Только Parquet поддерживается
2. **Equality deletes**: Не поддерживаются
3. **Максимум значений**: 1,000,000 на поле (настраивается)
4. **Максимум диапазонов**: 50 в BETWEEN предикате
5. **Таймаут**: 5 минут на запрос (по умолчанию)

## Лицензия

Apache License 2.0
