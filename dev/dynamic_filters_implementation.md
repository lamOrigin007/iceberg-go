# Реализация динамических фильтров для Iceberg FDW

## Статус: ✅ Завершено

Все компоненты динамических фильтров реализованы и успешно компилируются.

---

## Созданные файлы

### Документация
| Файл | Описание |
|------|----------|
| `dev/dynamic_filters_architecture.md` | Полная архитектура системы |
| `dynamicfilter/README.md` | Руководство пользователя |

### Протокол (GRPC)
| Файл | Описание |
|------|----------|
| `dynamicfilter/proto/dynamic_filter.proto` | Proto3 определение сервиса |
| `dynamicfilter/proto/dynamic_filter.pb.go` | Сгенерированный Go код |
| `dynamicfilter/proto/dynamic_filter_grpc.pb.go` | GRPC сервисы |

### Основные пакеты
| Файл | Описание | Статус |
|------|----------|--------|
| `dynamicfilter/types/types.go` | Общие типы (DynamicFilter, ValueRange, FieldMapping) | ✅ |
| `dynamicfilter/coordinator/coordinator.go` | GRPC сервер координатора | ✅ |
| `dynamicfilter/client/client.go` | GRPC клиент | ✅ |
| `dynamicfilter/config/config.go` | Парсинг GUC параметров | ✅ |
| `dynamicfilter/integration.go` | SourceValueCollector, TargetFilterApplier | ✅ |
| `dynamicfilter/fdw_stream.go` | FDWArrowStream для работы с ArrowArrayStream | ✅ |

### Тесты
| Файл | Описание |
|------|----------|
| `dynamicfilter/types/types_test.go` | Тесты типов и алгоритмов |
| `dynamicfilter/config/config_test.go` | Тесты конфигурации |

---

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                    Coordinator (GRPC Server)                    │
│  dynamicfilter/coordinator/coordinator.go                       │
│  - StartQuery()  - Приём и агрегация значений                   │
│  - StopQuery()   - Построение фильтров (IN/BETWEEN)             │
│  - CollectValues() - Управление сессиями                        │
│  - WaitForFilter()                                              │
└─────────────────────────────────────────────────────────────────┘
                                ▲
                                │ GRPC
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        FDW Segments                             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  dynamicfilter/client/client.go                         │    │
│  │  - Connect() / StartQuery() / StopQuery()               │    │
│  │  - SendValues() / SignalSourceComplete()                │    │
│  │  - GetFilter() / WaitForFilter()                        │    │
│  └─────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  dynamicfilter/integration.go                           │    │
│  │  - SourceValueCollector  - Извлечение значений          │    │
│  │  - TargetFilterApplier   - Применение фильтров          │    │
│  └─────────────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  dynamicfilter/fdw_stream.go                            │    │
│  │  - FDWArrowStream        - Обертка для Arrow итератора  │    │
│  │  - FDWStreamManager      - Управление потоками          │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Ключевые компоненты

### 1. Coordinator (Сервер)

**Пакет:** `dynamicfilter/coordinator`

**Функции:**
- `StartQuery(queryID, sessionID, mappings, totalSegments)` - Начало сессии
- `StopQuery(queryID, sessionID)` - Завершение сессии
- `CollectValues(stream)` - Приём значений от source (streaming)
- `WaitForFilter(queryID, targetAlias, fieldID, timeout)` - Ожидание фильтра
- `GetFilter(queryID, targetAlias, fieldID)` - Получение фильтра

**Алгоритм построения фильтра:**
1. Агрегация значений от всех сегментов
2. Дедупликация
3. Если значений ≤ 200 → IN предикат
4. Если значений > 200 → BETWEEN предикаты (до 50 диапазонов)

### 2. Client (Клиент)

**Пакет:** `dynamicfilter/client`

**Функции:**
- `Connect(coordinatorAddr, opts...)` - Подключение к координатору
- `StartQuery(queryID, sessionID, mappings, totalSegments)` - Начало сессии
- `SendValues(sourceAlias, fieldID, values)` - Отправка значений
- `SignalSourceComplete(sourceAlias, fieldID)` - Сигнал завершения
- `WaitForFilter(targetAlias, fieldID, timeout)` - Ожидание фильтра
- `GetFilter(targetAlias, fieldID)` - Получение фильтра

### 3. Types (Общие типы)

**Пакет:** `dynamicfilter/types`

**Типы:**
- `DynamicFilter` - Динамический фильтр
- `ValueRange` - Диапазон для BETWEEN
- `FieldMapping` - Маппинг source→target
- `FilterType` - Тип фильтра (IN/BETWEEN)
- `SessionStatus` - Статус сессии

**Функции:**
- `BuildFilter(values, fieldType, inLimit, maxRanges)` - Построение фильтра
- `filter.BuildExpression(fieldRef)` - Создание BooleanExpression

### 4. Config (Конфигурация)

**Пакет:** `dynamicfilter/config`

**GUC параметры:**
- `iceberg.enable_dynamic_filters` - Включение (on/off)
- `iceberg.dynamic_filter_mappings` - Маппинг полей
- `iceberg.dynamic_filter_coordinator_addr` - Адрес координатора
- `iceberg.dynamic_filter_wait_timeout` - Таймаут ожидания (сек)
- `iceberg.dynamic_filter_batch_size` - Размер пакета

**Функции:**
- `LoadFromGUC(getGUCFunc)` - Загрузка из GUC
- `ParseGUCMappings(gucValue, tableAliases)` - Парсинг маппингов

### 5. Integration (Интеграция)

**Пакет:** `dynamicfilter` (root)

**Компоненты:**
- `SourceValueCollector` - Извлечение значений из Arrow RecordBatch
- `TargetFilterApplier` - Применение фильтров к сканеру
- `FDWArrowStream` - Обертка для Arrow итератора
- `FDWStreamManager` - Управление потоками для множественных таблиц

---

## Поток выполнения

```
1. Query Start (Master)
   │
   ├─► Coordinator.StartQuery(queryID, mappings, totalSegments)
   │
2. Segment Execution (параллельно)
   │
   ├─► Source Table Scan:
   │   ├─► RegisterSegment()
   │   ├─► Scan files → Extract values → SendValues()
   │   └─► SignalSourceComplete()
   │
   ├─► Coordinator:
   │   ├─► Aggregate values from all segments
   │   ├─► Build filter (IN if ≤200, BETWEEN if >200)
   │   └─► Notify waiting targets
   │
   └─► Target Table Scan:
       ├─► RegisterSegment()
       ├─► WaitForFilter(timeout) ──── Wait ────┐
       ├─► Get filter → BuildExpression()       │
       ├─► Apply to scan: Scan(WithRowFilter()) │
       └─► Continue scan with filter ◄──────────┘

3. Query Stop
   └─► Coordinator.StopQuery()
```

---

## Примеры использования

### 1. Настройка координатора

```go
import (
    "github.com/apache/iceberg-go/dynamicfilter/coordinator"
)

coord := coordinator.New(coordinator.Config{
    ListenAddr:         ":9999",
    QueryTimeout:       5 * time.Minute,
    InPredicateLimit:   200,
    MaxRangesPerFilter: 50,
    EnableCompression:  true,
})

// Запуск сервера
go coord.Serve(":9999")
defer coord.Stop()
```

### 2. Настройка клиента (FDW)

```go
import (
    "github.com/apache/iceberg-go/dynamicfilter/client"
    "github.com/apache/iceberg-go/dynamicfilter/types"
)

// Подключение
dfClient, err := client.Connect("localhost:9999",
    client.WithBufferSize(10000),
    client.WithDialTimeout(10*time.Second),
)
defer dfClient.Close()

// Маппинги
mappings := []types.FieldMapping{
    {
        TargetAlias: "lineitem", TargetField: "l_orderkey",
        SourceAlias: "orders", SourceField: "o_orderkey",
        SourceFieldID: 1, TargetFieldID: 1,
        FieldType: iceberg.PrimitiveTypes.Int64,
    },
}

// Начало сессии
err = dfClient.StartQuery(ctx, queryID, sessionID, mappings, totalSegments)
defer dfClient.StopQuery(ctx)
```

### 3. Source таблица (извлечение значений)

```go
import (
    "github.com/apache/iceberg-go/dynamicfilter"
)

// Создание коллектора
collector := dynamicfilter.NewSourceValueCollector(
    queryID, sessionID, "orders",
    dfClient,
    []int{1}, // field IDs
    fieldTypes,
    10000, // buffer size
)

// Во время сканирования
for batch, err := range records {
    if err != nil { break }
    
    // Извлечение значений
    collector.CollectFromBatch(batch)
}

// Завершение
collector.SignalComplete(ctx)
```

### 4. Target таблица (применение фильтра)

```go
import (
    "github.com/apache/iceberg-go/dynamicfilter"
)

// Создание аппликатора
applier := dynamicfilter.NewTargetFilterApplier(
    queryID, sessionID, "lineitem",
    dfClient,
    []int{1}, // field IDs
    fieldTypes,
    30, // timeout seconds
)

// Ожидание фильтра
err := applier.WaitForFilters(ctx)

// Построение выражения
fieldRef := iceberg.Reference("l_orderkey")
expr, err := applier.BuildFilterExpression(1, fieldRef)

// Применение к сканеру
scan := table.Scan(table.WithRowFilter(expr))
```

### 5. Настройка GUC параметров (Postgres)

```sql
-- Включение динамических фильтров
SET iceberg.enable_dynamic_filters = on;

-- Адрес координатора
SET iceberg.dynamic_filter_coordinator_addr = 'master-host:9999';

-- Маппинг полей
SET iceberg.dynamic_filter_mappings = '
    lineitem.l_orderkey=orders.o_orderkey,
    lineitem.l_partkey=part.p_partkey
';

-- Таймаут и размер пакета
SET iceberg.dynamic_filter_wait_timeout = 60;
SET iceberg.dynamic_filter_batch_size = 50000;
```

---

## Алгоритмы

### Построение IN предиката

Если уникальных значений ≤ 200:
```
WHERE column IN (val1, val2, ..., valN)
```

Реализация через OR из EqualTo:
```go
EqualTo(col, val1) OR EqualTo(col, val2) OR ... OR EqualTo(col, valN)
```

### Построение BETWEEN предиката

Если уникальных значений > 200:

1. **Сортировка** значений
2. **Группировка в кластеры** (gap detection):
   - Int: разрыв > 100
   - Float: разрыв > 10%
   - String: разные префиксы
3. **Ограничение** количества диапазонов (≤ 50)
4. **Слияние** соседних кластеров при необходимости

Результат:
```
WHERE (col BETWEEN low1 AND high1)
   OR (col BETWEEN low2 AND high2)
   OR ...
```

---

## Тестирование

```bash
# Запуск тестов
go test ./dynamicfilter/...

# Тесты с покрытием
go test -cover ./dynamicfilter/...

# Benchmark тесты
go test -bench=. ./dynamicfilter/types
```

---

## Зависимости

```go
google.golang.org/grpc
google.golang.org/protobuf
github.com/google/uuid
github.com/apache/iceberg-go
github.com/apache/arrow-go/v18
```

---

## Ограничения

| Компонент | Ограничение | Настройка |
|-----------|-------------|-----------|
| **Максимум значений** | 1,000,000 на поле | `MaxValuesPerField` |
| **IN предикат** | ≤ 200 значений | `InPredicateLimit` |
| **BETWEEN диапазоны** | ≤ 50 | `MaxRangesPerFilter` |
| **Таймаут запроса** | 5 минут | `QueryTimeout` |
| **Формат файлов** | Только Parquet | - |
| **Equality deletes** | Не поддерживаются | - |

---

## Метрики для мониторинга

```go
// Статистика коллектора
collected, sent := collector.Stats()

// Статистика потока
batches, rows := stream.Stats()

// Статус запроса
status, _ := dfClient.QueryStatus(ctx)
// status.RegisteredSegments
// status.CompletedSources
// status.ReadyTargets
```

---

## Следующие шаги (опционально)

1. **Добавить метрики Prometheus** для мониторинга
2. **Реализовать retry логику** для GRPC соединений
3. **Добавить поддержку equality deletes**
4. **Оптимизировать сжатие** для больших объёмов данных
5. **Добавить интеграционные тесты** с реальным Postgres FDW

---

## Лицензия

Apache License 2.0
