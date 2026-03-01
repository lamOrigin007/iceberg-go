# Анализ интеграции динамических фильтров в основную библиотеку сканирования

## Текущая архитектура сканирования

### Ключевые компоненты

1. **Scan** (`table/scanner.go`)
   - `PlanFiles(ctx)` - планирование файлов для сканирования
   - `ToArrowRecords(ctx)` - получение итератора RecordBatch
   - `ToArrowTable(ctx)` - получение полной Arrow таблицы

2. **arrowScan** (`table/arrow_scanner.go`)
   - `GetRecords(ctx, tasks)` - чтение данных из файлов
   - `recordBatchesFromTasksAndDeletes()` - координация чтения
   - `recordsFromTask()` - обработка单个 файла

3. **Точки расширения**
   - `Scan` структура - содержит metadata, rowFilter, options
   - `arrowScan` структура - содержит projectedSchema, boundRowFilter

## Возможные точки интеграции

### 1. Scan Option для динамических фильтров

**Место:** `table/table.go` - Scan options

```go
// В table/table.go добавить:

import "github.com/apache/iceberg-go/dynamicfilter/client"

// WithDynamicFilterClient устанавливает клиент для динамических фильтров
func WithDynamicFilterClient(dfClient *client.Client) ScanOption {
    return func(scan *Scan) {
        scan.dfClient = dfClient
    }
}

// WithDynamicFilterConfig устанавливает конфигурацию динамических фильтров
func WithDynamicFilterConfig(cfg *dynamicfilter.Config) ScanOption {
    return func(scan *Scan) {
        scan.dfConfig = cfg
    }
}
```

**Изменения в Scan структуре:**
```go
type Scan struct {
    // ... существующие поля ...
    
    // Динамические фильтры
    dfClient  *client.Client      // GRPC клиент к координатору
    dfConfig  *dynamicfilter.Config
    dfSession *dynamicfilter.Session
}
```

**Преимущества:**
- ✅ Чистый API через functional options
- ✅ Обратная совместимость (опционально)
- ✅ Легко тестировать

**Недостатки:**
- ❌ Требует изменений в основной структуре Scan
- ❌ Зависимость от пакета dynamicfilter

---

### 2. Callback хуки для сбора значений

**Место:** `table/arrow_scanner.go` - processRecords

```go
// В arrowScan добавить:
type arrowScan struct {
    // ... существующие поля ...
    
    // Callback для сбора значений
    valueCollector func(arrow.RecordBatch) error
}

// В processRecords():
func (as *arrowScan) processRecords(...) error {
    // ... существующий код ...
    
    for recRdr.Next() {
        rec := recRdr.RecordBatch()
        
        // Вызов callback для сбора значений
        if as.valueCollector != nil {
            if err := as.valueCollector(rec); err != nil {
                return err
            }
        }
        
        // ... остальная обработка ...
    }
}
```

**Использование:**
```go
scan := table.Scan(
    table.WithValueCollector(func(batch arrow.RecordBatch) error {
        // Извлечение значений из указанных колонок
        values := extractValues(batch, fieldIDs)
        return dfClient.SendValues(ctx, sourceAlias, fieldID, values)
    }),
)
```

**Преимущества:**
- ✅ Гибкий механизм для различных use cases
- ✅ Может использоваться не только для динамических фильтров
- ✅ Минимальные изменения в основной код

**Недостатки:**
- ❌ Пользователь должен сам реализовать логику сбора
- ❌ Нет автоматической отправки по завершении

---

### 3. Interface-based подход (рекомендуется)

**Место:** `table/scanner.go` - определение интерфейса

```go
// В table/scanner.go добавить интерфейс:

// ValueCollector определяет интерфейс для сбора значений из RecordBatch
type ValueCollector interface {
    // Collect вызывается для каждого RecordBatch
    Collect(batch arrow.RecordBatch) error
    
    // Finalize вызывается после завершения сканирования
    Finalize() error
}

// ScanOption для установки коллектора
func WithValueCollector(collector ValueCollector) ScanOption {
    return func(scan *Scan) {
        scan.valueCollector = collector
    }
}
```

**Изменения в Scan:**
```go
type Scan struct {
    // ... существующие поля ...
    valueCollector ValueCollector
}
```

**Интеграция в ToArrowRecords:**
```go
func (scan *Scan) ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
    // ... существующий код ...
    
    as := &arrowScan{
        // ...
        valueCollector: scan.valueCollector,
    }
    
    return as.GetRecords(ctx, tasks)
}
```

**Реализация для dynamicfilter:**
```go
// В dynamicfilter/integration.go:

type DFValueCollector struct {
    client      *client.Client
    queryID     string
    sessionID   string
    sourceAlias string
    fieldIDs    []int
    buffers     map[int][]any
}

func (c *DFValueCollector) Collect(batch arrow.RecordBatch) error {
    for _, fieldID := range c.fieldIDs {
        values := extractFromBatch(batch, fieldID)
        c.buffers[fieldID] = append(c.buffers[fieldID], values...)
        
        // Отправка при заполнении буфера
        if len(c.buffers[fieldID]) >= DefaultBatchSize {
            if err := c.client.SendValues(ctx, c.sourceAlias, fieldID, c.buffers[fieldID]); err != nil {
                return err
            }
            c.buffers[fieldID] = nil
        }
    }
    return nil
}

func (c *DFValueCollector) Finalize() error {
    // Отправка оставшихся значений
    for fieldID, values := range c.buffers {
        if len(values) > 0 {
            if err := c.client.SendValues(ctx, c.sourceAlias, fieldID, values); err != nil {
                return err
            }
        }
        // Сигнал завершения
        if err := c.client.SignalSourceComplete(ctx, c.sourceAlias, fieldID); err != nil {
            return err
        }
    }
    return nil
}
```

**Преимущества:**
- ✅ Чистое разделение ответственности
- ✅ Основная библиотека не знает о dynamicfilter
- ✅ Легко тестировать через моки
- ✅ Расширяемо для других use cases
- ✅ Обратная совместимость

**Недостатки:**
- ❌ Требует больше кода для реализации

---

### 4. Интеграция в Scan.PlanFiles для применения фильтров

**Место:** `table/scanner.go` - PlanFiles

```go
// Добавить опцию для применения динамических фильтров:

func (scan *Scan) PlanFiles(ctx context.Context) ([]FileScanTask, error) {
    // Применить динамические фильтры перед планированием
    if scan.dfClient != nil && scan.dfConfig != nil {
        if err := scan.applyDynamicFilters(ctx); err != nil {
            return nil, err
        }
    }
    
    // ... существующий код ...
}

func (scan *Scan) applyDynamicFilters(ctx context.Context) error {
    // Ожидание фильтров для target полей
    for _, mapping := range scan.dfConfig.Mappings {
        filter, err := scan.dfClient.WaitForFilter(
            ctx,
            mapping.TargetAlias,
            mapping.TargetFieldID,
            DefaultWaitTimeout,
        )
        if err != nil {
            return err
        }
        
        if filter != nil {
            // Построение выражения
            fieldRef := iceberg.Reference(mapping.TargetField)
            expr := filter.BuildExpression(fieldRef)
            
            // Объединение с существующим фильтром
            if scan.rowFilter != nil && !scan.rowFilter.Equals(iceberg.AlwaysTrue{}) {
                scan.rowFilter = iceberg.NewAnd(scan.rowFilter, expr)
            } else {
                scan.rowFilter = expr
            }
        }
    }
    
    // Пересоздать partition filters с новым фильтром
    scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)
    
    return nil
}
```

**Преимущества:**
- ✅ Фильтры применяются до чтения данных
- ✅ Работает с существующим partition pruning
- ✅ Уменьшает объем читаемых данных

**Недостатки:**
- ❌ Блокирует выполнение до готовности фильтров
- ❌ Требует осторожности с таймаутами

---

## Рекомендуемая архитектура интеграции

### Минимальные изменения в основной библиотеке

1. **Добавить интерфейс ValueCollector** в `table/scanner.go`:
```go
type ValueCollector interface {
    Collect(batch arrow.RecordBatch) error
    Finalize() error
}
```

2. **Добавить ScanOption** в `table/table.go`:
```go
func WithValueCollector(collector ValueCollector) ScanOption
```

3. **Интегрировать в arrowScan** в `table/arrow_scanner.go`:
```go
type arrowScan struct {
    // ...
    valueCollector ValueCollector
}
```

4. **Вызывать collector** в `processRecords()`:
```go
for recRdr.Next() {
    rec := recRdr.RecordBatch()
    
    if as.valueCollector != nil {
        if err := as.valueCollector.Collect(rec); err != nil {
            return err
        }
    }
    
    // ... обработка ...
}

// После завершения
if as.valueCollector != nil {
    as.valueCollector.Finalize()
}
```

### Реализация в dynamicfilter

Создать адаптер в `dynamicfilter/integration.go`:

```go
// ScanValueCollector реализует table.ValueCollector для dynamicfilter
type ScanValueCollector struct {
    // ... поля ...
}

func (c *ScanValueCollector) Collect(batch arrow.RecordBatch) error {
    // Извлечение и отправка значений
}

func (c *ScanValueCollector) Finalize() error {
    // Завершение и отправка сигналов
}

// NewScanValueCollector создает коллектор для использования с table.Scan
func NewScanValueCollector(cfg *Config, dfClient *client.Client) *ScanValueCollector {
    // ...
}
```

### Использование в FDW

```go
// В FDW коде:
collector := dynamicfilter.NewScanValueCollector(cfg, dfClient)

scan := tbl.Scan(
    table.WithValueCollector(collector),
    table.WithRowFilter(existingFilter),
)

// Для target таблиц - ожидание фильтров
if cfg.IsTargetTable(alias) {
    filterApplier := dynamicfilter.NewFilterApplier(cfg, dfClient)
    expr, err := filterApplier.WaitForFilterAndBuild(ctx, alias, fieldID)
    if err == nil && expr != nil {
        scan = tbl.Scan(
            table.WithValueCollector(collector),
            table.WithRowFilter(iceberg.NewAnd(existingFilter, expr)),
        )
    }
}
```

---

## Сравнение подходов

| Подход | Сложность | Обратная совместимость | Гибкость | Рекомендуемость |
|--------|-----------|----------------------|----------|-----------------|
| Scan Option с client | Низкая | ✅ | Средняя | ⭐⭐⭐ |
| Callback хуки | Низкая | ✅ | Высокая | ⭐⭐ |
| **Interface-based** | Средняя | ✅ | **Высокая** | ⭐⭐⭐⭐⭐ |
| Интеграция в PlanFiles | Средняя | ✅ | Низкая | ⭐⭐⭐ |

---

## План реализации

### Этап 1: Базовый интерфейс (1-2 часа)

1. Добавить `ValueCollector` интерфейс в `table/scanner.go`
2. Добавить `WithValueCollector()` опцию в `table/table.go`
3. Интегрировать вызов collector в `table/arrow_scanner.go`

### Этап 2: Адаптер dynamicfilter (2-3 часа)

1. Создать `ScanValueCollector` в `dynamicfilter/integration.go`
2. Реализовать методы `Collect()` и `Finalize()`
3. Добавить фабричную функцию `NewScanValueCollector()`

### Этап 3: Применение фильтров (2-3 часа)

1. Добавить `WithDynamicFilterApplier()` опцию
2. Интегрировать в `PlanFiles()` или `ToArrowRecords()`
3. Обработать таймауты и fallback поведение

### Этап 4: Тесты и документация (2-3 часа)

1. Unit тесты для `ValueCollector`
2. Integration тесты с mock GRPC сервером
3. Обновление документации

**Итого:** 7-11 часов работы

---

## Пример кода после интеграции

```go
package main

import (
    "context"
    "github.com/apache/iceberg-go/table"
    "github.com/apache/iceberg-go/dynamicfilter"
    "github.com/apache/iceberg-go/dynamicfilter/client"
)

func main() {
    ctx := context.Background()
    
    // Подключение к координатору
    dfClient, _ := client.Connect("localhost:9999")
    defer dfClient.Close()
    
    // Конфигурация
    cfg := &dynamicfilter.Config{
        QueryID:     "query-123",
        SessionID:   "segment-1",
        TableAlias:  "orders",
        IsSource:    true,
        SourceFields: []int{1, 2},
    }
    
    // Создание коллектора
    collector := dynamicfilter.NewScanValueCollector(cfg, dfClient)
    
    // Сканирование с сбором значений
    scan := tbl.Scan(
        table.WithValueCollector(collector),
        table.WithSelectedFields("o_orderkey", "o_custkey"),
    )
    
    // Чтение данных
    schema, records, _ := scan.ToArrowRecords(ctx)
    for batch, err := range records {
        if err != nil { break }
        // Обработка батча
    }
    // collector.Finalize() вызывается автоматически
}
```

---

## Выводы

**Рекомендуемый подход:** Interface-based с минимальными изменениями

**Преимущества:**
- Основная библиотека остается независимой от dynamicfilter
- Чистый API через Scan options
- Легко тестировать и расширять
- Обратная совместимость сохраняется

**Изменения в основной библиотеке:**
- ~50 строк кода (интерфейс + опция + интеграция)
- Никаких новых зависимостей
- Минимальный риск регрессии
