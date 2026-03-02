# Анализ: Применение динамических фильтров с таймаутом сбора значений

## Требование

Применять дополнительные динамические фильтры можно только после того, как:
1. Значения полностью собраны из источника для конкретного поля
2. Но не более заданного времени на сбор значений
3. Если за заданное время значения не собрались - доп фильтры не применять

## Текущая архитектура

### Точки расширения

**1. Scan структура (`table/scanner.go`):**
```go
type Scan struct {
    // ...
    valueCollector ValueCollector
}
```

**2. arrowScan структура (`table/arrow_scanner.go`):**
```go
type arrowScan struct {
    // ...
    valueCollector ValueCollector
}
```

**3. ToArrowRecords():**
- Вызывается перед началом итерации по записям
- Планирует файлы через `PlanFiles()`
- Создает `arrowScan` для чтения данных

**4. processRecords():**
- Итерирует по RecordBatch
- Вызывает `valueCollector.Collect()` для каждого батча
- Применяет pipeline обработчиков

## Проблема текущей архитектуры

Текущая реализация вызывает `Collect()` для каждого батча **во время** чтения данных, но для применения динамических фильтров нужно:

1. **Дождаться** готовности фильтров от координатора
2. **С таймаутом** - если фильтр не готов за N секунд, продолжить без него
3. **Применить** фильтр к сканеру до начала чтения

## Варианты реализации

### Вариант 1: Блокировка в ToArrowRecords с таймаутом

**Место:** `table/scanner.go` - `ToArrowRecords()`

```go
// Добавить интерфейс для ожидания фильтров
type FilterWaiter interface {
    // WaitForFilters ожидает готовности фильтров с таймаутом
    // Возвращает true если фильтры готовы, false если таймаут
    WaitForFilters(ctx context.Context, timeout time.Duration) bool
    
    // BuildFilter строит BooleanExpression из собранных фильтров
    BuildFilter() (iceberg.BooleanExpression, error)
}

// Добавить в Scan
type Scan struct {
    // ...
    valueCollector ValueCollector
    filterWaiter   FilterWaiter  // <-- новое
    filterTimeout  time.Duration // <-- новое
}

// В ToArrowRecords()
func (scan *Scan) ToArrowRecords(ctx context.Context) (...) {
    // ...
    
    // Ожидание фильтров с таймаутом
    if scan.filterWaiter != nil {
        waitCtx, cancel := context.WithTimeout(ctx, scan.filterTimeout)
        defer cancel()
        
        if scan.filterWaiter.WaitForFilters(waitCtx, scan.filterTimeout) {
            // Фильтры готовы - применяем
            additionalFilter, err := scan.filterWaiter.BuildFilter()
            if err == nil && additionalFilter != nil {
                // Объединяем с существующим фильтром
                if scan.rowFilter != nil {
                    scan.rowFilter = iceberg.NewAnd(scan.rowFilter, additionalFilter)
                } else {
                    scan.rowFilter = additionalFilter
                }
                
                // Пересчитываем boundFilter с новым фильтром
                boundFilter, err = iceberg.BindExpr(...)
            }
        }
        // Если таймаут - продолжаем без фильтра
    }
    
    // ...
}
```

**Преимущества:**
- ✅ Фильтры применяются до начала чтения данных
- ✅ Работает с существующим partition pruning
- ✅ Чистая точка интеграции

**Недостатки:**
- ❌ Блокирует начало сканирования
- ❌ Требует разделения на "сбор" и "применение"

---

### Вариант 2: Lazy применение в processRecords

**Место:** `table/arrow_scanner.go` - `processRecords()`

```go
// Добавить в arrowScan
type arrowScan struct {
    // ...
    valueCollector    ValueCollector
    filterWaiter      FilterWaiter
    filterTimeout     time.Duration
    filterApplied     bool
    filterMu          sync.Mutex
    additionalFilter  iceberg.BooleanExpression
}

func (as *arrowScan) processRecords(...) error {
    // ...
    
    // Первый батч - пытаемся применить фильтр
    if !as.filterApplied && as.filterWaiter != nil {
        as.filterMu.Lock()
        if !as.filterApplied {
            waitCtx, cancel := context.WithTimeout(ctx, as.filterTimeout)
            defer cancel()
            
            if as.filterWaiter.WaitForFilters(waitCtx, as.filterTimeout) {
                as.additionalFilter, _ = as.filterWaiter.BuildFilter()
            }
            as.filterApplied = true
        }
        as.filterMu.Unlock()
    }
    
    for recRdr.Next() {
        prev = recRdr.RecordBatch()
        prev.Retain()

        // Вызов valueCollector
        if as.valueCollector != nil {
            if err := as.valueCollector.Collect(prev); err != nil {
                return err
            }
        }

        // Применение дополнительного фильтра (если есть)
        if as.additionalFilter != nil {
            // Проверяем проходит ли батч фильтр
            // Если нет - пропускаем батч
        }

        // ... pipeline ...
    }
}
```

**Преимущества:**
- ✅ Не блокирует начало сканирования
- ✅ Можно начать обработку сразу

**Недостатки:**
- ❌ Первый батч может быть обработан без фильтра
- ❌ Сложнее с thread safety
- ❌ Filter применяется к уже прочитанным данным

---

### Вариант 3: Двухфазное сканирование (рекомендуется)

**Идея:** Разделить сканирование на две фазы:
1. **Планирование** - ожидание фильтров с таймаутом
2. **Чтение** - применение фильтров

**Реализация:**

```go
// Новый интерфейс
type DynamicFilterManager interface {
    // StartCollection начинает сбор значений для source полей
    StartCollection(ctx context.Context) error
    
    // WaitForTargetFilters ожидает фильтры для target полей
    WaitForTargetFilters(ctx context.Context, timeout time.Duration) bool
    
    // BuildFilter строит фильтр
    BuildFilter() (iceberg.BooleanExpression, error)
    
    // GetCollector возвращает коллектор для передачи в Scan
    GetCollector() ValueCollector
}

// ScanOption
func WithDynamicFilterManager(mgr DynamicFilterManager, timeout time.Duration) ScanOption {
    return func(scan *Scan) {
        scan.filterManager = mgr
        scan.filterTimeout = timeout
    }
}

// В ToArrowRecords()
func (scan *Scan) ToArrowRecords(ctx context.Context) (...) {
    // Фаза 1: Ожидание фильтров (только для target таблиц)
    if scan.filterManager != nil {
        waitCtx, cancel := context.WithTimeout(ctx, scan.filterTimeout)
        defer cancel()
        
        if scan.filterManager.WaitForTargetFilters(waitCtx, scan.filterTimeout) {
            additionalFilter, err := scan.filterManager.BuildFilter()
            if err == nil && additionalFilter != nil {
                // Применяем фильтр до планирования файлов!
                if scan.rowFilter != nil {
                    scan.rowFilter = iceberg.NewAnd(scan.rowFilter, additionalFilter)
                } else {
                    scan.rowFilter = additionalFilter
                }
                
                // Пересоздаем partition filters с новым фильтром
                scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)
                
                // Перепланируем файлы с новым фильтром
                tasks, err = scan.PlanFiles(ctx)
            }
        }
    }
    
    // Фаза 2: Чтение данных с коллектором
    return (&arrowScan{
        // ...
        valueCollector: scan.valueCollector,
    }).GetRecords(ctx, tasks)
}
```

**Преимущества:**
- ✅ Фильтры применяются до планирования файлов (максимальная эффективность)
- ✅ Четкое разделение фаз
- ✅ Таймаут контролируется
- ✅ Если таймаут - сканирование продолжается без фильтра

**Недостатки:**
- ❌ Требует больше изменений в архитектуре

---

## Рекомендуемая реализация

### Модификация Вариант 3 с упрощениями

**1. Расширить ValueCollector:**

```go
// В table/scanner.go
type ValueCollector interface {
    Collect(batch arrow.RecordBatch) error
    Finalize() error
}

// Новый интерфейс для ожидания фильтров
type FilterProvider interface {
    // WaitForFilters ожидает готовности фильтров с таймаутом
    // Возвращает true если фильтры готовы к применению
    WaitForFilters(ctx context.Context, timeout time.Duration) bool
    
    // GetFilter возвращает построенный фильтр (после WaitForFilters)
    GetFilter() iceberg.BooleanExpression
}

// ValueCollector с поддержкой FilterProvider
type ValueCollectorWithFilter interface {
    ValueCollector
    FilterProvider
}
```

**2. Добавить опцию:**

```go
// В table/table.go
func WithDynamicFilterTimeout(timeout time.Duration) ScanOption {
    return func(scan *Scan) {
        scan.filterTimeout = timeout
    }
}
```

**3. Интеграция в ToArrowRecords:**

```go
// В table/scanner.go
func (scan *Scan) ToArrowRecords(ctx context.Context) (...) {
    // Ожидание фильтров если коллектор поддерживает FilterProvider
    if filterProvider, ok := scan.valueCollector.(FilterProvider); ok {
        timeout := scan.filterTimeout
        if timeout == 0 {
            timeout = 30 * time.Second // default
        }
        
        waitCtx, cancel := context.WithTimeout(ctx, timeout)
        defer cancel()
        
        if filterProvider.WaitForFilters(waitCtx, timeout) {
            additionalFilter := filterProvider.GetFilter()
            if additionalFilter != nil && !additionalFilter.Equals(iceberg.AlwaysTrue{}) {
                // Применяем фильтр
                if scan.rowFilter != nil {
                    scan.rowFilter = iceberg.NewAnd(scan.rowFilter, additionalFilter)
                } else {
                    scan.rowFilter = additionalFilter
                }
                
                // Пересоздаем partition filters
                scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)
                
                // Перепланируем файлы с новым фильтром
                tasks, err = scan.PlanFiles(ctx)
                if err != nil {
                    return nil, nil, err
                }
            }
        }
    }
    
    // ... остальной код
}
```

---

## Реализация в dynamicfilter

### ScanFilterApplier с поддержкой WaitForFilters

```go
// В dynamicfilter/scan_filter_applier.go

// ScanFilterApplier реализует FilterProvider
var _ table.FilterProvider = (*ScanFilterApplier)(nil)

// WaitForFilters ожидает готовности фильтров с таймаутом
func (a *ScanFilterApplier) WaitForFilters(ctx context.Context, timeout time.Duration) bool {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    // Если уже ожидано - возвращаем true
    if a.filtersWaited {
        return true
    }
    
    // Создаем контекст с таймаутом
    waitCtx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()
    
    // Ожидаем фильтры для всех полей
    for _, fieldID := range a.fieldIDs {
        filter, err := a.client.WaitForFilter(waitCtx, a.targetAlias, fieldID, timeout)
        if err != nil {
            // Таймаут или ошибка - продолжаем без фильтра
            return false
        }
        if filter != nil {
            a.appliedFilters[fieldID] = filter
        }
    }
    
    a.filtersWaited = true
    return true
}

// GetFilter возвращает построенный фильтр
func (a *ScanFilterApplier) GetFilter() iceberg.BooleanExpression {
    a.mu.RLock()
    defer a.mu.RUnlock()
    
    var predicates []iceberg.BooleanExpression
    
    for fieldID, filter := range a.appliedFilters {
        if filter == nil {
            continue
        }
        
        // Получаем имя поля из конфигурации
        fieldName := a.fieldNames[fieldID]
        fieldRef := iceberg.Reference(fieldName)
        
        expr := filter.BuildExpression(fieldRef)
        if expr != nil && !expr.Equals(iceberg.AlwaysTrue{}) {
            predicates = append(predicates, expr)
        }
    }
    
    if len(predicates) == 0 {
        return iceberg.AlwaysTrue{}
    }
    
    if len(predicates) == 1 {
        return predicates[0]
    }
    
    return iceberg.NewOr(predicates[0], predicates[1], predicates[2:]...)
}
```

---

## Пример использования

```go
package main

import (
    "context"
    "time"
    
    "github.com/apache/iceberg-go/table"
    "github.com/apache/iceberg-go/dynamicfilter"
)

func scanWithDynamicFilters(ctx context.Context, tbl *table.Table, dfClient *client.Client) error {
    // Создаем аппликатор фильтров
    applier := dynamicfilter.NewScanFilterApplier(
        dynamicfilter.ScanFilterApplierConfig{
            QueryID:     "query-123",
            SessionID:   "segment-1",
            TargetAlias: "lineitem",
            FieldIDs:    []int{1},
            FieldTypes:  map[int]iceberg.Type{1: iceberg.PrimitiveTypes.Int64},
            Timeout:     30 * time.Second,
        },
        dfClient,
    )
    
    // Сканирование с таймаутом ожидания фильтров
    scan := tbl.Scan(
        table.WithDynamicFilterManager(applier, 30*time.Second),
        table.WithSelectedFields("l_orderkey", "l_partkey"),
    )
    
    // ToArrowRecords() заблокируется максимум на 30 секунд
    // ожидая фильтры от координатора
    _, records, err := scan.ToArrowRecords(ctx)
    if err != nil {
        return err
    }
    
    for batch, err := range records {
        if err != nil {
            return err
        }
        defer batch.Release()
        
        // Обработка батча
        // ...
    }
    
    return nil
}
```

---

## Сравнение подходов

| Подход | Эффективность | Сложность | Обратная совместимость | Рекомендация |
|--------|--------------|-----------|----------------------|--------------|
| Блокировка в ToArrowRecords | Высокая | Низкая | ✅ | ⭐⭐⭐⭐ |
| Lazy в processRecords | Средняя | Средняя | ✅ | ⭐⭐ |
| Двухфазное сканирование | Максимальная | Высокая | ✅ | ⭐⭐⭐⭐⭐ |

---

## Выводы

**Рекомендуемый подход:** Модифицированный Вариант 3

**Изменения:**
1. Добавить интерфейс `FilterProvider` в `table/scanner.go`
2. Добавить `WithDynamicFilterTimeout()` опцию в `table/table.go`
3. Интегрировать ожидание фильтров в `ToArrowRecords()` до вызова `PlanFiles()`
4. Реализовать `FilterProvider` в `dynamicfilter/ScanFilterApplier`

**Преимущества:**
- Фильтры применяются до планирования файлов (partition pruning)
- Таймаут контролируется через context
- Если таймаут - сканирование продолжается без фильтра
- Минимальные изменения в основной библиотеке

**Время реализации:** 4-6 часов
