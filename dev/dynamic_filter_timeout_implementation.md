# Применение динамических фильтров с таймаутом

## Статус: ✅ Завершено

Реализована возможность ожидания динамических фильтров перед началом сканирования с заданным таймаутом.

---

## Архитектура

### Интерфейсы

**FilterProvider** (`table/scanner.go`):
```go
type FilterProvider interface {
    // WaitForFilters ожидает готовности фильтров с указанным таймаутом.
    // Возвращает true если фильтры готовы к применению, false если таймаут или ошибка.
    WaitForFilters(ctx context.Context, timeout time.Duration) bool

    // GetFilter возвращает построенный BooleanExpression из собранных фильтров.
    // Возвращает AlwaysTrue() если фильтры не готовы или не установлены.
    GetFilter() iceberg.BooleanExpression
}
```

### Изменения в Scan

**Поля** (`table/scanner.go`):
```go
type Scan struct {
    // ... существующие поля ...
    
    filterProvider FilterProvider
    filterTimeout  time.Duration
}
```

**ScanOption** (`table/table.go`):
```go
func WithDynamicFilterProvider(provider FilterProvider, timeout time.Duration) ScanOption
```

### Интеграция в ToArrowRecords

```go
func (scan *Scan) ToArrowRecords(ctx context.Context) (...) {
    // Ожидание фильтров если filterProvider установлен
    if scan.filterProvider != nil {
        timeout := scan.filterTimeout
        if timeout == 0 {
            timeout = 30 * time.Second // default
        }
        
        waitCtx, cancel := context.WithTimeout(ctx, timeout)
        defer cancel()
        
        if scan.filterProvider.WaitForFilters(waitCtx, timeout) {
            additionalFilter := scan.filterProvider.GetFilter()
            if additionalFilter != nil && !additionalFilter.Equals(iceberg.AlwaysTrue{}) {
                // Применяем фильтр
                scan.rowFilter = iceberg.NewAnd(scan.rowFilter, additionalFilter)
                
                // Пересоздаем partition filters
                scan.partitionFilters = newKeyDefaultMapWrapErr(scan.buildPartitionProjection)
            }
        }
        // Если таймаут - продолжаем без фильтра
    }
    
    // Планирование файлов с учетом всех фильтров
    tasks, err = scan.PlanFiles(ctx)
    // ...
}
```

---

## Реализация в dynamicfilter

### ScanFilterApplier

Реализует `table.FilterProvider`:

```go
// WaitForFilters ожидает готовности фильтров с таймаутом
func (a *ScanFilterApplier) WaitForFilters(ctx context.Context, timeout time.Duration) bool {
    a.mu.Lock()
    defer a.mu.Unlock()

    if a.filtersWaited {
        return true // уже ожидано
    }

    waitCtx, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    for _, fieldID := range a.fieldIDs {
        filter, err := a.client.WaitForFilter(waitCtx, a.targetAlias, fieldID, timeout)
        if err != nil {
            a.filtersWaited = true
            return false // таймаут или ошибка
        }
        if filter != nil {
            a.appliedFilters[fieldID] = filter
        }
    }

    a.filtersWaited = true
    return true
}

// GetFilter возвращает комбинированный фильтр
func (a *ScanFilterApplier) GetFilter() iceberg.BooleanExpression {
    var predicates []iceberg.BooleanExpression
    
    for fieldID, filter := range a.appliedFilters {
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

### Конфигурация

```go
type ScanFilterApplierConfig struct {
    QueryID     string
    SessionID   string
    TargetAlias string
    FieldIDs    []int
    FieldTypes  map[int]iceberg.Type
    FieldNames  map[int]string // fieldID -> fieldName
    Timeout     time.Duration
}
```

---

## Примеры использования

### 1. Базовое использование

```go
package main

import (
    "context"
    "time"
    
    "github.com/apache/iceberg-go/table"
    "github.com/apache/iceberg-go/dynamicfilter"
)

func scanWithTimeout(ctx context.Context, tbl *table.Table, dfClient *client.Client) error {
    // Создаем аппликатор фильтров
    applier := dynamicfilter.NewScanFilterApplier(
        dynamicfilter.ScanFilterApplierConfig{
            QueryID:     "query-123",
            SessionID:   "segment-1",
            TargetAlias: "lineitem",
            FieldIDs:    []int{1},
            FieldNames:  map[int]string{1: "l_orderkey"},
            FieldTypes:  map[int]iceberg.Type{1: iceberg.PrimitiveTypes.Int64},
            Timeout:     30 * time.Second,
        },
        dfClient,
    )
    
    // Сканирование с ожиданием фильтров (максимум 30 секунд)
    scan := tbl.Scan(
        table.WithDynamicFilterProvider(applier, 30*time.Second),
        table.WithSelectedFields("l_orderkey", "l_partkey"),
    )
    
    // ToArrowRecords() заблокируется максимум на 30 секунд
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
    }
    
    return nil
}
```

### 2. Комбинирование с ValueCollector

```go
func scanWithBoth(ctx context.Context, orders, lineitem *table.Table, dfClient *client.Client) error {
    // Для orders - сбор значений
    ordersCollector := dynamicfilter.NewScanValueCollector(
        dynamicfilter.ScanValueCollectorConfig{
            QueryID:     "query-123",
            SessionID:   "segment-1",
            SourceAlias: "orders",
            FieldIDs:    []int{1},
            FieldTypes:  map[int]iceberg.Type{1: iceberg.PrimitiveTypes.Int64},
            BufferSize:  10000,
        },
        dfClient,
    )
    
    // Для lineitem - применение фильтров с таймаутом
    lineitemApplier := dynamicfilter.NewScanFilterApplier(
        dynamicfilter.ScanFilterApplierConfig{
            QueryID:     "query-123",
            SessionID:   "segment-1",
            TargetAlias: "lineitem",
            FieldIDs:    []int{1},
            FieldNames:  map[int]string{1: "l_orderkey"},
            FieldTypes:  map[int]iceberg.Type{1: iceberg.PrimitiveTypes.Int64},
            Timeout:     30 * time.Second,
        },
        dfClient,
    )
    
    g, ctx := errgroup.WithContext(ctx)
    
    // Сканирование orders со сбором значений
    g.Go(func() error {
        ordersScan := orders.Scan(
            table.WithValueCollector(ordersCollector),
        )
        _, records, _ := ordersScan.ToArrowRecords(ctx)
        for range records {
            // обработка
        }
        return nil
    })
    
    // Сканирование lineitem с ожиданием фильтров
    g.Go(func() error {
        lineitemScan := lineitem.Scan(
            table.WithDynamicFilterProvider(lineitemApplier, 30*time.Second),
        )
        _, records, _ := lineitemScan.ToArrowRecords(ctx)
        for range records {
            // обработка
        }
        return nil
    })
    
    return g.Wait()
}
```

### 3. Разные таймауты для разных таблиц

```go
// Короткий таймаут для маленьких таблиц
quickScan := smallTbl.Scan(
    table.WithDynamicFilterProvider(applier, 5*time.Second),
)

// Длинный таймаут для больших таблиц
longScan := largeTbl.Scan(
    table.WithDynamicFilterProvider(applier, 60*time.Second),
)
```

---

## Поведение

### Сценарий 1: Фильтры готовы вовремя

```
ToArrowRecords() вызван
    │
    ├─► WaitForFilters(timeout=30s)
    │   ├─► Ожидание фильтров от координатора
    │   └─► Фильтры получены через 5s → true
    │
    ├─► GetFilter() → BooleanExpression
    │
    ├─► Применение фильтра к rowFilter
    │
    ├─► Пересоздание partition filters
    │
    ├─► PlanFiles() с новым фильтром
    │   └─► Partition pruning с фильтром
    │
    └─► Чтение данных с фильтром
```

### Сценарий 2: Таймаут

```
ToArrowRecords() вызван
    │
    ├─► WaitForFilters(timeout=30s)
    │   ├─► Ожидание фильтров от координатора
    │   └─► Таймаут через 30s → false
    │
    ├─► Пропуск применения фильтра
    │
    ├─► PlanFiles() без дополнительного фильтра
    │
    └─► Чтение данных без фильтра
```

### Сценарий 3: Фильтры не нужны

```
ToArrowRecords() вызван
    │
    ├─► filterProvider == nil
    │
    ├─► Пропуск ожидания
    │
    ├─► PlanFiles() с исходными фильтрами
    │
    └─► Чтение данных
```

---

## Преимущества

### 1. Эффективность

Фильтры применяются **до** планирования файлов, что обеспечивает:
- Partition pruning с учетом динамических фильтров
- Меньше файлов для чтения
- Меньше данных для обработки

### 2. Контроль времени

- Таймаут защищает от бесконечного ожидания
- Сканирование продолжается даже если фильтры не готовы
- Можно настроить разные таймауты для разных таблиц

### 3. Обратная совместимость

- Существующий код работает без изменений
- FilterProvider опционален
- Default timeout = 30 секунд

---

## Метрики

### Время ожидания

```go
start := time.Now()
scan := tbl.Scan(table.WithDynamicFilterProvider(applier, 30*time.Second))
_, records, _ := scan.ToArrowRecords(ctx)
waitTime := time.Since(start)

if waitTime < 30*time.Second {
    println("Фильтры получены быстро:", waitTime)
} else {
    println("Таймаут или долгое ожидание:", waitTime)
}
```

### Статистика фильтров

```go
count, totalValues, types := applier.GetFilterStats()
println("Применено фильтров:", count)
println("Всего значений:", totalValues)
for ft, cnt := range types {
    println("  ", ft.String(), ":", cnt)
}
```

---

## Ограничения

1. **Блокировка начала сканирования:** ToArrowRecords() блокируется до готовности фильтров или таймаута
2. **Один вызов WaitForFilters:** Фильтры ожидаются только один раз при первом вызове ToArrowRecords()
3. **Требуется FieldNames:** Необходимо указать имена полей для построения выражений

---

## Тесты

### Unit тест

```go
func TestScanFilterProviderTimeout(t *testing.T) {
    mockProvider := new(MockFilterProvider)
    mockProvider.On("WaitForFilters", mock.Anything, 5*time.Second).Return(false)
    mockProvider.On("GetFilter").Return(iceberg.AlwaysTrue{})
    
    scan := &Scan{}
    opt := WithDynamicFilterProvider(mockProvider, 5*time.Second)
    opt(scan)
    
    assert.NotNil(t, scan.filterProvider)
    assert.Equal(t, 5*time.Second, scan.filterTimeout)
}
```

---

## Будущие улучшения

1. **Асинхронное ожидание:** Не блокировать ToArrowRecords()
2. **Progressive filtering:** Применение фильтров по мере готовности
3. **Метрики Prometheus:** Мониторинг времени ожидания и таймаутов
4. **Retry логика:** Повторная попытка при временных ошибках

---

## Лицензия

Apache License 2.0
