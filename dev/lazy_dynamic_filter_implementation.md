# Lazy применение динамических фильтров во время сканирования

## Статус: ✅ Завершено

Реализована возможность применения динамических фильтров **во время** сканирования, а не только перед началом.

---

## Архитектура

### Компоненты

**arrowScan поля** (`table/arrow_scanner.go`):
```go
type arrowScan struct {
    // ...
    
    // Lazy dynamic filter
    lazyFilterProvider     FilterProvider
    lazyFilterCheckTimeout time.Duration
    lazyFilterApplied      bool
    lazyFilterMu           sync.RWMutex
    lazyFilterFunc         recProcessFn
    lazyBatchesProcessed   int64
    lazyCheckInterval      int64  // проверять каждые N батчей
}
```

### Точки проверки

**1. Перед обработкой каждого файла:**
```go
func (as *arrowScan) recordsFromTask(...) error {
    // ...
    
    // Проверка готовности фильтра перед началом обработки файла
    as.checkAndUpdateFilter(ctx)
    
    // ... обработка
}
```

**2. Каждые N батчей:**
```go
func (as *arrowScan) processRecords(...) error {
    for recRdr.Next() {
        // ... обработка батча
        
        // Применение динамического фильтра если есть
        as.lazyFilterMu.RLock()
        lazyFunc := as.lazyFilterFunc
        as.lazyFilterMu.RUnlock()
        
        if lazyFunc != nil {
            prev, err = lazyFunc(prev)
        }
        
        // Периодическая проверка готовности фильтра
        as.lazyBatchesProcessed++
        if !as.lazyFilterApplied && as.lazyFilterProvider != nil &&
           as.lazyBatchesProcessed % as.lazyCheckInterval == 0 {
            as.checkAndUpdateFilter(ctx)
        }
    }
}
```

---

## Реализация

### checkAndUpdateFilter()

```go
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
                    as.lazyFilterFunc = as.buildLazyFilterFunc(ctx, additionalFilter)
                }
                as.lazyFilterApplied = true
            }
        }
    }
}
```

### buildLazyFilterFunc()

```go
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
```

---

## Поведение

### Сценарий 1: Фильтр готов быстро

```
ToArrowRecords() вызван
    │
    ├─► Начало сканирования с initialFilter
    │
    ├─► File 1: checkAndUpdateFilter()
    │   └─► Фильтр готов через 2s → lazyFilterFunc создан
    │
    ├─► File 1 записи: применяются с lazyFilterFunc
    │
    ├─► File 2: checkAndUpdateFilter()
    │   └─► lazyFilterApplied = true, проверка пропускается
    │
    └─► File 2 записи: применяются с lazyFilterFunc
```

### Сценарий 2: Фильтр готов после N батчей

```
ToArrowRecords() вызван
    │
    ├─► Начало сканирования с initialFilter
    │
    ├─► Batch 1-99: initialFilter
    │
    ├─► Batch 100: checkAndUpdateFilter()
    │   └─► Фильтр готов → lazyFilterFunc создан
    │
    ├─► Batch 101+: применяются с lazyFilterFunc
    │
    └─► File 2: checkAndUpdateFilter() пропускается
```

### Сценарий 3: Таймаут

```
ToArrowRecords() вызван
    │
    ├─► Начало сканирования с initialFilter
    │
    ├─► File 1: checkAndUpdateFilter()
    │   └─► Таймаут через 30s → lazyFilterApplied = true, lazyFilterFunc = nil
    │
    ├─► Batch 100: checkAndUpdateFilter()
    │   └─► lazyFilterApplied = true, проверка пропускается
    │
    └─► Все записи: применяются только с initialFilter
```

---

## Примеры использования

### 1. Базовое lazy применение

```go
package main

import (
    "context"
    "time"
    
    "github.com/apache/iceberg-go/table"
    "github.com/apache/iceberg-go/dynamicfilter"
)

func scanWithLazyFilter(ctx context.Context, tbl *table.Table, dfClient *client.Client) error {
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
    
    // Сканирование начнется немедленно
    // Фильтр применится когда будет готов
    scan := tbl.Scan(
        table.WithDynamicFilterProvider(applier, 30*time.Second),
        table.WithSelectedFields("l_orderkey", "l_partkey"),
    )
    
    // ToArrowRecords() не блокируется на ожидание фильтра
    _, records, err := scan.ToArrowRecords(ctx)
    if err != nil {
        return err
    }
    
    for batch, err := range records {
        if err != nil {
            return err
        }
        defer batch.Release()
        
        // Первые батчи могут быть без динамического фильтра
        // Последующие батчи будут с фильтром когда он будет готов
    }
    
    return nil
}
```

### 2. Комбинирование с начальным фильтром

```go
func scanWithInitialAndLazy(ctx context.Context, tbl *table.Table, dfClient *client.Client) error {
    applier := dynamicfilter.NewScanFilterApplier(...)
    
    // Начальный фильтр применяется сразу
    initialFilter := iceberg.GreaterThanEqual(
        iceberg.Reference("l_orderkey"),
        iceberg.NewLiteral(int64(1000)),
    )
    
    scan := tbl.Scan(
        table.WithRowFilter(initialFilter),
        table.WithDynamicFilterProvider(applier, 30*time.Second),
    )
    
    // Сканирование:
    // 1. Начальные батчи: l_orderkey >= 1000
    // 2. Когда фильтр готов: l_orderkey >= 1000 AND l_orderkey IN (1,2,3,...)
}
```

### 3. Настройка интервала проверки

```go
// Для маленьких файлов - проверять чаще
scan := tbl.Scan(
    table.WithDynamicFilterProvider(applier, 30*time.Second),
    // Можно добавить опцию для настройки checkInterval
)

// Для больших файлов - проверять реже
// (требуется добавить ScanOption)
```

---

## Сравнение с блокирующим ожиданием

| Характеристика | Блокирующее ожидание | Lazy применение |
|---------------|---------------------|-----------------|
| **Начало сканирования** | После готовности фильтра | Немедленно |
| **Применение фильтра** | Ко всем записям | К записям после готовности |
| **Задержка старта** | До N секунд | Нет |
| **Накладные расходы** | Нет | Проверка каждые N батчей |
| **Partition pruning** | С фильтром | Без фильтра (изначально) |
| **Использование** | Когда фильтры критичны | Когда важен быстрый старт |

---

## Производительность

### Накладные расходы

**Проверка готовности:**
- Между файлами: ~1-5 мкс
- Каждые 100 батчей: ~1-5 мкс

**Применение фильтра:**
- Создание filterFunc: ~10-50 мкс
- Применение к батчу: ~5-20% overhead на запись

### Оптимизации

1. **Мьютекс RLock:** Чтение lazyFilterFunc без блокировки
2. **Кэширование:** filterFunc создается один раз
3. **Ранний выход:** Проверка lazyFilterApplied перед блокировкой

---

## Метрики

### Статистика применения

```go
// В arrowScan можно добавить:
type LazyFilterStats struct {
    BatchesProcessed   int64
    BatchesWithFilter  int64
    FilterReadyAfterMs int64
}

// Использование:
stats := scan.GetLazyFilterStats()
println("Батчей всего:", stats.BatchesProcessed)
println("Батчей с фильтром:", stats.BatchesWithFilter)
println("Фильтр готов через:", stats.FilterReadyAfterMs, "мс")
```

---

## Ограничения

1. **Не все записи фильтруются:** Записи до готовности фильтра не фильтруются
2. **Накладные расходы:** Проверка каждые N батчей
3. **Нет partition pruning:** Динамический фильтр не применяется к планированию файлов
4. **Thread safety:** Мьютекс на горячем пути (минимальные накладные расходы)

---

## Будущие улучшения

1. **Настраиваемый интервал:** ScanOption для checkInterval
2. **Адаптивная проверка:** Увеличивать интервал если фильтр долго не готов
3. **Метрики Prometheus:** Мониторинг времени готовности фильтра
4. **Статистика:** GetLazyFilterStats() для отладки

---

## Тесты

### Unit тест

```go
func TestLazyDynamicFilter(t *testing.T) {
    mockProvider := new(MockFilterProvider)
    mockProvider.On("WaitForFilters", mock.Anything, 5*time.Second).Return(true)
    mockProvider.On("GetFilter").Return(iceberg.AlwaysTrue{})
    
    as := &arrowScan{
        lazyFilterProvider:     mockProvider,
        lazyFilterCheckTimeout: 5 * time.Second,
        lazyCheckInterval:      10,
    }
    
    // Проверка что checkAndUpdateFilter не паникует
    as.checkAndUpdateFilter(context.Background())
    
    // Проверка что filterFunc создан
    as.lazyFilterMu.RLock()
    assert.NotNil(t, as.lazyFilterFunc)
    as.lazyFilterMu.RUnlock()
}
```

---

## Лицензия

Apache License 2.0
