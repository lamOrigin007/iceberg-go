# Анализ: Lazy применение динамических фильтров во время сканирования

## Требование

Начать сканирование с исходным фильтром, во время сканирования периодически проверять готовность динамического фильтра, и если он готов за определенное время - добавить его к существующему фильтру и продолжить сканирование с обновленным фильтром.

## Текущая архитектура

### Точки расширения

**1. processRecords()** (`table/arrow_scanner.go`):
```go
func (as *arrowScan) processRecords(...) error {
    for recRdr.Next() {
        prev = recRdr.RecordBatch()
        prev.Retain()

        // Вызов valueCollector
        if as.valueCollector != nil {
            as.valueCollector.Collect(prev)
        }

        // Применение pipeline (включая filterFunc)
        for _, f := range pipeline {
            prev, err = f(prev)
        }
    }
}
```

**2. recordsFromTask()** (`table/arrow_scanner.go`):
```go
func (as *arrowScan) recordsFromTask(...) error {
    // filterFunc создается один раз при начале обработки файла
    filterFunc, dropFile, err = as.getRecordFilter(ctx, iceSchema)
    
    pipeline := make([]recProcessFn, 0, 2)
    if filterFunc != nil {
        pipeline = append(pipeline, filterFunc)
    }
    
    // ... обработка записей
}
```

**3. createIterator()** (`table/arrow_scanner.go`):
```go
return func(yield func(arrow.RecordBatch, error) bool) {
    for {
        select {
        case enum, ok := <-sequenced:
            rec := enum.Record.Value
            if !yield(rec, nil) {
                return
            }
        }
    }
}
```

## Проблема

Текущая реализация создает `filterFunc` **один раз** при начале обработки каждого файла в `recordsFromTask()`. Для применения динамического фильтра во время сканирования нужно:

1. Проверять готовность фильтра **периодически** во время обработки
2. При готовности - **обновить pipeline** с новым фильтром
3. Применять новый фильтр к **последующим** записям

## Варианты реализации

### Вариант 1: Проверка в processRecords() перед каждой записью

**Идея:** Проверять готовность фильтра перед обработкой каждой записи и обновлять pipeline.

```go
// Добавить в arrowScan
type arrowScan struct {
    // ...
    filterProvider     FilterProvider
    filterCheckTimeout time.Duration
    filterApplied      bool
    filterMu           sync.RWMutex
    additionalFilter   iceberg.BooleanExpression
    filterFunc         recProcessFn
}

func (as *arrowScan) processRecords(...) error {
    for recRdr.Next() {
        // Проверка готовности фильтра (периодически)
        if !as.filterApplied && as.filterProvider != nil {
            as.filterMu.Lock()
            if !as.filterApplied {
                waitCtx, cancel := context.WithTimeout(ctx, as.filterCheckTimeout)
                if as.filterProvider.WaitForFilters(waitCtx, as.filterCheckTimeout) {
                    additionalFilter := as.filterProvider.GetFilter()
                    if additionalFilter != nil {
                        as.additionalFilter = additionalFilter
                        // Создаем новый filterFunc с динамическим фильтром
                        as.filterFunc = as.buildFilterFunc(ctx, additionalFilter)
                    }
                    as.filterApplied = true
                }
                cancel()
            }
            as.filterMu.Unlock()
        }

        prev = recRdr.RecordBatch()
        prev.Retain()

        // Применение pipeline с возможным новым фильтром
        for _, f := range pipeline {
            prev, err = f(prev)
        }
        
        // Применение динамического фильтра если есть
        if as.filterFunc != nil {
            prev, err = as.filterFunc(prev)
        }
    }
}
```

**Преимущества:**
- ✅ Фильтр применяется быстро после готовности
- ✅ Минимальная задержка

**Недостатки:**
- ❌ Проверка перед каждой записью - накладные расходы
- ❌ Сложность с thread safety (мьютекс на горячем пути)
- ❌ Нужно обновлять pipeline для каждого файла

---

### Вариант 2: Проверка между файлами (рекомендуется)

**Идея:** Проверять готовность фильтра между обработкой файлов (tasks).

```go
// В recordsFromTask()
func (as *arrowScan) recordsFromTask(ctx context.Context, task ..., out ...) error {
    // ...
    
    // Проверка готовности фильтра перед началом обработки файла
    if !as.filterApplied && as.filterProvider != nil {
        as.filterMu.Lock()
        if !as.filterApplied {
            waitCtx, cancel := context.WithTimeout(ctx, as.filterCheckTimeout)
            if as.filterProvider.WaitForFilters(waitCtx, as.filterCheckTimeout) {
                additionalFilter := as.filterProvider.GetFilter()
                if additionalFilter != nil {
                    as.additionalFilter = additionalFilter
                    // Обновляем filterFunc с новым фильтром
                    filterFunc, dropFile = as.getRecordFilterWithAdditional(ctx, iceSchema, additionalFilter)
                    if filterFunc != nil {
                        pipeline = append(pipeline, filterFunc)
                    }
                }
                as.filterApplied = true
            }
            cancel()
        }
        as.filterMu.Unlock()
    }
    
    // ... остальная обработка
}
```

**Преимущества:**
- ✅ Проверка только между файлами - меньше накладных расходов
- ✅ Проще с thread safety
- ✅ Естественная точка расширения

**Недостатки:**
- ❌ Задержка применения фильтра (до следующего файла)
- ❌ Для больших файлов может быть долгая задержка

---

### Вариант 3: Асинхронное уведомление + channel

**Идея:** Использовать channel для асинхронного уведомления о готовности фильтра.

```go
// Добавить в arrowScan
type arrowScan struct {
    // ...
    filterReadyChan chan iceberg.BooleanExpression
    filterApplied   bool
    filterMu        sync.RWMutex
    filterFunc      recProcessFn
}

// В ToArrowRecords()
func (scan *Scan) ToArrowRecords(ctx context.Context) (...) {
    // ...
    
    as := &arrowScan{
        // ...
        filterReadyChan: make(chan iceberg.BooleanExpression, 1),
    }
    
    // Асинхронное ожидание фильтра
    if scan.filterProvider != nil {
        go func() {
            if scan.filterProvider.WaitForFilters(ctx, scan.filterTimeout) {
                filter := scan.filterProvider.GetFilter()
                select {
                case as.filterReadyChan <- filter:
                default:
                }
            }
        }()
    }
    
    return as.GetRecords(ctx, tasks)
}

// В processRecords()
func (as *arrowScan) processRecords(...) error {
    for recRdr.Next() {
        // Неблокирующая проверка готовности фильтра
        select {
        case additionalFilter := <-as.filterReadyChan:
            as.filterMu.Lock()
            if !as.filterApplied {
                as.filterFunc = as.buildFilterFunc(ctx, additionalFilter)
                as.filterApplied = true
            }
            as.filterMu.Unlock()
        default:
            // Фильтр еще не готов - продолжаем
        }

        prev = recRdr.RecordBatch()
        prev.Retain()

        // Применение pipeline
        for _, f := range pipeline {
            prev, err = f(prev)
        }
        
        // Применение динамического фильтра если есть
        if as.filterFunc != nil {
            prev, err = as.filterFunc(prev)
        }
    }
}
```

**Преимущества:**
- ✅ Неблокирующая проверка
- ✅ Быстрое применение после готовности
- ✅ Чистая асинхронность

**Недостатки:**
- ❌ Сложнее реализация
- ❌ Channel на горячем пути
- ❌ Нужно обновлять pipeline для каждого файла

---

### Вариант 4: Гибридный (проверка между батчами)

**Идея:** Проверять готовность фильтра после обработки каждого RecordBatch.

```go
// Добавить в arrowScan
type arrowScan struct {
    // ...
    filterProvider     FilterProvider
    filterCheckTimeout time.Duration
    filterApplied      bool
    filterMu           sync.RWMutex
    filterFunc         recProcessFn
    batchesProcessed   int
    checkInterval      int // проверять каждые N батчей
}

func (as *arrowScan) processRecords(...) error {
    for recRdr.Next() {
        prev = recRdr.RecordBatch()
        prev.Retain()

        // Применение pipeline
        for _, f := range pipeline {
            prev, err = f(prev)
        }

        // Применение динамического фильтра если есть
        if as.filterFunc != nil {
            prev, err = as.filterFunc(prev)
        }

        // Отправка записи
        out <- enumeratedRecord{...}

        // Проверка готовности фильтра (периодически)
        as.batchesProcessed++
        if !as.filterApplied && as.filterProvider != nil && 
           as.batchesProcessed % as.checkInterval == 0 {
            as.filterMu.Lock()
            if !as.filterApplied {
                waitCtx, cancel := context.WithTimeout(ctx, as.filterCheckTimeout)
                if as.filterProvider.WaitForFilters(waitCtx, as.filterCheckTimeout) {
                    additionalFilter := as.filterProvider.GetFilter()
                    if additionalFilter != nil {
                        as.filterFunc = as.buildFilterFunc(ctx, additionalFilter)
                    }
                    as.filterApplied = true
                }
                cancel()
            }
            as.filterMu.Unlock()
        }
    }
}
```

**Преимущества:**
- ✅ Гибкая настройка частоты проверки
- ✅ Не требует channel
- ✅ Применяется к последующим батчам

**Недостатки:**
- ❌ Проверка на горячем пути
- ❌ Мьютекс может блокировать обработку

---

## Рекомендуемая реализация

### Вариант 2 + 4 (гибридный)

**Проверка между файлами + проверка после N батчей**

```go
// В arrowScan
type arrowScan struct {
    // ...
    filterProvider     FilterProvider
    filterCheckTimeout time.Duration
    filterApplied      bool
    filterMu           sync.RWMutex
    filterFunc         recProcessFn
    batchesProcessed   int64
    checkInterval      int64 // проверять каждые N батчей (по умолчанию 100)
}

// В recordsFromTask() - проверка между файлами
func (as *arrowScan) recordsFromTask(...) error {
    // ...
    
    // Проверка перед началом обработки файла
    as.checkAndUpdateFilter(ctx)
    
    // ...
}

// В processRecords() - проверка между батчами
func (as *arrowScan) processRecords(...) error {
    for recRdr.Next() {
        prev = recRdr.RecordBatch()
        prev.Retain()

        // Применение pipeline
        for _, f := range pipeline {
            prev, err = f(prev)
        }

        // Применение динамического фильтра если есть
        if as.filterFunc != nil {
            prev, err = as.filterFunc(prev)
        }

        out <- enumeratedRecord{...}

        // Периодическая проверка
        as.batchesProcessed++
        if !as.filterApplied && as.filterProvider != nil &&
           as.batchesProcessed % as.checkInterval == 0 {
            as.checkAndUpdateFilter(ctx)
        }
    }
}

// Общий метод проверки
func (as *arrowScan) checkAndUpdateFilter(ctx context.Context) {
    if !as.filterApplied && as.filterProvider != nil {
        as.filterMu.Lock()
        defer as.filterMu.Unlock()
        
        if !as.filterApplied {
            waitCtx, cancel := context.WithTimeout(ctx, as.filterCheckTimeout)
            defer cancel()
            
            if as.filterProvider.WaitForFilters(waitCtx, as.filterCheckTimeout) {
                additionalFilter := as.filterProvider.GetFilter()
                if additionalFilter != nil && !additionalFilter.Equals(iceberg.AlwaysTrue{}) {
                    as.filterFunc = as.buildFilterFunc(ctx, additionalFilter)
                }
                as.filterApplied = true
            }
        }
    }
}

// Построение filterFunc
func (as *arrowScan) buildFilterFunc(ctx context.Context, filter iceberg.BooleanExpression) recProcessFn {
    return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
        defer r.Release()
        
        input := compute.NewDatumWithoutOwning(r)
        mask, err := exprs.ExecuteScalarExpression(ctx, r.Schema(), filter, input)
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

## Необходимые изменения

### 1. table/arrow_scanner.go

**Добавить поля в arrowScan:**
```go
type arrowScan struct {
    // ... существующие поля ...
    
    // Lazy dynamic filter
    filterProvider     FilterProvider
    filterCheckTimeout time.Duration
    filterApplied      bool
    filterMu           sync.RWMutex
    filterFunc         recProcessFn
    batchesProcessed   int64
    checkInterval      int64
}
```

**Добавить ScanOption:**
```go
// В table/table.go
func WithLazyDynamicFilter(provider FilterProvider, timeout, checkInterval time.Duration) ScanOption {
    return func(scan *Scan) {
        scan.lazyFilterProvider = provider
        scan.lazyFilterTimeout = timeout
        scan.lazyFilterCheckInterval = checkInterval
    }
}
```

**Интеграция в processRecords():**
```go
// Периодическая проверка и применение фильтра
```

### 2. table/scanner.go

**Добавить поля в Scan:**
```go
type Scan struct {
    // ...
    lazyFilterProvider     FilterProvider
    lazyFilterTimeout      time.Duration
    lazyFilterCheckInterval int64
}
```

### 3. dynamicfilter/scan_filter_applier.go

**Добавить метод для проверки без блокировки:**
```go
// TryGetFilter проверяет готовность фильтра без ожидания
func (a *ScanFilterApplier) TryGetFilter() (iceberg.BooleanExpression, bool) {
    a.mu.RLock()
    defer a.mu.RUnlock()
    
    if !a.filtersWaited {
        return iceberg.AlwaysTrue{}, false
    }
    
    // Построение фильтра из уже полученных значений
    // ...
}
```

---

## Сравнение подходов

| Подход | Накладные расходы | Задержка применения | Сложность | Рекомендация |
|--------|------------------|---------------------|-----------|--------------|
| Проверка перед каждой записью | Высокие | Минимальная | Средняя | ❌ |
| **Проверка между файлами** | Низкие | До следующего файла | Низкая | ⭐⭐⭐⭐ |
| Асинхронное уведомление | Средние | Минимальная | Высокая | ⭐⭐⭐ |
| **Проверка между батчами** | Средние | До N батчей | Средняя | ⭐⭐⭐⭐ |
| **Гибридный (2+4)** | Средние | Гибкая | Средняя | ⭐⭐⭐⭐⭐ |

---

## Пример использования

```go
// Lazy применение фильтров
applier := dynamicfilter.NewScanFilterApplier(...)

scan := tbl.Scan(
    table.WithLazyDynamicFilter(
        applier,
        5*time.Second,    // timeout на проверку
        100,              // проверять каждые 100 батчей
    ),
    table.WithRowFilter(initialFilter), // начальный фильтр
)

// Сканирование начнется сразу с initialFilter
// Динамический фильтр применится когда будет готов
_, records, _ := scan.ToArrowRecords(ctx)
```

---

## Выводы

**Рекомендуемый подход:** Гибридный (проверка между файлами + между батчами)

**Преимущества:**
- Сканирование начинается немедленно
- Фильтр применяется как только готов
- Гибкая настройка частоты проверки
- Разумные накладные расходы

**Время реализации:** 6-8 часов
