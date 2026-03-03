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

// Package types предоставляет общие типы для динамических фильтров
package types

import (
	"fmt"
	"sort"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// Constants for filter generation
const (
	// DefaultInPredicateLimit - максимальное количество значений для IN предиката
	DefaultInPredicateLimit = 200

	// DefaultBetweenRangeCount - максимальное количество диапазонов для BETWEEN предиката
	DefaultBetweenRangeCount = 50

	// DefaultBatchSize - размер пакета для отправки значений
	DefaultBatchSize = 10000

	// DefaultWaitTimeout - таймаут ожидания фильтра по умолчанию
	DefaultWaitTimeout = 30 * time.Second

	// DefaultQueryTimeout - таймаут жизни запроса
	DefaultQueryTimeout = 5 * time.Minute
)

// FilterType определяет тип динамического фильтра
type FilterType int

const (
	FilterTypeUnspecified FilterType = iota
	FilterTypeIN                     // IN предикат
	FilterTypeBETWEEN                // BETWEEN предикаты (OR)
)

func (f FilterType) String() string {
	switch f {
	case FilterTypeIN:
		return "IN"
	case FilterTypeBETWEEN:
		return "BETWEEN"
	default:
		return "UNSPECIFIED"
	}
}

// SessionStatus определяет статус сессии запроса
type SessionStatus int

const (
	SessionStatusUnspecified SessionStatus = iota
	SessionStatusActive
	SessionStatusCollecting
	SessionStatusReady
	SessionStatusCompleted
	SessionStatusExpired
	SessionStatusError
)

func (s SessionStatus) String() string {
	switch s {
	case SessionStatusActive:
		return "ACTIVE"
	case SessionStatusCollecting:
		return "COLLECTING"
	case SessionStatusReady:
		return "READY"
	case SessionStatusCompleted:
		return "COMPLETED"
	case SessionStatusExpired:
		return "EXPIRED"
	case SessionStatusError:
		return "ERROR"
	default:
		return "UNSPECIFIED"
	}
}

// FieldMapping определяет маппинг поля из source таблицы в target таблицу
type FieldMapping struct {
	TargetAlias   string       // Алиас target таблицы
	TargetField   string       // Имя поля target таблицы
	SourceAlias   string       // Алиас source таблицы
	SourceField   string       // Имя поля source таблицы
	SourceFieldID int          // Iceberg field ID source
	TargetFieldID int          // Iceberg field ID target
	FieldType     iceberg.Type // Тип поля
}

// ValueRange представляет диапазон значений для BETWEEN предиката
type ValueRange struct {
	Lower          iceberg.Literal // Нижняя граница
	Upper          iceberg.Literal // Верхняя граница
	LowerInclusive bool            // Нижняя граница включительна
	UpperInclusive bool            // Верхняя граница включительна
}

// DynamicFilter представляет динамический фильтр, построенный на основе собранных значений
type DynamicFilter struct {
	FieldName        string        // Имя поля target
	FieldType        iceberg.Type  // Тип поля
	FilterType       FilterType    // Тип фильтра
	Values           []iceberg.Literal // Значения для IN фильтра
	Ranges           []ValueRange  // Диапазоны для BETWEEN фильтра
	TotalValues      int64         // Общее количество собранных значений
	BuildTimestampMs int64         // Время построения фильтра (ms since epoch)
}

// BuildExpression строит BooleanExpression из фильтра для указанного поля
func (f *DynamicFilter) BuildExpression(fieldRef iceberg.UnboundTerm) iceberg.BooleanExpression {
	if fieldRef == nil {
		return iceberg.AlwaysTrue{}
	}

	switch f.FilterType {
	case FilterTypeIN:
		if len(f.Values) == 0 {
			return iceberg.AlwaysFalse{}
		}

		// Строим OR из EqualTo предикатов используя рефлектподобный подход
		return buildInExpression(fieldRef, f.Values)

	case FilterTypeBETWEEN:
		if len(f.Ranges) == 0 {
			return iceberg.AlwaysFalse{}
		}

		predicates := make([]iceberg.BooleanExpression, 0, len(f.Ranges))
		for _, r := range f.Ranges {
			pred := buildBetweenExpression(fieldRef, r)
			predicates = append(predicates, pred)
		}

		if len(predicates) == 1 {
			return predicates[0]
		}
		return iceberg.NewOr(predicates[0], predicates[1], predicates[2:]...)

	default:
		return iceberg.AlwaysTrue{}
	}
}

// buildInExpression строит OR выражение из EqualTo предикатов
func buildInExpression(fieldRef iceberg.UnboundTerm, values []iceberg.Literal) iceberg.BooleanExpression {
	predicates := make([]iceberg.BooleanExpression, 0, len(values))
	
	for _, v := range values {
		pred := buildEqualToExpression(fieldRef, v)
		if pred != nil {
			predicates = append(predicates, pred)
		}
	}
	
	if len(predicates) == 0 {
		return iceberg.AlwaysFalse{}
	}
	if len(predicates) == 1 {
		return predicates[0]
	}
	
	return iceberg.NewOr(predicates[0], predicates[1], predicates[2:]...)
}

// buildEqualToExpression строит EqualTo предикат используя тип значения
func buildEqualToExpression(fieldRef iceberg.UnboundTerm, value iceberg.Literal) iceberg.BooleanExpression {
	val := value.Any()
	if val == nil {
		return nil
	}
	
	// Используем тип значения для выбора правильной функции
	switch v := val.(type) {
	case bool:
		return iceberg.EqualTo(fieldRef, v)
	case int32:
		return iceberg.EqualTo(fieldRef, v)
	case int64:
		return iceberg.EqualTo(fieldRef, v)
	case float32:
		return iceberg.EqualTo(fieldRef, v)
	case float64:
		return iceberg.EqualTo(fieldRef, v)
	case string:
		return iceberg.EqualTo(fieldRef, v)
	case []byte:
		return iceberg.EqualTo(fieldRef, v)
	case iceberg.Date:
		return iceberg.EqualTo(fieldRef, v)
	case iceberg.Time:
		return iceberg.EqualTo(fieldRef, v)
	case iceberg.Timestamp:
		return iceberg.EqualTo(fieldRef, v)
	case iceberg.Decimal:
		return iceberg.EqualTo(fieldRef, v)
	case uuid.UUID: // UUID
		return iceberg.EqualTo(fieldRef, v)
	default:
		return nil
	}
}

// buildBetweenExpression строит BETWEEN предикат (AND из GreaterThanEqual и LessThanEqual)
func buildBetweenExpression(fieldRef iceberg.UnboundTerm, r ValueRange) iceberg.BooleanExpression {
	lowerVal := r.Lower.Any()
	upperVal := r.Upper.Any()
	
	if lowerVal == nil || upperVal == nil {
		return nil
	}
	
	var lowerPred, upperPred iceberg.BooleanExpression
	
	// Строим GreaterThanEqual / GreaterThan
	switch v := lowerVal.(type) {
	case bool:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case int32:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case int64:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case float32:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case float64:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case string:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case iceberg.Date:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case iceberg.Time:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case iceberg.Timestamp:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	case iceberg.Decimal:
		if r.LowerInclusive {
			lowerPred = iceberg.GreaterThanEqual(fieldRef, v)
		} else {
			lowerPred = iceberg.GreaterThan(fieldRef, v)
		}
	}
	
	// Строим LessThanEqual / LessThan
	switch v := upperVal.(type) {
	case bool:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case int32:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case int64:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case float32:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case float64:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case string:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case iceberg.Date:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case iceberg.Time:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case iceberg.Timestamp:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	case iceberg.Decimal:
		if r.UpperInclusive {
			upperPred = iceberg.LessThanEqual(fieldRef, v)
		} else {
			upperPred = iceberg.LessThan(fieldRef, v)
		}
	}
	
	if lowerPred == nil || upperPred == nil {
		return nil
	}
	
	return iceberg.NewAnd(lowerPred, upperPred)
}

// BuildFilter создает DynamicFilter из набора значений
func BuildFilter(values []iceberg.Literal, fieldType iceberg.Type, inLimit, maxRanges int) *DynamicFilter {
	if len(values) == 0 {
		return &DynamicFilter{
			FieldType:        fieldType,
			FilterType:       FilterTypeUnspecified,
			BuildTimestampMs: time.Now().UnixMilli(),
		}
	}

	// Дедупликация значений
	uniqueValues := deduplicateLiterals(values)

	now := time.Now().UnixMilli()

	// Если значений мало, используем IN предикат
	if len(uniqueValues) <= inLimit {
		return &DynamicFilter{
			FieldType:        fieldType,
			FilterType:       FilterTypeIN,
			Values:           uniqueValues,
			TotalValues:      int64(len(values)),
			BuildTimestampMs: now,
		}
	}

	// Иначе строим диапазоны
	ranges := buildRanges(uniqueValues, maxRanges)
	return &DynamicFilter{
		FieldType:        fieldType,
		FilterType:       FilterTypeBETWEEN,
		Ranges:           ranges,
		TotalValues:      int64(len(values)),
		BuildTimestampMs: now,
	}
}

// deduplicateLiterals удаляет дубликаты из слайса литералов
func deduplicateLiterals(values []iceberg.Literal) []iceberg.Literal {
	seen := make(map[string]struct{}, len(values))
	result := make([]iceberg.Literal, 0, len(values))

	for _, v := range values {
		key := literalKey(v)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, v)
		}
	}

	return result
}

// literalKey создает строковый ключ для литерала
func literalKey(l iceberg.Literal) string {
	if l == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%T:%v", l.Any(), l.Any())
}

// buildRanges строит диапазоны из отсортированных значений
func buildRanges(values []iceberg.Literal, maxRanges int) []ValueRange {
	if len(values) == 0 {
		return nil
	}

	// Сортировка значений
	sorted := make([]iceberg.Literal, len(values))
	copy(sorted, values)
	sortLiterals(sorted)

	// Группировка в кластеры
	clusters := findClusters(sorted)

	// Ограничение количества диапазонов
	if len(clusters) > maxRanges {
		clusters = mergeClusters(clusters, maxRanges)
	}

	// Конвертация кластеров в диапазоны
	ranges := make([]ValueRange, 0, len(clusters))
	for _, cluster := range clusters {
		if len(cluster) == 0 {
			continue
		}

		ranges = append(ranges, ValueRange{
			Lower:          cluster[0],
			Upper:          cluster[len(cluster)-1],
			LowerInclusive: true,
			UpperInclusive: true,
		})
	}

	return ranges
}

// sortLiterals сортирует литералы
func sortLiterals(literals []iceberg.Literal) {
	sort.Slice(literals, func(i, j int) bool {
		return compareLiterals(literals[i], literals[j]) < 0
	})
}

// compareLiterals сравнивает два литерала
func compareLiterals(a, b iceberg.Literal) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	av := a.Any()
	bv := b.Any()

	// Сравнение для примитивных типов
	switch av := av.(type) {
	case int32:
		bv := bv.(int32)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int64:
		bv := bv.(int64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		bv := bv.(float32)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv := bv.(float64)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv := bv.(string)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// findClusters группирует отсортированные значения в кластеры на основе "разрывов"
func findClusters(values []iceberg.Literal) [][]iceberg.Literal {
	if len(values) == 0 {
		return nil
	}

	clusters := make([][]iceberg.Literal, 0)
	currentCluster := []iceberg.Literal{values[0]}

	for i := 1; i < len(values); i++ {
		prev := values[i-1]
		curr := values[i]

		// Если разрыв между значениями большой, начинаем новый кластер
		if isGap(prev, curr) {
			clusters = append(clusters, currentCluster)
			currentCluster = []iceberg.Literal{curr}
		} else {
			currentCluster = append(currentCluster, curr)
		}
	}

	if len(currentCluster) > 0 {
		clusters = append(clusters, currentCluster)
	}

	return clusters
}

// isGap определяет, есть ли "разрыв" между двумя значениями
func isGap(a, b iceberg.Literal) bool {
	av := a.Any()
	bv := b.Any()

	switch v := av.(type) {
	case int32:
		bv := bv.(int32)
		gap := int64(bv) - int64(v)
		return gap > 100
	case int64:
		bv := bv.(int64)
		gap := bv - v
		return gap > 100
	case float32:
		bv := bv.(float32)
		avg := (v + bv) / 2
		if avg == 0 {
			return bv-v > 0.001
		}
		return (bv - v) / avg > 0.1
	case float64:
		bv := bv.(float64)
		avg := (v + bv) / 2
		if avg == 0 {
			return bv-v > 0.001
		}
		return (bv - v) / avg > 0.1
	case string:
		// Для строк - новый кластер если разные префиксы
		bv := bv.(string)
		return getPrefix(v) != getPrefix(bv)
	default:
		return false
	}
}

func getPrefix(s string) string {
	if len(s) <= 3 {
		return s
	}
	return s[:3]
}

// mergeClusters объединяет кластеры для ограничения их количества
func mergeClusters(clusters [][]iceberg.Literal, maxCount int) [][]iceberg.Literal {
	if len(clusters) <= maxCount {
		return clusters
	}

	// Слияние соседних кластеров пока не достигнем maxCount
	for len(clusters) > maxCount {
		// Находим пару кластеров с минимальным расстоянием между ними
		minGap := int64(-1)
		minIdx := 0

		for i := 0; i < len(clusters)-1; i++ {
			gap := clusterGap(clusters[i], clusters[i+1])
			if minGap < 0 || gap < minGap {
				minGap = gap
				minIdx = i
			}
		}

		// Объединяем кластеры minIdx и minIdx+1
		merged := append(clusters[minIdx], clusters[minIdx+1]...)
		clusters = append(clusters[:minIdx], append([][]iceberg.Literal{merged}, clusters[minIdx+2:]...)...)
	}

	return clusters
}

// clusterGap вычисляет "расстояние" между двумя кластерами
func clusterGap(a, b []iceberg.Literal) int64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	last := a[len(a)-1]
	first := b[0]

	lv := last.Any()
	fv := first.Any()

	switch lv := lv.(type) {
	case int32:
		fv := fv.(int32)
		return int64(fv) - int64(lv)
	case int64:
		fv := fv.(int64)
		return fv - lv
	case float64:
		fv := fv.(float64)
		return int64((fv - lv) * 1000)
	default:
		return 1
	}
}
