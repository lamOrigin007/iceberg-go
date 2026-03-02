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

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter/types"
	pb "github.com/apache/iceberg-go/dynamicfilter/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ошибки координатора
var (
	ErrQueryNotFound      = errors.New("query not found")
	ErrSessionExpired     = errors.New("session expired")
	ErrDuplicateSegment   = errors.New("duplicate segment registration")
	ErrValuesOverflow     = errors.New("values overflow")
	ErrFilterNotReady     = errors.New("filter not ready")
	ErrInvalidMapping     = errors.New("invalid field mapping")
)

// Config конфигурация координатора
type Config struct {
	// ListenAddr адрес для прослушивания (например, ":9999")
	ListenAddr string

	// QueryTimeout таймаут сессии запроса
	QueryTimeout time.Duration

	// MaxValuesPerField максимум значений на поле
	MaxValuesPerField int

	// MaxRangesPerFilter максимум диапазонов в BETWEEN фильтре
	MaxRangesPerFilter int

	// InPredicateLimit порог для IN vs BETWEEN предиката
	InPredicateLimit int

	// CleanupInterval интервал очистки старых сессий
	CleanupInterval time.Duration

	// EnableCompression включение сжатия GRPC
	EnableCompression bool
}

func (c *Config) validate() {
	if c.QueryTimeout == 0 {
		c.QueryTimeout = types.DefaultQueryTimeout
	}
	if c.MaxValuesPerField == 0 {
		c.MaxValuesPerField = 1000000
	}
	if c.MaxRangesPerFilter == 0 {
		c.MaxRangesPerFilter = types.DefaultBetweenRangeCount
	}
	if c.InPredicateLimit == 0 {
		c.InPredicateLimit = types.DefaultInPredicateLimit
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = 1 * time.Minute
	}
}

// Coordinator GRPC сервер для управления динамическими фильтрами
type Coordinator struct {
	pb.UnimplementedDynamicFilterServiceServer

	config Config

	mu           sync.RWMutex
	sessions     map[string]*QuerySession
	segmentIndex map[string]string // session_id -> query_id

	listener io.Closer
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// QuerySession сессия запроса
type QuerySession struct {
	mu sync.RWMutex

	QueryID        string
	SessionID      string // для single-segment queries
	Status         types.SessionStatus
	StartTime      time.Time
	LastAccessTime time.Time
	Timeout        time.Duration

	// Маппинги полей
	Mappings []types.FieldMapping

	// Source состояние: source_alias -> field_name -> SourceFieldState
	Sources map[string]map[string]*SourceFieldState

	// Target состояние: target_alias -> field_name -> TargetFieldState
	Targets map[string]map[string]*TargetFieldState

	// Зарегистрированные сегменты
	Segments map[string]*SegmentInfo // session_id -> SegmentInfo

	// Счётчики
	TotalSegments      int
	CompletedSources   int
	ReadyTargets       int
}

// SourceFieldState состояние сбора значений для поля source таблицы
type SourceFieldState struct {
	Alias       string
	FieldName   string
	FieldType   iceberg.Type
	Values      []iceberg.Literal
	IsComplete  bool
	SegmentCount int // количество сегментов, завершивших сбор этого поля
}

// TargetFieldState состояние фильтра для поля target таблицы
type TargetFieldState struct {
	Alias       string
	FieldName   string
	FieldType   iceberg.Type
	SourceAlias string
	SourceFieldName string
	Filter      *types.DynamicFilter
	IsReady     bool
	Waiters     []chan<- *types.DynamicFilter
}

// SegmentInfo информация о сегменте
type SegmentInfo struct {
	SessionID     string
	Host          string
	Port          int32
	SourceAliases []string
	TargetAliases []string
	RegisteredAt  time.Time
}

// New создает новый Coordinator
func New(config Config) *Coordinator {
	config.validate()

	return &Coordinator{
		config:       config,
		sessions:     make(map[string]*QuerySession),
		segmentIndex: make(map[string]string),
		stopCh:       make(chan struct{}),
	}
}

// Serve запускает GRPC сервер
func (c *Coordinator) Serve(listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	c.listener = lis

	opts := []grpc.ServerOption{}
	if c.config.EnableCompression {
		opts = append(opts,
			grpc.RPCCompressor(grpc.NewGZIPCompressor()),
			grpc.RPCDecompressor(grpc.NewGZIPDecompressor()))
	}

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterDynamicFilterServiceServer(grpcServer, c)

	// Запуск очистки старых сессий
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.cleanupLoop()
	}()

	return grpcServer.Serve(lis)
}

// ServeWithServer запускает сервер с предоставленным grpc.Server
func (c *Coordinator) ServeWithServer(server *grpc.Server, lis net.Listener) error {
	c.listener = lis

	// Запуск очистки старых сессий
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.cleanupLoop()
	}()

	pb.RegisterDynamicFilterServiceServer(server, c)
	return server.Serve(lis)
}

// Stop останавливает сервер
func (c *Coordinator) Stop() {
	close(c.stopCh)

	if c.listener != nil {
		c.listener.Close()
	}

	c.wg.Wait()

	// Очистка всех сессий
	c.mu.Lock()
	defer c.mu.Unlock()

	for queryID := range c.sessions {
		c.cleanupSession(queryID)
	}
}

// StartQuery инициализирует сессию для запроса
func (c *Coordinator) StartQuery(ctx context.Context, req *pb.StartQueryRequest) (*pb.StartQueryResponse, error) {
	if req.QueryId == "" {
		return &pb.StartQueryResponse{
			Success: false,
			ErrorMessage: "query_id is required",
		}, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Проверка существующей сессии
	if existing, ok := c.sessions[req.QueryId]; ok {
		if existing.Status != types.SessionStatusCompleted &&
		   existing.Status != types.SessionStatusExpired {
			// Сессия уже активна
			if _, ok := existing.Segments[req.SessionId]; ok {
				return &pb.StartQueryResponse{
					Success: false,
					ErrorMessage: fmt.Sprintf("duplicate session_id: %s", req.SessionId),
				}, nil
			}
		} else {
			// Старая сессия, очищаем
			c.cleanupSession(req.QueryId)
		}
	}

	// Преобразование маппингов
	mappings := make([]types.FieldMapping, 0, len(req.Mappings))
	for _, m := range req.Mappings {
		fieldType, err := parseFieldType(m.FieldType)
		if err != nil {
			return &pb.StartQueryResponse{
				Success: false,
				ErrorMessage: fmt.Sprintf("invalid field type %q: %v", m.FieldType, err),
			}, nil
		}

		mappings = append(mappings, types.FieldMapping{
			TargetAlias:   m.TargetAlias,
			TargetField:   m.TargetField,
			SourceAlias:   m.SourceAlias,
			SourceField:   m.SourceField,
			SourceFieldID: int(m.SourceFieldId),
			TargetFieldID: int(m.TargetFieldId),
			FieldType:     fieldType,
		})
	}

	// Создание сессии
	timeout := c.config.QueryTimeout
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}

	session := &QuerySession{
		QueryID:        req.QueryId,
		SessionID:      req.SessionId,
		Status:         types.SessionStatusActive,
		StartTime:      time.Now(),
		LastAccessTime: time.Now(),
		Timeout:        timeout,
		Mappings:       mappings,
		Sources:        make(map[string]map[string]*SourceFieldState),
		Targets:        make(map[string]map[string]*TargetFieldState),
		Segments:       make(map[string]*SegmentInfo),
		TotalSegments:  int(req.TotalSegments),
	}

	// Инициализация source/target state из маппингов
	for _, m := range mappings {
		// Source
		if _, ok := session.Sources[m.SourceAlias]; !ok {
			session.Sources[m.SourceAlias] = make(map[string]*SourceFieldState)
		}
		if _, ok := session.Sources[m.SourceAlias][m.SourceField]; !ok {
			session.Sources[m.SourceAlias][m.SourceField] = &SourceFieldState{
				Alias:     m.SourceAlias,
				FieldName: m.SourceField,
				FieldType: m.FieldType,
				Values:    make([]iceberg.Literal, 0),
			}
		}

		// Target
		if _, ok := session.Targets[m.TargetAlias]; !ok {
			session.Targets[m.TargetAlias] = make(map[string]*TargetFieldState)
		}
		if _, ok := session.Targets[m.TargetAlias][m.TargetField]; !ok {
			session.Targets[m.TargetAlias][m.TargetField] = &TargetFieldState{
				Alias:         m.TargetAlias,
				FieldName:     m.TargetField,
				FieldType:     m.FieldType,
				SourceAlias:   m.SourceAlias,
				SourceFieldName: m.SourceField,
				IsReady:       false,
				Waiters:       make([]chan<- *types.DynamicFilter, 0),
			}
		}
	}

	c.sessions[req.QueryId] = session
	c.segmentIndex[req.SessionId] = req.QueryId

	return &pb.StartQueryResponse{
		Success: true,
	}, nil
}

// StopQuery завершает сессию запроса
func (c *Coordinator) StopQuery(ctx context.Context, req *pb.StopQueryRequest) (*pb.StopQueryResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	session, ok := c.sessions[req.QueryId]
	if !ok {
		return &pb.StopQueryResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("query not found: %s", req.QueryId),
		}, nil
	}

	// Уведомление ожидающих target
	session.mu.Lock()
	for _, targetFields := range session.Targets {
		for _, tf := range targetFields {
			if !tf.IsReady {
				// Создаем пустой фильтр для разблокировки ожидающих
				tf.Filter = &types.DynamicFilter{
					FieldName:        tf.FieldName,
					FieldType:        tf.FieldType,
					FilterType:       types.FilterTypeUnspecified,
					BuildTimestampMs: time.Now().UnixMilli(),
				}
				tf.IsReady = true

				for _, waiter := range tf.Waiters {
					select {
					case waiter <- tf.Filter:
					default:
					}
				}
				tf.Waiters = nil
			}
		}
	}
	session.Status = types.SessionStatusCompleted
	session.mu.Unlock()

	c.cleanupSession(req.QueryId)

	return &pb.StopQueryResponse{
		Success: true,
	}, nil
}

// QueryStatus возвращает статус запроса
func (c *Coordinator) QueryStatus(ctx context.Context, req *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	c.mu.RLock()
	session, ok := c.sessions[req.QueryId]
	c.mu.RUnlock()

	if !ok {
		return &pb.QueryStatusResponse{
			Status: pb.SessionStatus_SESSION_STATUS_UNSPECIFIED,
		}, status.Error(codes.NotFound, "query not found")
	}

	session.mu.RLock()
	defer session.mu.RUnlock()

	return &pb.QueryStatusResponse{
		QueryId:            session.QueryID,
		Status:             pb.SessionStatus(session.Status),
		RegisteredSegments: int32(len(session.Segments)),
		CompletedSources:   int32(session.CompletedSources),
		ReadyTargets:       int32(session.ReadyTargets),
	}, nil
}

// CollectValues принимает поток значений от source таблиц
func (c *Coordinator) CollectValues(stream pb.DynamicFilterService_CollectValuesServer) error {
	var currentQueryID, currentSourceAlias, currentFieldName string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.CollectValueResponse{
				Success: true,
			})
		}
		if err != nil {
			return err
		}

		currentQueryID = req.QueryId
		currentSourceAlias = req.SourceAlias
		currentFieldName = req.FieldName

		// Получение сессии
		c.mu.RLock()
		session, ok := c.sessions[currentQueryID]
		c.mu.RUnlock()

		if !ok {
			return status.Error(codes.NotFound, fmt.Sprintf("query not found: %s", currentQueryID))
		}

		session.mu.Lock()

		// Проверка существования source field
		sourceFields, ok := session.Sources[currentSourceAlias]
		if !ok {
			session.mu.Unlock()
			return status.Error(codes.InvalidArgument,
				fmt.Sprintf("unknown source alias: %s", currentSourceAlias))
		}

		sourceField, ok := sourceFields[currentFieldName]
		if !ok {
			session.mu.Unlock()
			return status.Error(codes.InvalidArgument,
				fmt.Sprintf("unknown field name: %s for source %s", currentFieldName, currentSourceAlias))
		}

		// Проверка лимита
		if len(sourceField.Values)+len(req.Values) > c.config.MaxValuesPerField {
			session.mu.Unlock()
			return status.Error(codes.ResourceExhausted,
				fmt.Sprintf("values overflow: max %d values per field", c.config.MaxValuesPerField))
		}

		// Преобразование и добавление значений
		for _, lit := range req.Values {
			val, err := protoLiteralToIceberg(lit, sourceField.FieldType)
			if err != nil {
				session.mu.Unlock()
				return status.Error(codes.InvalidArgument,
					fmt.Sprintf("invalid literal value: %v", err))
			}
			sourceField.Values = append(sourceField.Values, val)
		}

		session.LastAccessTime = time.Now()

		// Если это финальный пакет
		if req.IsFinal && !sourceField.IsComplete {
			sourceField.IsComplete = true
			sourceField.SegmentCount++
			session.CompletedSources++

			// Проверка готовности фильтра для target
			c.checkAndBuildFilter(session, currentSourceAlias, currentFieldName)
		}

		session.mu.Unlock()
	}
}

// checkAndBuildFilter проверяет готов ли фильтр и строит его
func (c *Coordinator) checkAndBuildFilter(session *QuerySession, sourceAlias string, sourceFieldName string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	// Поиск target полей для этого source
	for _, targetFields := range session.Targets {
		for _, tf := range targetFields {
			if tf.SourceAlias != sourceAlias || tf.SourceFieldName != sourceFieldName {
				continue
			}
			if tf.IsReady {
				continue
			}

			// Проверка: все ли сегменты завершили сбор этого source поля
			sourceField := session.Sources[sourceAlias][sourceFieldName]
			if sourceField.SegmentCount < session.TotalSegments {
				continue // ещё не все сегменты завершили
			}

			// Построение фильтра
			filter := types.BuildFilter(
				sourceField.Values,
				tf.FieldType,
				c.config.InPredicateLimit,
				c.config.MaxRangesPerFilter,
			)
			
			// Устанавливаем имя поля в фильтре
			filter.FieldName = tf.FieldName

			tf.Filter = filter
			tf.IsReady = true
			session.ReadyTargets++

			// Уведомление ожидающих
			for _, waiter := range tf.Waiters {
				select {
				case waiter <- filter:
				default:
				}
			}
			tf.Waiters = nil

			// Обновление статуса сессии
			if session.ReadyTargets >= len(session.Targets)*len(targetFields) {
				session.Status = types.SessionStatusReady
			}
		}
	}
}

// GetFilter возвращает фильтр для target поля
func (c *Coordinator) GetFilter(ctx context.Context, req *pb.GetFilterRequest) (*pb.GetFilterResponse, error) {
	c.mu.RLock()
	session, ok := c.sessions[req.QueryId]
	c.mu.RUnlock()

	if !ok {
		return &pb.GetFilterResponse{
			IsReady: false,
			ErrorMessage: fmt.Sprintf("query not found: %s", req.QueryId),
		}, nil
	}

	session.mu.RLock()
	defer session.mu.RUnlock()

	targetFields, ok := session.Targets[req.TargetAlias]
	if !ok {
		return &pb.GetFilterResponse{
			IsReady: false,
			ErrorMessage: fmt.Sprintf("unknown target alias: %s", req.TargetAlias),
		}, nil
	}

	tf, ok := targetFields[req.FieldName]
	if !ok {
		return &pb.GetFilterResponse{
			IsReady: false,
			ErrorMessage: fmt.Sprintf("unknown field name: %s", req.FieldName),
		}, nil
	}

	if !tf.IsReady {
		return &pb.GetFilterResponse{
			IsReady: false,
		}, nil
	}

	return &pb.GetFilterResponse{
		IsReady: true,
		Filter:  icebergFilterToProto(tf.Filter),
	}, nil
}

// WaitForFilter блокируется до готовности фильтра
func (c *Coordinator) WaitForFilter(ctx context.Context, req *pb.WaitForFilterRequest) (*pb.WaitForFilterResponse, error) {
	c.mu.RLock()
	session, ok := c.sessions[req.QueryId]
	c.mu.RUnlock()

	if !ok {
		return &pb.WaitForFilterResponse{
			IsReady: false,
			TimedOut: true,
			ErrorMessage: fmt.Sprintf("query not found: %s", req.QueryId),
		}, nil
	}

	session.mu.Lock()

	targetFields, ok := session.Targets[req.TargetAlias]
	if !ok {
		session.mu.Unlock()
		return &pb.WaitForFilterResponse{
			IsReady: false,
			TimedOut: false,
			ErrorMessage: fmt.Sprintf("unknown target alias: %s", req.TargetAlias),
		}, nil
	}

	tf, ok := targetFields[req.FieldName]
	if !ok {
		session.mu.Unlock()
		return &pb.WaitForFilterResponse{
			IsReady: false,
			TimedOut: false,
			ErrorMessage: fmt.Sprintf("unknown field name: %s", req.FieldName),
		}, nil
	}

	// Если фильтр уже готов
	if tf.IsReady {
		filter := tf.Filter
		session.mu.Unlock()
		return &pb.WaitForFilterResponse{
			IsReady: true,
			Filter:  icebergFilterToProto(filter),
		}, nil
	}

	// Подписка на уведомление
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = types.DefaultWaitTimeout
	}

	waitCh := make(chan *types.DynamicFilter, 1)
	tf.Waiters = append(tf.Waiters, waitCh)
	session.mu.Unlock()

	// Ожидание
	select {
	case filter := <-waitCh:
		return &pb.WaitForFilterResponse{
			IsReady: true,
			Filter:  icebergFilterToProto(filter),
		}, nil

	case <-time.After(timeout):
		// Таймаут
		session.mu.Lock()
		// Удаляем waiter из списка
		for i, w := range tf.Waiters {
			if w == waitCh {
				tf.Waiters = append(tf.Waiters[:i], tf.Waiters[i+1:]...)
				break
			}
		}
		session.mu.Unlock()

		return &pb.WaitForFilterResponse{
			IsReady: false,
			TimedOut: true,
		}, nil

	case <-ctx.Done():
		return &pb.WaitForFilterResponse{
			IsReady: false,
			TimedOut: true,
			ErrorMessage: ctx.Err().Error(),
		}, nil
	}
}

// RegisterSegment регистрирует сегмент
func (c *Coordinator) RegisterSegment(ctx context.Context, req *pb.RegisterSegmentRequest) (*pb.RegisterSegmentResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	session, ok := c.sessions[req.QueryId]
	if !ok {
		return &pb.RegisterSegmentResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("query not found: %s", req.QueryId),
		}, nil
	}

	// Проверка дубликата
	if _, ok := session.Segments[req.SessionId]; ok {
		return &pb.RegisterSegmentResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("duplicate segment: %s", req.SessionId),
		}, nil
	}

	session.Segments[req.SessionId] = &SegmentInfo{
		SessionID:     req.SessionId,
		Host:          req.SegmentHost,
		Port:          req.SegmentPort,
		SourceAliases: req.SourceAliases,
		TargetAliases: req.TargetAliases,
		RegisteredAt:  time.Now(),
	}
	c.segmentIndex[req.SessionId] = req.QueryId

	session.LastAccessTime = time.Now()

	return &pb.RegisterSegmentResponse{
		Success: true,
		TotalRegistered: int32(len(session.Segments)),
	}, nil
}

// SignalSourceComplete сигнализирует о завершении сбора source поля
func (c *Coordinator) SignalSourceComplete(ctx context.Context, req *pb.SignalSourceCompleteRequest) (*pb.SignalSourceCompleteResponse, error) {
	c.mu.RLock()
	session, ok := c.sessions[req.QueryId]
	c.mu.RUnlock()

	if !ok {
		return &pb.SignalSourceCompleteResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("query not found: %s", req.QueryId),
		}, nil
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	sourceFields, ok := session.Sources[req.SourceAlias]
	if !ok {
		return &pb.SignalSourceCompleteResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("unknown source alias: %s", req.SourceAlias),
		}, nil
	}

	sourceField, ok := sourceFields[req.FieldName]
	if !ok {
		return &pb.SignalSourceCompleteResponse{
			Success: false,
			ErrorMessage: fmt.Sprintf("unknown field name: %s", req.FieldName),
		}, nil
	}

	if !sourceField.IsComplete {
		sourceField.IsComplete = true
		sourceField.SegmentCount++
		session.CompletedSources++

		// Проверка готовности фильтра
		c.checkAndBuildFilter(session, req.SourceAlias, req.FieldName)
	}

	// Проверка готовности фильтра для target
	filterReady := false
	for _, targetFields := range session.Targets {
		for _, tf := range targetFields {
			if tf.SourceAlias == req.SourceAlias && tf.SourceFieldName == req.FieldName {
				filterReady = filterReady || tf.IsReady
			}
		}
	}

	return &pb.SignalSourceCompleteResponse{
		Success: true,
		FilterReady: filterReady,
	}, nil
}

// cleanupLoop цикл очистки старых сессий
func (c *Coordinator) cleanupLoop() {
	ticker := time.NewTicker(c.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpiredSessions()
		case <-c.stopCh:
			return
		}
	}
}

// cleanupExpiredSessions очищает сессии с истёкшим таймаутом
func (c *Coordinator) cleanupExpiredSessions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for queryID, session := range c.sessions {
		session.mu.RLock()
		expired := now.Sub(session.LastAccessTime) > session.Timeout
		session.mu.RUnlock()

		if expired {
			c.cleanupSession(queryID)
		}
	}
}

// cleanupSession очищает сессию
func (c *Coordinator) cleanupSession(queryID string) {
	session, ok := c.sessions[queryID]
	if !ok {
		return
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	// Уведомление всех ожидающих
	for _, targetFields := range session.Targets {
		for _, tf := range targetFields {
			for _, waiter := range tf.Waiters {
				select {
				case waiter <- nil:
				default:
				}
			}
		}
	}

	session.Status = types.SessionStatusExpired
	delete(c.sessions, queryID)
	delete(c.segmentIndex, session.SessionID)
}

// Вспомогательные функции для конвертации

func parseFieldType(typeName string) (iceberg.Type, error) {
	switch typeName {
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int32", "int":
		return iceberg.PrimitiveTypes.Int32, nil
	case "int64", "long":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float32", "float":
		return iceberg.PrimitiveTypes.Float32, nil
	case "float64", "double":
		return iceberg.PrimitiveTypes.Float64, nil
	case "string":
		return iceberg.PrimitiveTypes.String, nil
	case "binary":
		return iceberg.PrimitiveTypes.Binary, nil
	case "date":
		return iceberg.PrimitiveTypes.Date, nil
	case "time":
		return iceberg.PrimitiveTypes.Time, nil
	case "timestamp":
		return iceberg.PrimitiveTypes.Timestamp, nil
	case "timestamptz":
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case "uuid":
		return iceberg.PrimitiveTypes.UUID, nil
	default:
		if strings.HasPrefix(typeName, "decimal") {
			// decimal(p,s)
			return iceberg.PrimitiveTypes.String, nil // упрощённо
		}
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
}

func protoLiteralToIceberg(lit *pb.Literal, typ iceberg.Type) (iceberg.Literal, error) {
	if lit == nil {
		return nil, errors.New("nil literal")
	}

	switch typ {
	case iceberg.PrimitiveTypes.Bool:
		return iceberg.NewLiteral(lit.GetBoolValue()), nil
	case iceberg.PrimitiveTypes.Int32:
		return iceberg.NewLiteral(lit.GetInt32Value()), nil
	case iceberg.PrimitiveTypes.Int64:
		return iceberg.NewLiteral(lit.GetInt64Value()), nil
	case iceberg.PrimitiveTypes.Float32:
		return iceberg.NewLiteral(lit.GetFloat32Value()), nil
	case iceberg.PrimitiveTypes.Float64:
		return iceberg.NewLiteral(lit.GetFloat64Value()), nil
	case iceberg.PrimitiveTypes.String:
		return iceberg.NewLiteral(lit.GetStringValue()), nil
	case iceberg.PrimitiveTypes.Binary:
		return iceberg.NewLiteral(lit.GetBytesValue()), nil
	case iceberg.PrimitiveTypes.Date:
		return iceberg.NewLiteral(iceberg.Date(lit.GetDateValue())), nil
	case iceberg.PrimitiveTypes.Time:
		return iceberg.NewLiteral(iceberg.Time(lit.GetTimeValue())), nil
	case iceberg.PrimitiveTypes.Timestamp:
		return iceberg.NewLiteral(iceberg.Timestamp(lit.GetTimestampValue())), nil
	case iceberg.PrimitiveTypes.TimestampTz:
		return iceberg.NewLiteral(iceberg.Timestamp(lit.GetTimestampValue())), nil
	case iceberg.PrimitiveTypes.UUID:
		return iceberg.NewLiteral(lit.GetUuidValue()), nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", typ)
	}
}

func icebergFilterToProto(f *types.DynamicFilter) *pb.DynamicFilter {
	if f == nil {
		return nil
	}

	pbFilter := &pb.DynamicFilter{
		FieldName:        f.FieldName,
		FieldType:        typeToString(f.FieldType),
		FilterType:       pb.FilterType(f.FilterType),
		TotalValues:      f.TotalValues,
		BuildTimestampMs: f.BuildTimestampMs,
	}

	for _, v := range f.Values {
		pbFilter.InValues = append(pbFilter.InValues, icebergLiteralToProto(v))
	}

	for _, r := range f.Ranges {
		pbFilter.Ranges = append(pbFilter.Ranges, &pb.ValueRange{
			Lower:          icebergLiteralToProto(r.Lower),
			Upper:          icebergLiteralToProto(r.Upper),
			LowerInclusive: r.LowerInclusive,
			UpperInclusive: r.UpperInclusive,
		})
	}

	return pbFilter
}

func icebergLiteralToProto(l iceberg.Literal) *pb.Literal {
	if l == nil {
		return nil
	}

	pbLit := &pb.Literal{}

	switch v := l.Any().(type) {
	case bool:
		pbLit.Value = &pb.Literal_BoolValue{BoolValue: v}
	case int32:
		pbLit.Value = &pb.Literal_Int32Value{Int32Value: v}
	case int64:
		pbLit.Value = &pb.Literal_Int64Value{Int64Value: v}
	case float32:
		pbLit.Value = &pb.Literal_Float32Value{Float32Value: v}
	case float64:
		pbLit.Value = &pb.Literal_Float64Value{Float64Value: v}
	case string:
		pbLit.Value = &pb.Literal_StringValue{StringValue: v}
	case []byte:
		pbLit.Value = &pb.Literal_BytesValue{BytesValue: v}
	case iceberg.Date:
		pbLit.Value = &pb.Literal_DateValue{DateValue: int64(v)}
	case iceberg.Time:
		pbLit.Value = &pb.Literal_TimeValue{TimeValue: int64(v)}
	case iceberg.Timestamp:
		pbLit.Value = &pb.Literal_TimestampValue{TimestampValue: int64(v)}
	case iceberg.Decimal:
		pbLit.Value = &pb.Literal_DecimalValue{DecimalValue: v.String()}
	case uuid.UUID:
		pbLit.Value = &pb.Literal_UuidValue{UuidValue: v.String()}
	default:
		pbLit.Value = &pb.Literal_StringValue{StringValue: fmt.Sprintf("%v", v)}
	}

	return pbLit
}

func typeToString(t iceberg.Type) string {
	switch t {
	case iceberg.PrimitiveTypes.Bool:
		return "boolean"
	case iceberg.PrimitiveTypes.Int32:
		return "int32"
	case iceberg.PrimitiveTypes.Int64:
		return "int64"
	case iceberg.PrimitiveTypes.Float32:
		return "float32"
	case iceberg.PrimitiveTypes.Float64:
		return "float64"
	case iceberg.PrimitiveTypes.String:
		return "string"
	case iceberg.PrimitiveTypes.Binary:
		return "binary"
	case iceberg.PrimitiveTypes.Date:
		return "date"
	case iceberg.PrimitiveTypes.Time:
		return "time"
	case iceberg.PrimitiveTypes.Timestamp:
		return "timestamp"
	case iceberg.PrimitiveTypes.TimestampTz:
		return "timestamptz"
	case iceberg.PrimitiveTypes.UUID:
		return "uuid"
	default:
		return "unknown"
	}
}
