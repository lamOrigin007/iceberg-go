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

// Package client предоставляет клиент для подключения к координатору динамических фильтров
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/dynamicfilter/types"
	pb "github.com/apache/iceberg-go/dynamicfilter/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client клиент для подключения к координатору динамических фильтров
type Client struct {
	conn   *grpc.ClientConn
	client pb.DynamicFilterServiceClient

	mu           sync.RWMutex
	queryID      string
	sessionID    string
	sessionActive bool

	// Буферы для отправки значений
	valueBuffers map[fieldKey]*valueBuffer
	bufferSize   int
}

type fieldKey struct {
	sourceAlias string
	fieldName   string
}

type valueBuffer struct {
	values   []iceberg.Literal
	fieldType iceberg.Type
	mu       sync.Mutex
}

// Config конфигурация клиента
type Config struct {
	// BufferSize размер буфера для отправки значений (по умолчанию DefaultBatchSize)
	BufferSize int

	// DialTimeout таймаут подключения
	DialTimeout time.Duration

	// EnableCompression включение сжатия
	EnableCompression bool
}

// Default значения для клиента
const (
	DefaultBatchSize   = types.DefaultBatchSize
	DefaultWaitTimeout = types.DefaultWaitTimeout
)

func (c *Config) applyDefaults() {
	if c.BufferSize == 0 {
		c.BufferSize = DefaultBatchSize
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
}

// Option функциональные опции для клиента
type Option func(*Config)

// WithBufferSize устанавливает размер буфера для отправки значений
func WithBufferSize(size int) Option {
	return func(c *Config) {
		c.BufferSize = size
	}
}

// WithDialTimeout устанавливает таймаут подключения
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.DialTimeout = timeout
	}
}

// WithCompression включает сжатие GRPC сообщений
func WithCompression() Option {
	return func(c *Config) {
		c.EnableCompression = true
	}
}

// Connect подключается к координатору
func Connect(coordinatorAddr string, opts ...Option) (*Client, error) {
	cfg := &Config{}
	for _, opt := range opts {
		opt(cfg)
	}
	cfg.applyDefaults()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	if cfg.EnableCompression {
		dialOpts = append(dialOpts,
			grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, coordinatorAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	client := &Client{
		conn:         conn,
		client:       pb.NewDynamicFilterServiceClient(conn),
		valueBuffers: make(map[fieldKey]*valueBuffer),
		bufferSize:   cfg.BufferSize,
	}

	return client, nil
}

// ConnectWithContext подключается к координатору с предоставленным контекстом
func ConnectWithContext(ctx context.Context, coordinatorAddr string, opts ...Option) (*Client, error) {
	cfg := &Config{}
	for _, opt := range opts {
		opt(cfg)
	}
	cfg.applyDefaults()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	if cfg.EnableCompression {
		dialOpts = append(dialOpts,
			grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")))
	}

	conn, err := grpc.DialContext(ctx, coordinatorAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to coordinator: %w", err)
	}

	client := &Client{
		conn:         conn,
		client:       pb.NewDynamicFilterServiceClient(conn),
		valueBuffers: make(map[fieldKey]*valueBuffer),
		bufferSize:   cfg.BufferSize,
	}

	return client, nil
}

// Close закрывает соединение с координатором
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Остановка активной сессии если есть
	if c.sessionActive && c.queryID != "" {
		c.stopQueryUnsafe(context.Background())
	}

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// StartQuery начинает сессию для запроса
func (c *Client) StartQuery(ctx context.Context, queryID, sessionID string,
	mappings []types.FieldMapping, totalSegments int) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sessionActive {
		return fmt.Errorf("session already active for query %s", c.queryID)
	}

	// Преобразование маппингов в proto
	pbMappings := make([]*pb.FieldMapping, 0, len(mappings))
	for _, m := range mappings {
		pbMappings = append(pbMappings, &pb.FieldMapping{
			TargetAlias:   m.TargetAlias,
			TargetField:   m.TargetField,
			SourceAlias:   m.SourceAlias,
			SourceField:   m.SourceField,
			SourceFieldId: int32(m.SourceFieldID),
			TargetFieldId: int32(m.TargetFieldID),
			FieldType:     typeToString(m.FieldType),
		})
	}

	req := &pb.StartQueryRequest{
		QueryId:       queryID,
		SessionId:     sessionID,
		Mappings:      pbMappings,
		TotalSegments: int32(totalSegments),
		TimeoutMs:     int64(5 * time.Minute / time.Millisecond),
	}

	resp, err := c.client.StartQuery(ctx, req)
	if err != nil {
		return fmt.Errorf("start query failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("start query failed: %s", resp.ErrorMessage)
	}

	c.queryID = queryID
	c.sessionID = sessionID
	c.sessionActive = true
	c.valueBuffers = make(map[fieldKey]*valueBuffer)

	return nil
}

// StopQuery завершает сессию запроса
func (c *Client) StopQuery(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.stopQueryUnsafe(ctx)
}

func (c *Client) stopQueryUnsafe(ctx context.Context) error {
	if !c.sessionActive {
		return nil
	}

	req := &pb.StopQueryRequest{
		QueryId:   c.queryID,
		SessionId: c.sessionID,
	}

	resp, err := c.client.StopQuery(ctx, req)
	if err != nil {
		return fmt.Errorf("stop query failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("stop query failed: %s", resp.ErrorMessage)
	}

	c.sessionActive = false
	c.queryID = ""
	c.sessionID = ""

	return nil
}

// RegisterSegment регистрирует сегмент на координаторе
func (c *Client) RegisterSegment(ctx context.Context, queryID, sessionID string,
	host string, port int32, sourceAliases, targetAliases []string) error {

	req := &pb.RegisterSegmentRequest{
		QueryId:       queryID,
		SessionId:     sessionID,
		SegmentHost:   host,
		SegmentPort:   port,
		SourceAliases: sourceAliases,
		TargetAliases: targetAliases,
	}

	resp, err := c.client.RegisterSegment(ctx, req)
	if err != nil {
		return fmt.Errorf("register segment failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("register segment failed: %s", resp.ErrorMessage)
	}

	return nil
}

// SendValues отправляет значения для source поля
func (c *Client) SendValues(ctx context.Context, sourceAlias, fieldName string,
	values []iceberg.Literal, fieldType iceberg.Type) error {

	c.mu.RLock()
	queryID := c.queryID
	sessionID := c.sessionID
	c.mu.RUnlock()

	if !c.sessionActive {
		return fmt.Errorf("no active session")
	}

	key := fieldKey{sourceAlias: sourceAlias, fieldName: fieldName}

	// Получение или создание буфера
	c.mu.Lock()
	buf, ok := c.valueBuffers[key]
	if !ok {
		buf = &valueBuffer{
			values:    make([]iceberg.Literal, 0, c.bufferSize),
			fieldType: fieldType,
		}
		c.valueBuffers[key] = buf
	}
	c.mu.Unlock()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	// Добавление значений в буфер
	buf.values = append(buf.values, values...)

	// Отправка если буфер заполнен
	if len(buf.values) >= c.bufferSize {
		if err := c.flushBuffer(ctx, queryID, sessionID, key, buf); err != nil {
			return err
		}
	}

	return nil
}

// FlushValues принудительно отправляет все буферизированные значения
func (c *Client) FlushValues(ctx context.Context, sourceAlias, fieldName string) error {
	c.mu.RLock()
	queryID := c.queryID
	sessionID := c.sessionID
	c.mu.RUnlock()

	key := fieldKey{sourceAlias: sourceAlias, fieldName: fieldName}

	c.mu.Lock()
	buf, ok := c.valueBuffers[key]
	c.mu.Unlock()

	if !ok || len(buf.values) == 0 {
		return nil
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()

	return c.flushBuffer(ctx, queryID, sessionID, key, buf)
}

func (c *Client) flushBuffer(ctx context.Context, queryID, sessionID string,
	key fieldKey, buf *valueBuffer) error {

	if len(buf.values) == 0 {
		return nil
	}

	// Создание стрима для отправки
	stream, err := c.client.CollectValues(ctx)
	if err != nil {
		return fmt.Errorf("collect values stream failed: %w", err)
	}

	// Отправка значений пакетами
	const batchSize = 1000
	for i := 0; i < len(buf.values); i += batchSize {
		end := i + batchSize
		if end > len(buf.values) {
			end = len(buf.values)
		}

		batch := buf.values[i:end]
		pbValues := make([]*pb.Literal, 0, len(batch))
		for _, v := range batch {
			pbValues = append(pbValues, icebergLiteralToProto(v))
		}

		req := &pb.CollectValueRequest{
			QueryId:     queryID,
			SessionId:   sessionID,
			SourceAlias: key.sourceAlias,
			FieldName:   key.fieldName,
			Values:      pbValues,
			IsFinal:     false,
		}

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("send values failed: %w", err)
		}
	}

	// Завершение стрима
	resp, err := stream.CloseAndRecv()
	if err != nil {
		if err == io.EOF {
			// Нормальное завершение
			buf.values = buf.values[:0]
			return nil
		}
		return fmt.Errorf("close stream failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("collect values failed: %s", resp.ErrorMessage)
	}

	// Очистка буфера
	buf.values = buf.values[:0]

	return nil
}

// SignalSourceComplete сигнализирует о завершении сбора значений для source поля
func (c *Client) SignalSourceComplete(ctx context.Context, sourceAlias, fieldName string) error {
	c.mu.RLock()
	queryID := c.queryID
	sessionID := c.sessionID
	c.mu.RUnlock()

	if !c.sessionActive {
		return fmt.Errorf("no active session")
	}

	// Сначала отправляем оставшиеся значения
	if err := c.FlushValues(ctx, sourceAlias, fieldName); err != nil {
		return err
	}

	req := &pb.SignalSourceCompleteRequest{
		QueryId:     queryID,
		SessionId:   sessionID,
		SourceAlias: sourceAlias,
		FieldName:   fieldName,
	}

	resp, err := c.client.SignalSourceComplete(ctx, req)
	if err != nil {
		return fmt.Errorf("signal source complete failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("signal source complete failed: %s", resp.ErrorMessage)
	}

	return nil
}

// GetFilter получает фильтр для target поля (не блокируется)
func (c *Client) GetFilter(ctx context.Context, targetAlias, fieldName string) (*types.DynamicFilter, error) {
	c.mu.RLock()
	queryID := c.queryID
	sessionID := c.sessionID
	c.mu.RUnlock()

	if !c.sessionActive {
		return nil, fmt.Errorf("no active session")
	}

	req := &pb.GetFilterRequest{
		QueryId:     queryID,
		SessionId:   sessionID,
		TargetAlias: targetAlias,
		FieldName:   fieldName,
	}

	resp, err := c.client.GetFilter(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get filter failed: %w", err)
	}

	if !resp.IsReady {
		return nil, nil
	}

	return protoFilterToTypes(resp.Filter)
}

// WaitForFilter ожидает готовности фильтра для target поля (блокируется)
func (c *Client) WaitForFilter(ctx context.Context, targetAlias, fieldName string,
	timeout time.Duration) (*types.DynamicFilter, error) {

	c.mu.RLock()
	queryID := c.queryID
	sessionID := c.sessionID
	c.mu.RUnlock()

	if !c.sessionActive {
		return nil, fmt.Errorf("no active session")
	}

	if timeout == 0 {
		timeout = DefaultWaitTimeout
	}

	req := &pb.WaitForFilterRequest{
		QueryId:     queryID,
		SessionId:   sessionID,
		TargetAlias: targetAlias,
		FieldName:   fieldName,
		TimeoutMs:   int64(timeout.Milliseconds()),
	}

	resp, err := c.client.WaitForFilter(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("wait for filter failed: %w", err)
	}

	if resp.TimedOut {
		return nil, fmt.Errorf("wait for filter timed out")
	}

	if !resp.IsReady {
		return nil, nil
	}

	return protoFilterToTypes(resp.Filter)
}

// QueryStatus запрашивает статус запроса
func (c *Client) QueryStatus(ctx context.Context) (*pb.QueryStatusResponse, error) {
	c.mu.RLock()
	queryID := c.queryID
	sessionID := c.sessionID
	c.mu.RUnlock()

	if !c.sessionActive {
		return nil, fmt.Errorf("no active session")
	}

	req := &pb.QueryStatusRequest{
		QueryId:   queryID,
		SessionId: sessionID,
	}

	return c.client.QueryStatus(ctx, req)
}

// Вспомогательные функции

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

func protoFilterToTypes(f *pb.DynamicFilter) (*types.DynamicFilter, error) {
	if f == nil {
		return nil, nil
	}

	fieldType, err := parseFieldType(f.FieldType)
	if err != nil {
		return nil, err
	}

	result := &types.DynamicFilter{
		FieldName:        f.FieldName,
		FieldType:        fieldType,
		FilterType:       types.FilterType(f.FilterType),
		TotalValues:      f.TotalValues,
		BuildTimestampMs: f.BuildTimestampMs,
	}

	for _, v := range f.InValues {
		lit, err := protoLiteralToIceberg(v, fieldType)
		if err != nil {
			return nil, err
		}
		result.Values = append(result.Values, lit)
	}

	for _, r := range f.Ranges {
		lower, err := protoLiteralToIceberg(r.Lower, fieldType)
		if err != nil {
			return nil, err
		}
		upper, err := protoLiteralToIceberg(r.Upper, fieldType)
		if err != nil {
			return nil, err
		}

		result.Ranges = append(result.Ranges, types.ValueRange{
			Lower:          lower,
			Upper:          upper,
			LowerInclusive: r.LowerInclusive,
			UpperInclusive: r.UpperInclusive,
		})
	}

	return result, nil
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
			return iceberg.PrimitiveTypes.String, nil
		}
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
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
