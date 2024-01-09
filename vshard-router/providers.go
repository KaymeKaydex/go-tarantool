package vshard_router

import (
	"context"
	"log"
	"time"
)

// Logs

type LogProvider interface {
	Info(context.Context, string)
	Debug(context.Context, string)
	Error(context.Context, string)
	Warn(context.Context, string)
}

type EmptyLogger struct{}

func (e *EmptyLogger) Info(ctx context.Context, msg string)  {}
func (e *EmptyLogger) Debug(ctx context.Context, msg string) {}
func (e *EmptyLogger) Error(ctx context.Context, msg string) {}
func (e *EmptyLogger) Warn(ctx context.Context, msg string)  {}

type StdoutLogger struct{}

func (e *StdoutLogger) Info(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Debug(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Error(ctx context.Context, msg string) {
	log.Println(msg)
}
func (e *StdoutLogger) Warn(ctx context.Context, msg string) {
	log.Println(msg)
}

// Metrics

type MetricsProvider interface {
	CronDiscoveryEvent(ok bool, duration time.Duration, reason string)
	RetryOnCall(reason string)
	// todo: метрики для вызова
}

// todo: добавить may metrics provider

type EmptyMetrics struct{}

func (e *EmptyMetrics) CronDiscoveryEvent(ok bool, duration time.Duration, reason string) {}
func (e *EmptyMetrics) RetryOnCall(reason string)                                         {}

// todo: cделать provider к изменению топологии роутера
// 1 - vshard.storage.internal.replicasets
// 2 - callback
