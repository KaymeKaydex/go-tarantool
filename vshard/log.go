package vshard

type LogProvider interface {
	Info(string)
	Debug(string)
	Error(string)
	Warn(string)
}

type EmptyLogger struct{}

func (e *EmptyLogger) Info(msg string)  {}
func (e *EmptyLogger) Debug(msg string) {}
func (e *EmptyLogger) Error(msg string) {}
func (e *EmptyLogger) Warn(msg string)  {}
