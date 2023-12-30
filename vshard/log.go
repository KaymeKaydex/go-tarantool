package vshard

type LogProvider interface {
	Info(string)
	Debug(string)
	Error(string)
}
