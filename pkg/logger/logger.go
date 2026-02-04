package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// LogLevel representa os níveis de log disponíveis
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

// String retorna a representação em string do nível de log
func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel converte uma string em LogLevel
func ParseLogLevel(level string) LogLevel {
	levelUpper := strings.ToUpper(strings.TrimSpace(level))
	switch levelUpper {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN", "WARNING":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO // Padrão
	}
}

// Logger gerencia mensagens de log com níveis configuráveis
type Logger struct {
	level      LogLevel
	logger     *log.Logger
	mu         sync.RWMutex
	output     io.Writer
	prefix     string
	flags      int
}

var (
	// defaultLogger é o logger padrão usado globalmente (singleton)
	// Inicializado automaticamente no init() ou pode ser configurado via InitFromConfig
	defaultLogger *Logger
	once          sync.Once
	mu            sync.RWMutex // Proteção para acesso ao defaultLogger
)

// init inicializa o logger padrão (singleton)
func init() {
	once.Do(func() {
		defaultLogger = NewLogger(INFO, "", log.LstdFlags)
	})
}

// getDefaultLogger retorna o logger padrão de forma thread-safe
func getDefaultLogger() *Logger {
	mu.RLock()
	defer mu.RUnlock()
	if defaultLogger == nil {
		// Fallback: cria um logger se por algum motivo não foi inicializado
		return NewLogger(INFO, "", log.LstdFlags)
	}
	return defaultLogger
}

// NewLogger cria uma nova instância de Logger
// level: nível mínimo de log a ser exibido
// prefix: prefixo para todas as mensagens de log
// flags: flags do log padrão (log.LstdFlags, etc.)
func NewLogger(level LogLevel, prefix string, flags int) *Logger {
	return &Logger{
		level:  level,
		logger: log.New(os.Stderr, prefix, flags),
		output: os.Stderr,
		prefix: prefix,
		flags:  flags,
	}
}

// SetLevel define o nível mínimo de log
func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// GetLevel retorna o nível atual de log
func (l *Logger) GetLevel() LogLevel {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level
}

// SetOutput define o destino de saída do log
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.output = w
	l.logger = log.New(w, l.prefix, l.flags)
}

// shouldLog verifica se o nível de log deve ser exibido
func (l *Logger) shouldLog(level LogLevel) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level >= l.level
}

// log escreve uma mensagem de log se o nível for suficiente
func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if !l.shouldLog(level) {
		return
	}
	
	levelStr := level.String()
	message := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] %s", levelStr, message)
}

// Debug registra uma mensagem de debug
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DEBUG, format, args...)
}

// Info registra uma mensagem informativa
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(INFO, format, args...)
}

// Warn registra uma mensagem de aviso
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WARN, format, args...)
}

// Error registra uma mensagem de erro
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ERROR, format, args...)
}

// SetDefaultLogger define o logger padrão usado globalmente (singleton)
// Thread-safe: protege o acesso ao defaultLogger
func SetDefaultLogger(logger *Logger) {
	if logger == nil {
		return // Ignora tentativa de definir nil
	}
	mu.Lock()
	defer mu.Unlock()
	defaultLogger = logger
}

// GetDefaultLogger retorna o logger padrão (singleton)
// Thread-safe: sempre retorna uma instância válida
func GetDefaultLogger() *Logger {
	return getDefaultLogger()
}

// SetDefaultLevel define o nível do logger padrão (singleton)
func SetDefaultLevel(level LogLevel) {
	getDefaultLogger().SetLevel(level)
}

// SetDefaultLevelFromString define o nível do logger padrão a partir de uma string
func SetDefaultLevelFromString(level string) {
	SetDefaultLevel(ParseLogLevel(level))
}

// Funções globais estáticas para usar o logger padrão (singleton)
// Estas funções podem ser chamadas de qualquer lugar sem precisar de uma instância

// Debug registra uma mensagem de debug no logger padrão
// Função estática - usa o singleton automaticamente
func Debug(format string, args ...interface{}) {
	getDefaultLogger().Debug(format, args...)
}

// Info registra uma mensagem informativa no logger padrão
// Função estática - usa o singleton automaticamente
func Info(format string, args ...interface{}) {
	getDefaultLogger().Info(format, args...)
}

// Warn registra uma mensagem de aviso no logger padrão
// Função estática - usa o singleton automaticamente
func Warn(format string, args ...interface{}) {
	getDefaultLogger().Warn(format, args...)
}

// Error registra uma mensagem de erro no logger padrão
// Função estática - usa o singleton automaticamente
func Error(format string, args ...interface{}) {
	getDefaultLogger().Error(format, args...)
}

// WouldLog verifica se um nível de log seria exibido no logger padrão
// Útil para decidir se deve chamar t.Logf() em testes
func WouldLog(level LogLevel) bool {
	return getDefaultLogger().shouldLog(level)
}

// TestLogger é uma interface para logging em testes
// Permite que o logger seja usado com *testing.T sem criar dependência direta
type TestLogger interface {
	Helper()
	Logf(format string, args ...interface{})
}

// TestDebug registra uma mensagem de debug usando logger e t.Logf (se nível DEBUG estiver habilitado)
// Útil para testes que querem combinar logger centralizado com output do teste (-v)
func TestDebug(t TestLogger, format string, args ...interface{}) {
	if t != nil {
		t.Helper()
	}
	// Chama logger.Debug primeiro (ele verifica o nível internamente)
	Debug(format, args...)
	// Se o logger exibiria a mensagem (nível DEBUG habilitado), também chama t.Logf
	// Isso garante que mensagens apareçam no output do teste quando o nível permitir
	if t != nil && WouldLog(DEBUG) {
		t.Logf(format, args...)
	}
}

// TestInfo registra uma mensagem informativa usando logger e t.Logf (se nível INFO estiver habilitado)
// Útil para testes que querem combinar logger centralizado com output do teste (-v)
func TestInfo(t TestLogger, format string, args ...interface{}) {
	if t != nil {
		t.Helper()
	}
	// Chama logger.Info primeiro (ele verifica o nível internamente)
	Info(format, args...)
	// Se o logger exibiria a mensagem (nível INFO habilitado), também chama t.Logf
	if t != nil && WouldLog(INFO) {
		t.Logf(format, args...)
	}
}

// TestWarn registra uma mensagem de aviso usando logger e t.Logf (se nível WARN estiver habilitado)
// Útil para testes que querem combinar logger centralizado com output do teste (-v)
func TestWarn(t TestLogger, format string, args ...interface{}) {
	if t != nil {
		t.Helper()
	}
	// Chama logger.Warn primeiro (ele verifica o nível internamente)
	Warn(format, args...)
	// Se o logger exibiria a mensagem (nível WARN habilitado), também chama t.Logf
	if t != nil && WouldLog(WARN) {
		t.Logf(format, args...)
	}
}

// TestError registra uma mensagem de erro usando logger e t.Logf (sempre aparece, ERROR é o nível mais alto)
// Útil para testes que querem combinar logger centralizado com output do teste (-v)
func TestError(t TestLogger, format string, args ...interface{}) {
	if t != nil {
		t.Helper()
	}
	// ERROR sempre aparece no t.Logf (é o nível mais alto, sempre habilitado)
	if t != nil {
		t.Logf(format, args...)
	}
	Error(format, args...)
}
