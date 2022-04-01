package log

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"hxextract/app/config"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"runtime/debug"
	"strings"
	"time"
)

var Log *zap.Logger
var Status *zap.Logger

var logConfig zapcore.EncoderConfig

func init() {
	logConfig = zapcore.EncoderConfig{
		TimeKey:        "logtime",
		LevelKey:       "level",
		NameKey:        "name",
		CallerKey:      "line",
		MessageKey:     "msg",
		FunctionKey:    "func",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"),
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}

}

// InitLog 初始化日志
func InitLog() {
	logCfg := config.GetLog()
	level := LevelParse(logCfg.LogLevel)
	Log = NewLog(logCfg.LogPath, level)
	Status = NewStatus(logCfg.StatLogPath)
}

// NewLog
//  @Description: 创建logger实例，用于记录到日志文件并打印出来
//  @param file 日志路径
//  @param lvl 日志等级
//  @return *zap.Logger
//
func NewLog(file string, lvl zapcore.Level) *zap.Logger {
	ws := getFileSync(file)
	fileCore := zapcore.NewCore(zapcore.NewJSONEncoder(logConfig),
		zapcore.NewMultiWriteSyncer(ws, zapcore.AddSync(os.Stdout)),
		lvl)
	return zap.New(fileCore, zap.AddCaller())
}

// NewStatus
//  @Description:
//  @param file
//  @return *zap.Logger
//
func NewStatus(file string) *zap.Logger {
	ws := getFileSync(file)
	fileCore := zapcore.NewCore(zapcore.NewJSONEncoder(logConfig), ws, zapcore.InfoLevel)
	return zap.New(fileCore)
}

func GinLog(file string, r *gin.Engine) {
	ws := getFileSync(file)
	fileCore := zapcore.NewCore(zapcore.NewConsoleEncoder(logConfig), ws, zapcore.DebugLevel)
	lg := zap.New(fileCore)
	r.Use(GinLogger(lg), GinRecovery(lg, true))
}

func getFileSync(file string) zapcore.WriteSyncer {
	return zapcore.AddSync(&lumberjack.Logger{
		Filename:   file,  //日志文件存放目录
		MaxSize:    10,    //文件大小限制,单位MB
		MaxBackups: 30,    //最大保留日志文件数量
		MaxAge:     30,    //日志文件保留天数
		Compress:   false, //是否压缩处理
	})
}

// GinLogger 接收gin框架默认的日志
func GinLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()

		cost := time.Since(start)
		logger.Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.String("errors", c.Errors.ByType(gin.ErrorTypePrivate).String()),
			zap.Duration("cost", cost),
		)
	}
}

// GinRecovery recover掉项目可能出现的panic
func GinRecovery(logger *zap.Logger, stack bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Check for a broken connection, as it is not really a
				// condition that warrants a panic stack trace.
				var brokenPipe bool
				if ne, ok := err.(*net.OpError); ok {
					if se, ok := ne.Err.(*os.SyscallError); ok {
						if strings.Contains(strings.ToLower(se.Error()), "broken pipe") || strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
							brokenPipe = true
						}
					}
				}

				httpRequest, _ := httputil.DumpRequest(c.Request, false)
				if brokenPipe {
					logger.Error(c.Request.URL.Path,
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
					)
					// If the connection is dead, we can't write a status to it.
					c.Error(err.(error)) // nolint: errcheck
					c.Abort()
					return
				}

				if stack {
					logger.Error("[Recovery from panic]",
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
						zap.String("stack", string(debug.Stack())),
					)
				} else {
					logger.Error("[Recovery from panic]",
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
					)
				}
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	}
}
