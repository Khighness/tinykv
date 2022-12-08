package log

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// init initializes zap.Logger then we can use zap.L() to get this logger.
func init() {
	var core zapcore.Core
	fileCore := zapcore.NewCore(zapFileEncoder(), zapWriteSyncer(), zapLevelEnabler())
	consoleCore := zapcore.NewCore(zapConsoleEncoder(), os.Stdout, zapLevelEnabler())
	core = zapcore.NewTee(fileCore, consoleCore)
	logger := zap.New(core, zap.AddCaller())
	zap.ReplaceGlobals(logger)
}

func zapLevelEnabler() zapcore.Level {
	return zapcore.DebugLevel
}

func zapEncodeConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		TimeKey:        "ts",
		NameKey:        "logger",
		CallerKey:      "caller_line",
		StacktraceKey:  "stacktrace",
		LineEnding:     "\n",
		EncodeLevel:    zapEncodeLevel,
		EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapEncodeCaller,
	}
}

func zapFileEncoder() zapcore.Encoder {
	return zapcore.NewJSONEncoder(zapEncodeConfig())
}

func zapConsoleEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zapEncodeConfig())
}

func zapEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

func zapEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(caller.TrimmedPath())
}

func zapWriteSyncer() zapcore.WriteSyncer {
	logDir := "log/"
	_ = os.Mkdir(logDir, 0777)
	lumberJackLogger := &lumberjack.Logger{
		Filename:   logDir + "/raft.log",
		MaxSize:    1000,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   true,
	}
	return zapcore.AddSync(lumberJackLogger)
}
