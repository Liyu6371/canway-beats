package logger

import (
	"canway-beats/config"
	"canway-beats/define"

	"github.com/sirupsen/logrus"
)

var (
	defaultLogger  *logrus.Logger
	defaultLevel   = logrus.DebugLevel
	logPath        = ""
	defaultLogPath = "/var/log/gse/" + define.Name + ".log"

	maxSize    = 30   // 每个日志文件最大10MB
	maxBackups = 3    // 保留最近的3个日志文件
	maxAge     = 7    // 保留最近7天的日志
	compress   = true // 是否压缩旧日志

	logLevelMap = map[string]logrus.Level{
		"ERROR": logrus.ErrorLevel,
		"WARN":  logrus.WarnLevel,
		"INFO":  logrus.InfoLevel,
		"DEBUG": logrus.DebugLevel,
	}
)

func Init(c config.MapConf) {

}
