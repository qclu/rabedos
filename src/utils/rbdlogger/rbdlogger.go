package rbdlogger

const (
	//go-logger version
	_VER string = "1.0.3"
)

type LogLvlT int32
type UNIT int64
type LogRollT int //dailyRolling ,rollingFile

const DATEFORMAT = "2006-01-02"

var logLevel LogLvlT = 1

const (
	_       = iota
	KB UNIT = 1 << (iota * 10)
	MB
	GB
	TB
)

const (
	ALL LogLvlT = iota
	DEBUG
	INFO
	WARN
	ERROR
	FATAL
	OFF
)

const (
	RT_DAILY LogRollT = iota
	RT_SIZE
)

func LogSetConsole(isConsole bool) {
	defaultlog.setConsole(isConsole)
}

func LogSetLevel(_level LogLvlT) {
	defaultlog.setLevel(_level)
}

func LogSetFormat(logFormat string) {
	defaultlog.setFormat(logFormat)
}

func LogSetRollingFile(fileDir, fileName string, maxNumber int32, maxSize int64, _unit UNIT) {
	defaultlog.setRollingFile(fileDir, fileName, maxNumber, maxSize, _unit)
}

func LogSetRollingDaily(fileDir, fileName string) {
	defaultlog.setRollingDaily(fileDir, fileName)
}

func Debug(v ...interface{}) {
	defaultlog.debug(v...)
}
func Info(v ...interface{}) {
	defaultlog.info(v...)
}
func Warn(v ...interface{}) {
	defaultlog.warn(v...)
}
func Error(v ...interface{}) {
	defaultlog.error(v...)
}
func Fatal(v ...interface{}) {
	defaultlog.fatal(v...)
}

func LogSetLevelFile(level LogLvlT, dir, fileName string) {
	defaultlog.setLevelFile(level, dir, fileName)
}
