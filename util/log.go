package util

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Level 日志级别
type Level int

const (
	// DEBUG 调试级别
	DEBUG Level = iota
	// INFO 信息级别
	INFO
	// WARN 警告级别
	WARN
	// ERROR 错误级别
	ERROR
	// FATAL 致命错误级别
	FATAL
	// PANIC panic级别
	PANIC
)

var levelColors = map[Level]string{
	DEBUG: "34", // 蓝色
	INFO:  "32", // 绿色
	WARN:  "33", // 黄色
	ERROR: "31", // 红色
	FATAL: "35", // 紫色
	PANIC: "35", // 紫色
}

var levelNames = map[Level]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
	FATAL: "FATAL",
	PANIC: "PANIC",
}

var Log *Logger // 全局日志记录器

// Logger 日志记录器
type Logger struct {
	mu      sync.Mutex  // 确保并发安全
	out     io.Writer   // 输出目标
	file    *os.File    // 日志文件
	level   Level       // 日志级别
	buf     []byte      // 临时缓冲区，避免频繁分配内存
	stdLog  *log.Logger // 标准库logger
	format  string      // 日志格式，支持"text"和"json"
	console bool        // 是否输出到控制台
}

// Options 日志选项
type Options struct {
	Level     Level  // 日志级别
	FilePath  string // 日志文件路径
	MaxSize   int64  // 单个日志文件最大尺寸（字节）
	ToConsole bool   // 是否同时输出到控制台
	Format    string // 日志格式，支持"text"和"json"
}

func init() {
	// TODO 从配置文件中获取初始化参数
	Log, _ = newLogger(Options{
		Level:     DEBUG,
		FilePath:  "",
		MaxSize:   1024 * 1024 * 100, // 100MB
		ToConsole: true,
		Format:    "text",
	})
	// 设置全局日志记录器
}

// newLogger 创建新的日志记录器
func newLogger(opts Options) (*Logger, error) {
	var writers []io.Writer
	var logFile *os.File
	var err error
	var hasConsole bool

	// 如果指定了文件路径，创建或打开日志文件
	if opts.FilePath != "" {
		// 确保目录存在
		dir := filepath.Dir(opts.FilePath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("创建日志目录失败: %v", err)
		}

		// 打开日志文件
		logFile, err = os.OpenFile(opts.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("打开日志文件失败: %v", err)
		}
		writers = append(writers, logFile)
	}

	// 如果需要同时输出到控制台
	if opts.ToConsole {
		writers = append(writers, os.Stdout)
		hasConsole = true
	}

	// 如果没有指定任何输出目标，默认输出到控制台
	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
		hasConsole = true
	}

	// 创建多重输出
	mw := io.MultiWriter(writers...)

	return &Logger{
		out:     mw,
		file:    logFile,
		level:   opts.Level,
		buf:     make([]byte, 0, 1024),
		stdLog:  log.New(mw, "", 0),
		format:  opts.Format,
		console: hasConsole,
	}, nil
}

// Close 关闭日志文件
func (l *Logger) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// formatHeader 格式化日志头部信息
func (l *Logger) formatHeader(level Level, caller runtime.Frame) []byte {
	l.buf = l.buf[:0]

	l.buf = append(l.buf, "Level: ["...)
	// 如果输出到控制台，为日志级别添加颜色
	if l.console {
		colorCode := levelColors[level]
		l.buf = append(l.buf, "\033["...)
		l.buf = append(l.buf, colorCode...)
		l.buf = append(l.buf, 'm')
	}
	l.buf = append(l.buf, levelNames[level]...) // 添加日志级别
	// 如果输出到控制台，重置颜色
	if l.console {
		l.buf = append(l.buf, "\033[0m"...)
	}

	l.buf = append(l.buf, "]\t"...)

	// 添加时间戳
	l.buf = append(l.buf, "Time: "...)
	l.buf = append(l.buf, time.Now().Format("2006-01-02 15:04")...)
	l.buf = append(l.buf, '\t')

	// 添加文件名和行号
	l.buf = append(l.buf, "Caller: "...)
	fileName := filepath.Base(caller.File)
	dirName := filepath.Base(filepath.Dir(caller.File))
	l.buf = append(l.buf, dirName...)
	l.buf = append(l.buf, '/')
	l.buf = append(l.buf, fileName...)
	l.buf = append(l.buf, ':')
	l.buf = append(l.buf, fmt.Sprint(caller.Line)...)
	l.buf = append(l.buf, '\t')

	return l.buf
}

// log 通用日志记录函数
func (l *Logger) log(level Level, format string, args ...any) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 获取调用者信息
	pc, file, line, ok := runtime.Caller(2)
	var frame runtime.Frame
	if ok {
		frame = runtime.Frame{File: file, Line: line, PC: pc}
	}

	// 格式化日志头部
	l.buf = l.formatHeader(level, frame)

	// 格式化日志内容
	var msg string
	if format == "" {
		msg = fmt.Sprint(args...)
	} else {
		msg = fmt.Sprintf(format, args...)
	}
	l.buf = append(l.buf, "message: "...)
	l.buf = append(l.buf, msg...)

	// 确保消息以换行符结束
	if len(msg) == 0 || msg[len(msg)-1] != '\n' {
		l.buf = append(l.buf, '\n')
	}

	// 写入日志
	l.stdLog.Output(0, string(l.buf))

	// 对于FATAL级别，输出日志后退出程序
	if level == FATAL {
		os.Exit(1)
	}

	// 对于PANIC级别，输出日志后触发panic
	if level == PANIC {
		panic(msg)
	}
}

// logJSON 以JSON格式记录日志
func (l *Logger) logJSON(level Level, format string, args ...any) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// 获取调用者信息
	pc, file, line, ok := runtime.Caller(2)
	var frame runtime.Frame
	if ok {
		frame = runtime.Frame{File: file, Line: line, PC: pc}
	}

	entry := struct {
		Level   string `json:"level"`
		Time    string `json:"time"`
		Caller  string `json:"caller"`
		Message string `json:"message"`
	}{
		Level:   levelNames[level],
		Time:    time.Now().Format("2006-01-02 15:04"),
		Caller:  filepath.Base(frame.File) + "/" + filepath.Base(filepath.Dir(frame.File)) + ":" + strconv.Itoa(frame.Line),
		Message: fmt.Sprintf(format, args...),
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "JSON序列化错误: %v\n", err)
		return
	}

	// 写入日志
	l.stdLog.Output(0, string(jsonData)+"\n")

	// 对于FATAL级别，输出日志后退出程序
	if level == FATAL {
		os.Exit(1)
	}

	// 对于PANIC级别，输出日志后触发panic
	if level == PANIC {
		panic(entry.Message)
	}
}

// Debug 输出Debug级别日志
func (l *Logger) Debug(format string, args ...any) {
	if l.format == "json" {
		l.logJSON(DEBUG, format, args...)
	} else {
		l.log(DEBUG, format, args...)
	}
}

// Info 输出Info级别日志
func (l *Logger) Info(format string, args ...any) {
	if l.format == "json" {
		l.logJSON(INFO, format, args...)
	} else {
		l.log(INFO, format, args...)
	}
}

// Warn 输出Warn级别日志
func (l *Logger) Warn(format string, args ...any) {
	if l.format == "json" {
		l.logJSON(WARN, format, args...)
	} else {
		l.log(WARN, format, args...)
	}
}

// Error 输出Error级别日志
func (l *Logger) Error(format string, args ...any) {
	if l.format == "json" {
		l.logJSON(ERROR, format, args...)
	} else {
		l.log(ERROR, format, args...)
	}
}

// Fatal 输出Fatal级别日志并退出程序
func (l *Logger) Fatal(format string, args ...any) {
	if l.format == "json" {
		l.logJSON(FATAL, format, args...)
	} else {
		l.log(FATAL, format, args...)
	}
}

// Panic 输出Panic级别日志并触发panic
func (l *Logger) Panic(format string, args ...any) {
	if l.format == "json" {
		l.logJSON(PANIC, format, args...)
	} else {
		l.log(PANIC, format, args...)
	}
}
