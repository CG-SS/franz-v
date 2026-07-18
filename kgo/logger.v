// Logging for the client. A minimal interface plus two implementations;
// call sites use V string interpolation rather than key-value pairs.
module kgo

import time

// LogLevel is a level to log at. Levels are ordered: a logger at .info
// also receives .warn and .err messages.
pub enum LogLevel {
	silent
	err
	warn
	info
	debug
}

// str returns the level name in log-line form.
pub fn (l LogLevel) str() string {
	return match l {
		.silent { 'SILENT' }
		.err { 'ERROR' }
		.warn { 'WARN' }
		.info { 'INFO' }
		.debug { 'DEBUG' }
	}
}

// Logger receives client log lines.
pub interface Logger {
	// level returns the maximum level this logger wants.
	level() LogLevel
	// log receives a message at the given level; it is only called for
	// levels at or below level().
	log(level LogLevel, msg string)
}

// NopLogger drops all logs; the Config default.
pub struct NopLogger {}

// level implements Logger; NopLogger accepts nothing.
pub fn (n NopLogger) level() LogLevel {
	return .silent
}

// log implements Logger by dropping the message.
pub fn (n NopLogger) log(level LogLevel, msg string) {
}

// BasicLogger writes timestamped lines to stderr.
pub struct BasicLogger {
pub mut:
	max_level LogLevel = .info
	prefix    string
}

// level implements Logger with the configured max level.
pub fn (b BasicLogger) level() LogLevel {
	return b.max_level
}

// log implements Logger, writing a timestamped line to stderr.
pub fn (b BasicLogger) log(level LogLevel, msg string) {
	ts := time.now().format_ss_micro()
	eprintln('${ts} [${level}] ${b.prefix}${msg}')
}

// log emits a log line if the configured logger accepts the level.
fn (c &Config) log(level LogLevel, msg string) {
	if int(level) <= int(c.logger.level()) && level != .silent {
		c.logger.log(level, msg)
	}
}
