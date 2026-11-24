// Simple structured logger
const LOG_LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
const CURRENT_LEVEL = LOG_LEVELS[process.env.LOG_LEVEL || 'INFO'] ?? LOG_LEVELS.INFO;

function formatMessage(level, message, meta = {}) {
  const timestamp = new Date().toISOString();
  const metaString = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
  return `[${timestamp}] [${level}] ${message}${metaString}`;
}

const logger = {
  debug: (msg, meta) => {
    if (CURRENT_LEVEL <= LOG_LEVELS.DEBUG) console.debug(formatMessage('DEBUG', msg, meta));
  },
  info: (msg, meta) => {
    if (CURRENT_LEVEL <= LOG_LEVELS.INFO) console.log(formatMessage('INFO', msg, meta));
  },
  warn: (msg, meta) => {
    if (CURRENT_LEVEL <= LOG_LEVELS.WARN) console.warn(formatMessage('WARN', msg, meta));
  },
  error: (msg, meta) => {
    if (CURRENT_LEVEL <= LOG_LEVELS.ERROR) console.error(formatMessage('ERROR', msg, meta));
  }
};

module.exports = logger;
