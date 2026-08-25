const config = require("../config");

const LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

const currentLevel = LEVELS[config.logLevel.toLowerCase()] || LEVELS.info;

const log = (level, message, ...args) => {
  if (LEVELS[level] >= currentLevel) {
    const timestamp = new Date().toISOString();
    console[level](
      `[${timestamp}] [${level.toUpperCase()}] ${message}`,
      ...args,
    );
  }
};

module.exports = {
  debug: (...args) => log("debug", ...args),
  info: (...args) => log("info", ...args),
  warn: (...args) => log("warn", ...args),
  error: (...args) => log("error", ...args),
};
