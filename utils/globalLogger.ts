import * as log4js from 'log4js';
import * as config from "config";
import SafeRabbitMQAppender from "./safeRabbitMqLogger";

// Determine if the environment is local or server
const isLocal = process.env.NODE_ENV === "local";

// Configure log4js with different appenders and categories based on environment
const appenders: any = {
  console: {
    type: "console",
    layout: { type: "colored" },
  },
  fileLogger: {
    type: "file",
    filename: "logs/all-logs.log",
    maxLogSize: 10485760, // 10 MB
  },
};

// If rabbitmq isn't install on your system just set the NODE_ENV to local
if (!isLocal) {
  appenders.safeMq = {
    type: { configure: (config: any) => new SafeRabbitMQAppender(config) },
    ...config.get("AMPQ_CONFIGURATIONS")
  };
}

log4js.configure({
  appenders,
  categories: {
    default: { appenders: ["console", "fileLogger"], level: "debug" },
    mq: { appenders: isLocal ? ["console"] : ["safeMq"], level: "debug" },
  },
});


// Export logger instance
export const logger = log4js.getLogger();
logger.info("Initializing Logging Service");