// Custom RabbitMQ appender for log4js with Error handler
class SafeRabbitMQAppender {
    private rabbitMQAppender: any;
  
    constructor(config: any) {
      this.rabbitMQAppender = require("@log4js-node/rabbitmq").configure(config);
    }
  
    write(loggingEvent: any) {
      try {
        this.rabbitMQAppender(loggingEvent);
      } catch (error) {
        console.error("Failed to send log to RabbitMQ:", error);
      }
    }
  
    shutdown(callback: any) {
      this.rabbitMQAppender.shutdown(callback);
    }
  }

module.exports = SafeRabbitMQAppender