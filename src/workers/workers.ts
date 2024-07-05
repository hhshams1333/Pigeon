import {Consumer, EachMessageHandler, Kafka, logLevel} from "kafkajs";
import config from "config";
import {logger} from "../../utils/globalLogger";

// Connect to kafka brokers 
export const kafka = new Kafka({
    logLevel: logLevel.INFO,
    clientId: 'pigeon',
    brokers: config.get("KAFKA_BROKERS"),
});

// Main Worker Class who actually does the main work
abstract class Worker{
    consumer: Consumer | undefined = undefined;

    protected constructor(protected name:string,protected groupId:string,protected topics:string[],protected messageHandler:EachMessageHandler ) {
        this.setupKafkaConsumer()
      
    }

    // Create A Kafak Consumer and subscribes to topics
    async setupKafkaConsumer(){
        try{
            this.consumer = kafka.consumer({ groupId: this.groupId });
            
            await this.consumer.connect()
            logger.info( this.name+ " Consumer connected ")

            await this.consumer.subscribe({topics: this.topics})
            logger.info(this.name+" Consumer subscribed to "+ this.topics);

            await this.consumer.run({eachMessage: this.messageHandler})
            logger.info(this.name+" Consumer started processing messages !!")

        }catch (e) {
            logger.error(e)
        }
    }
}