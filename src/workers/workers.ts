import {Kafka, logLevel} from "kafkajs";
import config from "config";

export const kafka = new Kafka({
    logLevel: logLevel.INFO,
    clientId: 'pigeon',
    brokers: config.get("KAFKA_BROKERS"),
});

abstract class Worker{

    protected constructor(name:string,groupId:string,topics:string[] ) {
      
    }
}