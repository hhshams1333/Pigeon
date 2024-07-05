import {Consumer, EachMessageHandler, Kafka, logLevel} from "kafkajs";
import config from "config";
import {logger} from "../../utils/globalLogger";
import { WorkerConfig } from "src/DAO";

// Connect to kafka brokers 
export const kafka = new Kafka({
    logLevel: logLevel.INFO,
    clientId: 'pigeon',
    brokers: config.get("KAFKA_BROKERS"),
});

// Main Worker Class who actually does the main work
abstract class Worker{
    consumer: Consumer | undefined = undefined;
    protected name : string; 
    protected topics: string[];
    protected groupId: string;
    protected lagTreshold : number;
    protected messageHandler: EachMessageHandler;
    protected consumerMonitoringInterval: number | undefined 

    protected constructor(protected workerConfig : WorkerConfig ) {
        this.setupKafkaConsumer();
        this.startMonitoringConsumer()
        const { consumerMonitoringInterval,lagTreshold,messageHandler,groupId,topics,name  } = workerConfig
        this.consumerMonitoringInterval = consumerMonitoringInterval || 2000;
        this.messageHandler = messageHandler;
        this.lagTreshold = lagTreshold || 20
        this.groupId = groupId
        this.topics = topics
        this.name = name
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
    startMonitoringConsumer(){
        setInterval(async () => {
            await this.checkConsumerLag();
            await this.checkConsumerConnection();
        }, this.consumerMonitoringInterval);
    }
   async checkConsumerLag() {
        const status:any[] = []
        const admin = kafka.admin()
        await admin.connect()
        const groups = await admin.listGroups()
        const groupsNames = await groups.groups.map(x => x.groupId)
        const gd = await admin.describeGroups(groupsNames)
        let currentMapOfTopicOffsetByGroupId: any = {}
        for (const g of gd.groups) {
            const topicOffset = await admin.fetchOffsets({ groupId: g.groupId })
            if (currentMapOfTopicOffsetByGroupId[g.groupId] == undefined) {
                currentMapOfTopicOffsetByGroupId[g.groupId] = {}
            }
            topicOffset.forEach(to => {
                to.partitions.forEach(p => {
                    if (currentMapOfTopicOffsetByGroupId[g.groupId][to.topic] == undefined) {
                        currentMapOfTopicOffsetByGroupId[g.groupId][to.topic] = {}
                    }
                    currentMapOfTopicOffsetByGroupId[g.groupId][to.topic][parseInt(String(p.partition))] = p.offset
                })
            })

            for (const m of g.members) {
                for (const t of this.topics) {
                    const res = await admin.fetchTopicOffsets(t)
                    res.forEach(r => {
                        if (currentMapOfTopicOffsetByGroupId[g.groupId][t] !== undefined) {
                            const  lag = parseInt(r.high) - parseInt(currentMapOfTopicOffsetByGroupId[g.groupId][t][parseInt(String(r.partition))])
                           if(lag > this.lagTreshold)
                                this.restartKafkaConsumer()
                        }
                    })
                }
            }
        }
        await admin.disconnect()
    }
    async checkConsumerConnection(){
        try {
            await this.consumer?.describeGroup();
        } catch (error) {
            console.log('Consumer disconnected, reconnecting...');
            await this.setupKafkaConsumer();
        }
    }
    async restartKafkaConsumer(){
        logger.info("Disconnecting consumer from topic "+this.topics+" for "+this.name)
        await this.consumer?.disconnect();
        logger.info("Disconnected consumer from topic "+this.topics+" for "+this.name)
        await this.setupKafkaConsumer();
    }
}