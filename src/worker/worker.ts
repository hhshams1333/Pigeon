import {Consumer, EachMessageHandler, Kafka, logLevel} from "kafkajs";
import {logger} from "../../utils/globalLogger";
import { WorkerConfig } from "src/dao";
import path from "path";
import fs from "fs";

export function getConfig() {
    const packageJsonPath = path.resolve(process.cwd(), 'package.json');
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
    const config = packageJson.kafkaPigeonConfig;
  
    if (!config) {
      throw new Error('Configuration for "your-package-name" not found in package.json');
    }
  
    return config;
  }

// Connect to kafka brokers 
export const kafka = new Kafka(getConfig());

// Main Worker Class who actually does the main work
export class Worker{
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

    startMonitoringConsumer(){
        setInterval(async () => {
            await this.checkConsumerLagStatus();
            await this.checkConsumerConnection();
        }, this.consumerMonitoringInterval);
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
    async checkConsumerLagStatus():Promise<any[]> {
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
                            this.restartConsumer()
                            status.push({
                                HOST: m.clientHost,
                                STATE: g.state,
                                MEMBER_ID: m.memberId,
                                GROUP_ID: g.groupId,
                                TOPIC: t,
                                PARTITION: r.partition,
                                OFFSET: r.offset,
                                C_OFFSET: parseInt(currentMapOfTopicOffsetByGroupId[g.groupId][t][parseInt(String(r.partition))]),
                                LAG: lag
                            })
                        }
                    })
                }
            }
        }
        await admin.disconnect()
        return status;
    }
    async checkConsumerConnection():Promise<boolean>{
        try {
            await this.consumer?.describeGroup();
            return true
        } catch (error) {
            logger.info('Consumer disconnected, reconnecting...');
            await this.setupKafkaConsumer();
            return false;
        }
    }
    async restartConsumer():Promise<void>{
        logger.info("Disconnecting consumer from topic "+this.topics+" for "+this.name)
        await this.consumer?.disconnect();
        logger.info("Disconnected consumer from topic "+this.topics+" for "+this.name)
        await this.setupKafkaConsumer();
    }
}