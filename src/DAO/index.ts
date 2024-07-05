import { EachMessageHandler } from "kafkajs";

export type WorkerConfig = {
  name: string;
  groupId: string;
  topics: string[];
  messageHandler: EachMessageHandler;
  consumerMonitoringInterval?: number; // default 2000 ms
  lagTreshold:number; // default 20
};
