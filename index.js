import pkg from "kafkajs";
const { Kafka } = pkg;
import dotenv from "dotenv";
dotenv.config();
import { KinesisClient, PutRecordCommand } from "@aws-sdk/client-kinesis";

// Initialize Kinesis Client
const kinesisClient = new KinesisClient({
  region: process.env.AWS_REGION || "us-east-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// Create the Kafka client
const kafka = new Kafka({
  clientId: "push-data-service-" + Date.now(),
  brokers: [
    process.env.KAFKA_BOOTSTRAP_SERVER_URL ||
      "my-cluster-kafka-bootstrap.kafka:9092",
  ],
  sasl: {
    mechanism: "scram-sha-512",
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

let currentTimestamp = Math.floor(Date.now() / 1000);
const consumerData = kafka.consumer({
  groupId: `push-data-service-group-${currentTimestamp}`,
});

const publishToKinesis = async (streamName, partitionKey, data) => {
  const payload = JSON.stringify(data);
  const command = new PutRecordCommand({
    StreamName: streamName,
    PartitionKey: partitionKey,
    Data: Buffer.from(payload),
  });

  try {
    const response = await kinesisClient.send(command);
    console.log("Successfully sent record to Kinesis:", response);
    return response;
  } catch (error) {
    console.error("Error publishing to Kinesis:", error);
    throw error;
  }
};

const run = async () => {
  await consumerData.connect();
  console.info("Connected to Kafka Broker.");
  await consumerData.subscribe({
    topic: process.env.SUBSCRIBE_TOPIC || "input",
    fromBeginning: false,
  });

  consumerData.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        let payLoadParsed = JSON.parse(message.value.toString());
        console.log("Payload:", payLoadParsed);
        if (payLoadParsed) {
          let partitionKey = payLoadParsed?.imeiNo ? payLoadParsed.imeiNo : Math.random().toString(36).substring(2, 15);
          await publishToKinesis(
            process.env.KINESIS_STREAM_NAME || "my-kinesis-stream",
            partitionKey,
            payLoadParsed
          );
        }
      } catch (error) {
        console.error("Error processing message:", error);
      }
    },
  });
};

run().catch((error) => console.error("Run error:", error));

consumerData.on("consumer.crash", function () {
  console.log("Crash detected");
  process.exit(0);
});

consumerData.on("consumer.disconnect", function () {
  console.log("Disconnect detected");
  process.exit(0);
});

consumerData.on("consumer.stop", function () {
  console.log("Stop detected");
  process.exit(0);
});

const errorTypes = ["unhandledRejection"];

errorTypes.map((type) => {
  process.on(type, async (e) => {
    console.log(`process.on ${type}`);
    console.error(e);
    process.exit(0);
  });
});
