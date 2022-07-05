const ip = require('ip');
const fs = require('fs');
const AWS = require('aws-sdk');

const { Kafka, logLevel } = require('kafkajs')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9092`],
  clientId: 'example1',
})

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group' })

//Initialize AWS
const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
});

const receiveMessage = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      fs.appendFileSync("./textcons.txt", "\n")
      fs.appendFileSync("./textcons.txt", message.value.toString());  

      //Upload this file to AWS, it will get updated as it is a default setting in S3.

      //Declare AWS params
      const params = {
        Bucket: 'jai-sync-test',
        Key: 'kafka testing/textcons.txt',
        //Body: ''
      };

      //Upload file to AWS
      s3.upload(params);

      console.log({
        value: message.value.toString(),
      })
    },
  })
  console.log("message received")
}

receiveMessage();
