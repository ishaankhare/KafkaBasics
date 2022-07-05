const ip = require('ip')
const { Kafka, logLevel } = require('kafkajs')

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.NOTHING,
  brokers: [`${host}:9092`],
  clientId: 'example1',
})

const topic = 'topic-test'

const producer = kafka.producer()

const run = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
}

const sendMessage = async () => {
  //await producer.connect()
  await producer.send({
    topic,
    messages: [
      { value: `Hello KafkaJS user!: ${new Date().toTimeString()}` },
    ],
  })
  console.log("Message sent");
}

run();




