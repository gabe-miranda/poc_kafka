const { Kafka } = require('kafkajs');
const avro = require('avsc');

const kafka = new Kafka({
    clientId: 'node-consumer',
    brokers: ['localhost:9092']
});

const type = avro.Type.forSchema({
    type: 'record',
    name: 'msg',
    fields: [
        { name: 'key', type: 'string' },
        { name: 'message', type: 'string' }
    ]
});

async function readTestMessage() {
    const consumer = kafka.consumer({ groupId: 'test-group' });

    await consumer.connect();
    await consumer.subscribe({ topic: 'quickstart', fromBeginning:"true" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const msgReceived = type.fromBuffer(message.value);
            console.log(msgReceived);
            // console.log({ value: message.value.toString() })
            // console.log({
            //     key: message.key,
            //     value: message.value.toString(),
            //     headers: message.headers,
            //     topic: topic
            // })
            // console.log(message);
        }
    });
}

readTestMessage();