const { Kafka } = require('kafkajs');
const fs = require('fs');

const kafka = new Kafka({
    clientId: 'node-consumer',
    brokers: ['localhost:9092']
});

async function readTestMessage() {
    const consumer = kafka.consumer({ groupId: 'test-group' });

    await consumer.connect();
    await consumer.subscribe({ topic: 'files', fromBeginning:"true" });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const fileBuffer = message.value;

            fs.writeFileSync('test.pdf', fileBuffer);
        }
    });
}

readTestMessage();