const { Kafka } = require('kafkajs');
const fs = require('fs');

const kafka = new Kafka({
    clientId: 'node-producer',
    brokers: ['localhost:9092']
});

async function sendMessageFile() {
    const producer = kafka.producer();

    await producer.connect();

    const filePath = 'swift/305431296.pdf';

    const fileBuffer = readSwiftFile(filePath);
    
    await producer.send({
        topic: 'files',
        messages: [
            { value: fileBuffer }
        ],
    })
    
    await producer.disconnect();
}

function readSwiftFile(path) {
    return fs.readFileSync(path);
}

sendMessageFile()