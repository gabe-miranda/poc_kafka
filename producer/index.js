const { Kafka } = require('kafkajs');
const avro = require('avsc');

const kafka = new Kafka({
    clientId: 'node-producer',
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

async function sendMessageTest() {
    const producer = kafka.producer();

    await producer.connect();

    const messageToSend = type.toBuffer({ key: 'xablau', message: 'sei la prt 2534' })
    
    await producer.send({
        topic: 'quickstart',
        messages: [
            { value: messageToSend }
        ],
    })
    
    await producer.disconnect();
}

sendMessageTest();