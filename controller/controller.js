const {Kafka, Partitioners} = require('kafkajs')
const name = "mani"

const kafka = new Kafka({
    clientId:'Manikanta03',
    brokers:['localhost:9092']
})

async function handleLogin(req, res){
    try{
        const producer = kafka.producer({createPartitioner: Partitioners.LegacyPartitioner})
        const body = req.body
        await producer.connect()
        console.log("producer connected")
        await producer.send({
            topic:'user-reqq-topic',
            messages:[{
                key:'login',value:JSON.stringify({action:'login', data:body})
            }]
        })
        await producer.disconnect()
        console.log("producer disconnected")
        const consumer = kafka.consumer({groupId:'user-ress-group'})
        await consumer.connect()
        console.log("consumer connected")
        await consumer.subscribe({topic:'user-ress-topic', fromBeginning:true})
        await consumer.run({
            eachMessage: async({topic, partition, message}) =>{
                const userMessage = JSON.parse(message.value.toString())
                if (!res.headersSent){
                    res.status(200).json({message:userMessage[0]})
                    consumer.disconnect()
                }
            }
        })
        
//.then(async () => await consumer.disconnect())
        
    }
    finally{
        console.log("process completed")
    }
}

module.exports = {handleLogin}