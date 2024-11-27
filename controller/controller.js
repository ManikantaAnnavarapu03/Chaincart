const {Kafka, Partitioners} = require('kafkajs')
const jwt = require('jsonwebtoken')
const name = "mani"

const kafka = new Kafka({
    clientId:'Manikanta03',
    brokers:['localhost:9092']
})
const secret = "manikanta@03"

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
        await consumer.subscribe({topic:'user-ress-topic'})
        await consumer.run({
            eachMessage: async({topic, partition, message}) =>{
                const userMessage = JSON.parse(message.value.toString())
                if (!res.headersSent){
                    if(userMessage == "user valid"){
                        const payload = {user_name:body.username, role:"developer"}
                        options ={expriresIn:'1h'}
                        const token = await jwt.sign(payload, secret, options)

                        res.status(200).json({Access_Token:token})
                        consumer.disconnect()
                    }
                    else if(userMessage == "user not found"){
                        res.status(404).json({message:userMessage})
                        consumer.disconnect()
                    }
                    
                }
            }
        })
        
//.then(async () => await consumer.disconnect())
        
    }catch(error){
        console.log(error)
    }
    finally{
        console.log("process completed")
    }
}

async function handleSignup(req, res){
    try{
        const producer = kafka.producer({createPartitioner: Partitioners.LegacyPartitioner})
        const body = req.body
        await producer.connect()
        console.log("producer connected")
        await producer.send({
            topic:'user-reqq-topic',
            messages:[{
                key:'signup',value:JSON.stringify({action:'signup', data:body})
            }]
        })
        await producer.disconnect()
        console.log("producer disconnected")
        const consumer = kafka.consumer({groupId:'user-ress-group'})
        await consumer.connect()
        console.log("consumer connected")
        await consumer.subscribe({topic:'user-ress-topic'})
        await consumer.run({
            eachMessage: async({topic, partition, message}) =>{
                const userMessage = JSON.parse(message.value.toString())
                if (!res.headersSent){
                    if(userMessage == "user created"){
                        res.status(201).json({message:userMessage})
                        consumer.disconnect()
                    }
                    else if(userMessage == "user not created"){
                        res.status(400).json({message:userMessage})
                        consumer.disconnect()
                    }
                    
                }
            }
        })
        
//.then(async () => await consumer.disconnect())
        
    }
    finally{
        console.log("process completed")
    }
}

async function handlePostOrderDetails(req, res){
    try{
    const data = req.body
   const producer = kafka.producer({createPartitioner:Partitioners.LegacyPartitioner})
   await producer.connect()
   console.log("producer connected")
   await producer.send({
    topic:'order-req-topic',
    messages:[{key:"post", value:JSON.stringify({action:'post', data:data})}]
   })
   await producer.disconnect()
   const consumer = kafka.consumer({groupId:'order-res-group'})
   await consumer.connect()
   await consumer.subscribe({topic:'order-res-topic'})
   await consumer.run({
    eachMessage: async ({topic, partition, message}) =>{
        const userMessage = JSON.parse(message.value.toString())
        console.log(userMessage)
        if(!res.headersSent){
            if(userMessage == "order created"){
                res.status(201).json({message:userMessage})
                consumer.disconnect()
            }
            else{
                res.status(400).json({message:'order not created'})
            }
        }
    }
   })
}catch(error){
    console.log(error)
}
}


async function handleGetOrderDetails(req, res){
    try{
        const data = req.body
       const producer = kafka.producer({createPartitioner:Partitioners.LegacyPartitioner})
       await producer.connect()
       console.log("producer connected")
       await producer.send({
        topic:'order-req-topic',
        messages:[{key:"post", value:JSON.stringify({action:'get', data:"get user details"})}]
       })
       await producer.disconnect()
       const consumer = kafka.consumer({groupId:'order-res-group'})
       await consumer.connect()
       await consumer.subscribe({topic:'order-res-topic'})
       await consumer.run({
        eachMessage: async ({topic, partition, message}) =>{
            const userMessage = JSON.parse(message.value.toString())
            console.log(userMessage)
            if(!res.headersSent){
                res.status(200).json({data:userMessage})
                consumer.disconnect()
            }
        }
       })
    }catch(error){
        console.log(error)
    }

}

module.exports = {handleLogin, handleSignup, handlePostOrderDetails, handleGetOrderDetails}