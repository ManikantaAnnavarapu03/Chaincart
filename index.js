const express = require('express')
const router = require('./routs/router.js')

const port = 7002
const app = express()

app.use(express.json())


app.use('/user',router)

app.use('/order', router)

app.listen(port, ()=> console.log(`server started at ${port}`))
