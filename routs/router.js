const {Router} = require('express')
const { handleLogin, handleSignup, handlePostOrderDetails, handleGetOrderDetails} = require('../controller/controller.js')

const router = Router()

router.post('/signup', handleSignup)

router.post('/login', handleLogin)

router.post('/data', handlePostOrderDetails)

router.get('/data', handleGetOrderDetails)

module.exports = router