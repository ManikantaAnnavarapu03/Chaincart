const {Router} = require('express')
const { handleLogin} = require('../controller/controller.js')

const router = Router()

//router.post('/signup', handleSignup)

router.post('/login', handleLogin)

module.exports = router