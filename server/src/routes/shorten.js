const router = require('express').Router();
const { shorten } = require('../controllers/shortenController');
const rateLimiter = require('../middleware/rateLimiter');

router.post('/', rateLimiter(10, 60000), shorten);

module.exports = router;
