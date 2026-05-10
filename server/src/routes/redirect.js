const router = require('express').Router();
const { redirect } = require('../controllers/redirectController');

router.get('/:shortCode', redirect);

module.exports = router;
