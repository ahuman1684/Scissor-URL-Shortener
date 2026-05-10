const router = require('express').Router();
const { getAnalytics, getSummary } = require('../controllers/analyticsController');

// /summary must be declared before /:shortCode to avoid being caught by the param route
router.get('/summary', getSummary);
router.get('/:shortCode', getAnalytics);

module.exports = router;
