const logger = require('../utils/logger');

function errorHandler(err, req, res, next) {
  logger.error('Unhandled application error', {
    message: err.message,
    stack: process.env.NODE_ENV === 'development' ? err.stack : undefined,
    path: req.path,
    tenantId: req.tenantId
  });

  const statusCode = err.statusCode || (err.message.includes('NOT_FOUND') ? 404 : 500);

  res.status(statusCode).json({
    error: err.name || 'INTERNAL_SERVER_ERROR',
    message: err.message || 'An unexpected server error occurred.',
    timestamp: new Date().toISOString()
  });
}

module.exports = errorHandler;
