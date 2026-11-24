const jwt = require('jsonwebtoken');
const config = require('../config');

/**
 * Generates a signed JWT with tenant context claims
 */
function generateTenantToken(userId, tenantId, role = 'MEMBER', expiresIn = '12h') {
  const payload = {
    sub: userId,
    userId,
    tenant_id: tenantId,
    tenantId: tenantId, // both snake_case and camelCase for compatibility
    role
  };

  return jwt.sign(payload, config.jwtSecret, { expiresIn });
}

/**
 * Verifies and decodes JWT token
 */
function verifyToken(token) {
  try {
    return jwt.verify(token, config.jwtSecret);
  } catch (err) {
    return null;
  }
}

module.exports = {
  generateTenantToken,
  verifyToken
};
