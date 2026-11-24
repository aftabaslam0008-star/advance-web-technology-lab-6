const express = require('express');
const router = express.Router();
const productService = require('../services/productService');

/**
 * GET /api/v1/products
 * Retrieves inventory strictly for the active tenant
 */
router.get('/', async (req, res, next) => {
  try {
    const products = await productService.getProducts(req.tenantId);
    res.json({
      tenantId: req.tenantId,
      count: products.length,
      products
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
