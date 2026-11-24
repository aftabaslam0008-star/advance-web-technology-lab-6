const { generateInvoice } = require('../invoiceWorker');

/**
 * Vercel Serverless Function entrypoint
 * Accepts POST /api/generate-invoice
 */
module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed. Use POST.' });
  }

  try {
    const result = await generateInvoice({ body: req.body });
    return res.status(200).json(result);
  } catch (error) {
    return res.status(500).json({
      error: 'SERVERLESS_INVOCATION_FAILED',
      message: error.message
    });
  }
};
