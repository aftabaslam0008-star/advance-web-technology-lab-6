const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const { getDatabasePool } = require('../src/config/database');
const logger = require('../src/utils/logger');

// Container-level state to distinguish Cold Start vs Warm Start (Lecture 17 Requirement)
let isCold = true;
let containerInitTimestamp = Date.now();
let invocationCount = 0;

// Ensure local invoice storage directory exists
const INVOICE_DIR = path.join(__dirname, '..', 'storage', 'invoices');
if (!fs.existsSync(INVOICE_DIR)) {
  fs.mkdirSync(INVOICE_DIR, { recursive: true });
}

/**
 * Serverless Async Background Worker: PDF Invoice Generator
 * Compatible with AWS Lambda and Vercel Serverless Function signatures
 */
async function generateInvoice(event) {
  const startTime = performance.now();
  invocationCount++;

  // Record whether this execution was a Cold Start or a Warm Start
  const executionIsCold = isCold;
  if (isCold) {
    // Heavy cold-start initialization: load libraries, allocate buffers, warm up DB pool
    isCold = false;
  }

  // Parse payload (supports AWS Lambda event.body, Vercel req.body, or direct payload)
  let payload = event.body;
  if (typeof payload === 'string') {
    try {
      payload = JSON.parse(payload);
    } catch (e) {
      // payload is already an object
    }
  }
  payload = payload || event;

  const { tenantId, orderId, items = [], totalAmount = 0 } = payload;
  const invoiceNumber = `INV-${tenantId ? tenantId.toUpperCase().replace('-', '_') : 'TENANT'}-${orderId || 'ORD'}`;
  const pdfFilename = `${invoiceNumber}.pdf`;
  const pdfFilePath = path.join(INVOICE_DIR, pdfFilename);

  // Generate PDF document in memory using PDFKit
  const pdfBuffer = await new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ margin: 50 });
      const buffers = [];

      doc.on('data', chunk => buffers.push(chunk));
      doc.on('end', () => resolve(Buffer.concat(buffers)));
      doc.on('error', err => reject(err));

      // PDF Header
      doc.fontSize(20).text('TAX INVOICE', { align: 'center' });
      doc.fontSize(10).text('Multi-Tenant Enterprise SaaS Platform', { align: 'center' });
      doc.moveDown(1.5);

      // Invoice & Tenant Metadata
      doc.fontSize(12).text(`Invoice Number: ${invoiceNumber}`, { bold: true });
      doc.text(`Tenant ID: ${tenantId}`);
      doc.text(`Order ID: ${orderId}`);
      doc.text(`Date: ${new Date().toISOString()}`);
      doc.text(`Execution State: ${executionIsCold ? 'COLD START (Container Spin-up)' : 'WARM START (Reused Container)'}`);
      doc.moveDown(1);

      // Table Header
      doc.fontSize(10).text('----------------------------------------------------------------------------------------------------');
      doc.text('Item Description                                  Qty         Unit Price        Subtotal');
      doc.text('----------------------------------------------------------------------------------------------------');

      // Table Rows
      for (const item of items) {
        const desc = (item.productId || 'Item').padEnd(46, ' ');
        const qty = String(item.quantity || 1).padEnd(12, ' ');
        const price = `$${Number(item.unitPrice || 0).toFixed(2)}`.padEnd(18, ' ');
        const sub = `$${Number(item.subtotal || 0).toFixed(2)}`;
        doc.text(`${desc}${qty}${price}${sub}`);
      }

      doc.text('----------------------------------------------------------------------------------------------------');
      doc.moveDown(0.5);
      doc.fontSize(14).text(`Total Amount: $${Number(totalAmount).toFixed(2)}`, { align: 'right' });

      doc.end();
    } catch (error) {
      reject(error);
    }
  });

  // Write file to storage
  fs.writeFileSync(pdfFilePath, pdfBuffer);

  // Persist invoice record to PostgreSQL database scoped to tenant
  try {
    const pool = getDatabasePool();
    // Ensure parent order exists before inserting invoice foreign key
    if (orderId && tenantId) {
      await pool.query(
        `INSERT INTO orders (id, tenant_id, status, total_amount)
         VALUES ($1, $2, 'CONFIRMED', $3)
         ON CONFLICT (id) DO NOTHING;`,
        [orderId, tenantId, totalAmount]
      );
    }
    await pool.query(
      `INSERT INTO invoices (id, tenant_id, order_id, invoice_number, pdf_path, status)
       VALUES ($1, $2, $3, $4, $5, 'GENERATED')
       ON CONFLICT (id) DO NOTHING;`,
      [`inv_${orderId}`, tenantId, orderId, invoiceNumber, pdfFilePath]
    );
  } catch (dbErr) {
    logger.warn('Could not record invoice in database', { error: dbErr.message });
  }

  const executionTimeMs = Number((performance.now() - startTime).toFixed(2));

  return {
    success: true,
    invoiceNumber,
    pdfFilePath,
    pdfSize: pdfBuffer.length,
    isColdStart: executionIsCold,
    executionTimeMs,
    invocationCount,
    containerInitTimestamp
  };
}

/**
 * Standard AWS Lambda Handler Entrypoint
 */
exports.handler = async (event, context) => {
  try {
    const result = await generateInvoice(event);
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(result)
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};

/**
 * Direct export for internal worker dispatch and benchmarking
 */
module.exports = {
  generateInvoice,
  resetColdState: () => { isCold = true; }
};
