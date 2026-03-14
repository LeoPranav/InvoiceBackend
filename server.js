const http = require("http");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "0.0.0.0";
const INDEX_PATH = path.join(__dirname, "index.html");
const MAX_BODY_SIZE = 15 * 1024 * 1024;
const TARGET_CURRENCY = "USD";
const INR_TO_USD_RATE = Number(process.env.INR_TO_USD_RATE || 0.012);
const EUR_TO_USD_RATE = Number(process.env.EUR_TO_USD_RATE || 1.09);
const GBP_TO_USD_RATE = Number(process.env.GBP_TO_USD_RATE || 1.28);
const AUD_TO_USD_RATE = Number(process.env.AUD_TO_USD_RATE || 0.66);
const CAD_TO_USD_RATE = Number(process.env.CAD_TO_USD_RATE || 0.74);
const JPY_TO_USD_RATE = Number(process.env.JPY_TO_USD_RATE || 0.0067);
const CNY_TO_USD_RATE = Number(process.env.CNY_TO_USD_RATE || 0.14);
const SGD_TO_USD_RATE = Number(process.env.SGD_TO_USD_RATE || 0.74);
const AED_TO_USD_RATE = Number(process.env.AED_TO_USD_RATE || 0.27);
const CHF_TO_USD_RATE = Number(process.env.CHF_TO_USD_RATE || 1.11);
const CURRENCY_TOKEN = "(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\\.?|₹|â‚¹|\\$|£|€|¥)?";
const USD_EXCHANGE_RATES = {
  USD: 1,
  INR: INR_TO_USD_RATE,
  EUR: EUR_TO_USD_RATE,
  GBP: GBP_TO_USD_RATE,
  AUD: AUD_TO_USD_RATE,
  CAD: CAD_TO_USD_RATE,
  JPY: JPY_TO_USD_RATE,
  CNY: CNY_TO_USD_RATE,
  SGD: SGD_TO_USD_RATE,
  AED: AED_TO_USD_RATE,
  CHF: CHF_TO_USD_RATE
};

const server = http.createServer(async (req, res) => {
  try {
    // CORS HEADERS
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    // Handle preflight request
    if (req.method === "OPTIONS") {
      res.writeHead(204);
      return res.end();
    }
    const requestUrl = new URL(req.url, `http://${req.headers.host || "localhost"}`);

    if (req.method === "GET" && requestUrl.pathname === "/") {
      return serveIndex(res);
    }

    if (req.method === "GET" && requestUrl.pathname === "/health") {
      return sendJson(res, 200, {
        ok: true,
        mode: "rule-based",
        timestamp: new Date().toISOString()
      });
    }

    if (req.method === "POST" && requestUrl.pathname === "/api/anthropic/messages") {
      return handleAudit(req, res);
    }

    sendJson(res, 404, { error: { message: "Route not found" } });
  } catch (error) {
    console.error("Unhandled server error:", error);
    sendJson(res, 500, { error: { message: "Internal server error" } });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`AuditBlox backend running at http://localhost:${PORT}`);
});

function serveIndex(res) {
  fs.readFile(INDEX_PATH, "utf8", (error, html) => {
    if (error) {
      console.error("Failed to read index.html:", error);
      return sendJson(res, 500, { error: { message: "Unable to load frontend" } });
    }

    const rewrittenHtml = html.replaceAll(
      "https://api.anthropic.com/v1/messages",
      "/api/anthropic/messages"
    );

    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store"
    });
    res.end(rewrittenHtml);
  });
}

async function handleAudit(req, res) {
  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    return sendJson(res, error.statusCode || 400, {
      error: { message: error.message || "Invalid request body" }
    });
  }

  const messageContent = body?.messages?.[0]?.content;
  if (!Array.isArray(messageContent) || messageContent.length === 0) {
    return sendJson(res, 400, {
      error: { message: "Request must include a user message with content." }
    });
  }

  try {
    const audit = analyzeInvoiceContent(messageContent);
    sendJson(res, 200, {
      id: `local-rule-${Date.now()}`,
      type: "message",
      role: "assistant",
      model: "local-rule-engine-v1",
      content: [
        {
          type: "text",
          text: JSON.stringify(audit)
        }
      ],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: {
        input_tokens: 0,
        output_tokens: 0
      }
    });
  } catch (error) {
    console.error("Rule-based audit error:", error);
    sendJson(res, 500, {
      error: {
        message: error.message || "Failed to audit invoice."
      }
    });
  }
}

function analyzeInvoiceContent(contentParts) {
  const filePart = contentParts.find((part) => part && (part.type === "document" || part.type === "image"));
  if (!filePart?.source?.data) {
    throw new Error("No uploaded invoice file was found in the request.");
  }

  const mediaType = filePart.source.media_type || "application/octet-stream";
  const buffer = Buffer.from(filePart.source.data, "base64");

  if (filePart.type === "image") {
    return buildImageFallback(mediaType);
  }

  if (mediaType.includes("pdf")) {
    const extractedText = extractPdfText(buffer);
    if (!extractedText.trim()) {
      return buildUnreadableFallback("PDF text could not be extracted. This backend works best with text-based PDFs.");
    }

    return buildAuditFromText(extractedText, {
      mediaType,
      source: "pdf",
      seededLineItems: extractStructuredPdfLineItems(buffer)
    });
  }

  if (isSpreadsheetMediaType(mediaType)) {
    const spreadsheetPayload = extractSpreadsheetPayload(buffer, mediaType);
    const structuredAudit = buildAuditFromSpreadsheetPayload(spreadsheetPayload, mediaType);
    if (structuredAudit) {
      return structuredAudit;
    }

    if (!spreadsheetPayload.text.trim()) {
      return buildUnreadableFallback("Spreadsheet data could not be extracted. Try saving the file as .xlsx or .csv.");
    }

    return buildAuditFromText(spreadsheetPayload.text, { mediaType, source: "spreadsheet" });
  }

  const plainText = buffer.toString("utf8");
  return buildAuditFromText(plainText, { mediaType, source: "text" });
}

function isSpreadsheetMediaType(mediaType) {
  return /spreadsheet|excel|csv|sheet/i.test(mediaType);
}

function extractSpreadsheetText(buffer, mediaType) {
  return extractSpreadsheetPayload(buffer, mediaType).text;
}

function extractSpreadsheetPayload(buffer, mediaType) {
  if (/csv|text\//i.test(mediaType)) {
    const rows = parseDelimitedRows(buffer.toString("utf8"));
    return { rows, text: rowsToAuditText(rows) };
  }

  if (/sheet|openxml|xlsx/i.test(mediaType)) {
    return extractXlsxPayload(buffer);
  }

  if (/ms-excel|xls/i.test(mediaType)) {
    const plainText = buffer.toString("utf8");
    if (looksLikePlainText(plainText)) {
      const rows = parseDelimitedRows(plainText);
      return { rows, text: rowsToAuditText(rows) };
    }

    return { rows: [], text: "" };
  }

  return { rows: [], text: "" };
}

function extractCsvText(text) {
  return rowsToAuditText(parseDelimitedRows(text));
}

function parseDelimitedRows(text) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        i += 1;
      }
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      continue;
    }

    cell += char;
  }

  if (cell.length || row.length) {
    row.push(cell);
    rows.push(row);
  }

  return rows;
}

function extractXlsxText(buffer) {
  return extractXlsxPayload(buffer).text;
}

function extractXlsxPayload(buffer) {
  const zipEntries = unzipEntries(buffer);
  const sharedStrings = parseSharedStrings(zipEntries.get("xl/sharedStrings.xml") || "");
  const worksheetNames = [...zipEntries.keys()]
    .filter((name) => /^xl\/worksheets\/sheet\d+\.xml$/i.test(name))
    .sort();

  const rows = [];
  for (const worksheetName of worksheetNames) {
    const worksheetXml = zipEntries.get(worksheetName) || "";
    rows.push(...parseWorksheetRows(worksheetXml, sharedStrings));
  }

  return {
    rows,
    text: rowsToAuditText(rows)
  };
}

function unzipEntries(buffer) {
  const entries = new Map();
  let offset = 0;

  while (offset + 30 <= buffer.length) {
    const signature = buffer.readUInt32LE(offset);
    if (signature !== 0x04034b50) {
      offset += 1;
      continue;
    }

    const compressionMethod = buffer.readUInt16LE(offset + 8);
    const compressedSize = buffer.readUInt32LE(offset + 18);
    const fileNameLength = buffer.readUInt16LE(offset + 26);
    const extraFieldLength = buffer.readUInt16LE(offset + 28);
    const fileNameStart = offset + 30;
    const fileNameEnd = fileNameStart + fileNameLength;
    const fileName = buffer.toString("utf8", fileNameStart, fileNameEnd);
    const dataStart = fileNameEnd + extraFieldLength;
    const dataEnd = dataStart + compressedSize;

    if (dataEnd > buffer.length) {
      break;
    }

    const compressed = buffer.subarray(dataStart, dataEnd);
    let content = "";

    try {
      if (compressionMethod === 0) {
        content = compressed.toString("utf8");
      } else if (compressionMethod === 8) {
        content = zlib.inflateRawSync(compressed).toString("utf8");
      }
    } catch {}

    if (content) {
      entries.set(fileName, content);
    }

    offset = dataEnd;
  }

  return entries;
}

function parseSharedStrings(xml) {
  const strings = [];
  const siRegex = /<si[\s\S]*?>([\s\S]*?)<\/si>/g;
  let match;

  while ((match = siRegex.exec(xml)) !== null) {
    const textParts = [];
    const textRegex = /<t[^>]*>([\s\S]*?)<\/t>/g;
    let textMatch;

    while ((textMatch = textRegex.exec(match[1])) !== null) {
      textParts.push(decodeXmlEntities(textMatch[1]));
    }

    strings.push(textParts.join(""));
  }

  return strings;
}

function parseWorksheetRows(xml, sharedStrings) {
  const rows = [];
  const rowRegex = /<row\b[\s\S]*?>([\s\S]*?)<\/row>/g;
  let rowMatch;

  while ((rowMatch = rowRegex.exec(xml)) !== null) {
    const cells = [];
    const cellRegex = /<c\b([^>]*)>([\s\S]*?)<\/c>/g;
    let cellMatch;

    while ((cellMatch = cellRegex.exec(rowMatch[1])) !== null) {
      const attrs = cellMatch[1];
      const body = cellMatch[2];
      const refMatch = attrs.match(/\br="([A-Z]+)\d+"/i);
      const typeMatch = attrs.match(/\bt="([^"]+)"/i);
      const inlineMatch = body.match(/<is>[\s\S]*?<t[^>]*>([\s\S]*?)<\/t>[\s\S]*?<\/is>/i);
      const valueMatch = body.match(/<v>([\s\S]*?)<\/v>/i);
      const columnIndex = refMatch ? columnLettersToIndex(refMatch[1]) : cells.length;
      const cellType = typeMatch?.[1] || "";
      let value = "";

      if (inlineMatch?.[1]) {
        value = decodeXmlEntities(inlineMatch[1]);
      } else if (valueMatch?.[1]) {
        const rawValue = decodeXmlEntities(valueMatch[1]);
        value =
          cellType === "s"
            ? sharedStrings[Number(rawValue)] || ""
            : rawValue;
      }

      cells[columnIndex] = normalizeCellValue(value);
    }

    rows.push(cells.map((cell) => normalizeCellValue(cell)));
  }

  return rows;
}

function columnLettersToIndex(letters) {
  let index = 0;
  for (const char of letters.toUpperCase()) {
    index = index * 26 + (char.charCodeAt(0) - 64);
  }
  return Math.max(0, index - 1);
}

function decodeXmlEntities(value) {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#10;/g, "\n")
    .replace(/&#13;/g, "\r");
}

function normalizeCellValue(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function buildAuditFromSpreadsheetPayload(payload, mediaType) {
  const rows = payload?.rows || [];
  if (rows.length < 2) {
    return null;
  }

  const headers = rows[0].map(normalizeHeaderLabel);
  if (!headers.some(Boolean) || !headers.includes("Description")) {
    return null;
  }

  const structuredRows = rows
    .slice(1)
    .map((row) => spreadsheetRowToObject(headers, row))
    .filter((row) => Object.keys(row).length > 0);

  if (!structuredRows.length) {
    return null;
  }

  const uniqueInvoices = [...new Set(structuredRows.map((row) => row.invoiceNumber).filter(Boolean))];
  const uniqueVendors = [...new Set(structuredRows.map((row) => row.vendorName).filter(Boolean))];

  const sourceCurrency = detectSourceCurrency(payload.text || "");
  const conversionRate = getUsdConversionRate(sourceCurrency);
  const lineItems = convertLineItemsToCurrency(
    structuredRows.map((row) => ({
      description: row.description || "Line item",
      hsn_code: findHsnInText(row.description || ""),
      quantity: sanitizeNumber(row.quantity) || 1,
      unit: row.unit || "unit",
      unit_price: sanitizeNumber(row.unitPrice),
      amount: sanitizeNumber(row.amount),
      gst_rate: sanitizeNumber(row.gstRate),
      gst_amount: sanitizeNumber(row.gstAmount)
    })),
    conversionRate
  );

  const subtotal = convertAmount(
    structuredRows.reduce((sum, row) => sum + (row.subtotal ?? row.amount ?? 0), 0),
    conversionRate
  );
  const totalGst = convertAmount(
    structuredRows.reduce((sum, row) => sum + (row.gstAmount ?? 0), 0),
    conversionRate
  );
  const totalAmount = convertAmount(
    structuredRows.reduce(
      (sum, row) => sum + (row.totalAmount ?? ((row.subtotal ?? row.amount ?? 0) + (row.gstAmount ?? 0))),
      0
    ),
    conversionRate
  );

  const discrepancies = [];
  for (const item of lineItems) {
    const expectedAmount = round2(sanitizeNumber(item.quantity) * sanitizeNumber(item.unit_price));
    if (expectedAmount > 0 && Math.abs(expectedAmount - sanitizeNumber(item.amount)) > 1) {
      discrepancies.push({
        type: "calculation_error",
        severity: differenceSeverity(Math.abs(expectedAmount - sanitizeNumber(item.amount))),
        line_item: item.description,
        description: `${item.description}: quantity x unit price is ${formatMoney(expectedAmount, TARGET_CURRENCY)}, but billed amount is ${formatMoney(item.amount, TARGET_CURRENCY)}.`,
        billed_amount: item.amount,
        correct_amount: expectedAmount,
        overcharge: round2(item.amount - expectedAmount)
      });
    }

    if (item.gst_rate > 0) {
      const expectedGst = round2((sanitizeNumber(item.amount) * sanitizeNumber(item.gst_rate)) / 100);
      if (Math.abs(expectedGst - sanitizeNumber(item.gst_amount)) > 1) {
        discrepancies.push({
          type: "gst_mismatch",
          severity: differenceSeverity(Math.abs(expectedGst - sanitizeNumber(item.gst_amount))),
          line_item: item.description,
          description: `${item.description}: GST at ${item.gst_rate}% should be ${formatMoney(expectedGst, TARGET_CURRENCY)}, but billed GST is ${formatMoney(item.gst_amount, TARGET_CURRENCY)}.`,
          billed_amount: item.gst_amount,
          correct_amount: expectedGst,
          overcharge: round2(item.gst_amount - expectedGst)
        });
      }
    }
  }

  const totalOvercharge = round2(
    discrepancies.reduce((sum, item) => sum + Math.max(0, sanitizeNumber(item.overcharge)), 0)
  );

  const firstRow = structuredRows[0];
  return {
    vendor_name:
      uniqueVendors.length === 1
        ? uniqueVendors[0]
        : uniqueVendors.length > 1
          ? `Spreadsheet Summary (${uniqueVendors.length} vendors)`
          : "Spreadsheet Summary",
    invoice_number:
      uniqueInvoices.length === 1
        ? uniqueInvoices[0]
        : uniqueInvoices.length > 1
          ? `${uniqueInvoices.length} invoices`
          : firstRow.invoiceNumber || "",
    invoice_date: firstRow.invoiceDate || "",
    due_date: firstRow.dueDate || "",
    vendor_gstin: null,
    buyer_gstin: null,
    line_items: lineItems,
    subtotal: round2(subtotal),
    total_gst: round2(totalGst),
    total_amount: round2(totalAmount),
    currency: TARGET_CURRENCY,
    discrepancies,
    total_overcharge: totalOvercharge,
    correct_total: round2(totalAmount - totalOvercharge),
    audit_summary:
      uniqueInvoices.length > 1
        ? `Spreadsheet summary generated for ${uniqueInvoices.length} invoices across ${structuredRows.length} rows.`
        : buildSummary(discrepancies, "spreadsheet"),
    confidence_score: Math.min(0.95, round2(0.55 + Math.min(lineItems.length, 20) * 0.02)),
    extraction_notes: `Source=spreadsheet; mediaType=${mediaType}; rows=${structuredRows.length}; uniqueInvoices=${uniqueInvoices.length}. Structured spreadsheet parsing was used.`
  };
}

function spreadsheetRowToObject(headers, row) {
  const record = {};

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    const value = normalizeCellValue(row[i]);
    if (!header || !value) {
      continue;
    }

    switch (header) {
      case "Vendor":
        record.vendorName = value;
        break;
      case "Invoice Number":
        record.invoiceNumber = value;
        break;
      case "Invoice Date":
        record.invoiceDate = normalizeSpreadsheetDate(value);
        break;
      case "Due Date":
        record.dueDate = normalizeSpreadsheetDate(value);
        break;
      case "Description":
        record.description = value;
        break;
      case "Quantity":
        record.quantity = toNumber(value);
        break;
      case "Unit Price":
        record.unitPrice = toNumber(value);
        break;
      case "GST Rate":
        record.gstRate = toNumber(value);
        break;
      case "Amount":
        record.amount = toNumber(value);
        break;
      case "Sales Tax":
        record.gstAmount = toNumber(value);
        break;
      case "Subtotal":
        record.subtotal = toNumber(value);
        break;
      case "Total Due":
        record.totalAmount = toNumber(value);
        break;
      default:
        break;
    }
  }

  if (record.amount == null && record.subtotal != null) {
    record.amount = record.subtotal;
  }

  return record;
}

function normalizeSpreadsheetDate(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return "";
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed) || /^\d{1,2}[./-]\d{1,2}[./-]\d{2,4}$/.test(trimmed)) {
    return trimmed;
  }

  const numeric = Number(trimmed);
  if (Number.isFinite(numeric) && numeric > 20000 && numeric < 70000) {
    return excelSerialToIsoDate(numeric);
  }

  return trimmed;
}

function excelSerialToIsoDate(serial) {
  const epoch = Date.UTC(1899, 11, 30);
  const millis = epoch + Math.round(serial) * 24 * 60 * 60 * 1000;
  return new Date(millis).toISOString().slice(0, 10);
}

function rowsToAuditText(rows) {
  const cleanedRows = rows
    .map((row) => row.map((cell) => normalizeCellValue(cell)))
    .filter((row) => row.some(Boolean));

  if (!cleanedRows.length) {
    return "";
  }

  const headerRow = cleanedRows[0];
  const normalizedHeaders = headerRow.map(normalizeHeaderLabel);
  const hasStructuredHeaders = normalizedHeaders.some(Boolean);

  if (!hasStructuredHeaders) {
    return cleanedRows
      .map((row) => row.filter(Boolean).join(" "))
      .filter(Boolean)
      .join("\n");
  }

  const lines = [];
  for (const row of cleanedRows.slice(1)) {
    const pairs = [];
    for (let i = 0; i < normalizedHeaders.length; i++) {
      const header = normalizedHeaders[i];
      const value = row[i];
      if (header && value) {
        pairs.push(`${header} ${value}`);
      }
    }

    const itemLine = buildSpreadsheetItemLine(normalizedHeaders, row);
    if (itemLine) {
      lines.push(itemLine);
    }

    lines.push(...pairs);
  }

  return lines.filter(Boolean).join("\n");
}

function normalizeHeaderLabel(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) {
    return "";
  }

  const normalized = raw
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (/vendor/.test(normalized)) return "Vendor";
  if (/invoice\s*(id|number|#|no)\b/.test(normalized)) return "Invoice Number";
  if (/invoice\s*date|^date$/.test(normalized)) return "Invoice Date";
  if (/due/.test(normalized)) return "Due Date";
  if (/description|product|item/.test(normalized)) return "Description";
  if (/quantity|qty/.test(normalized)) return "Quantity";
  if (/unit\s*price|price\s*per\s*unit|price\b|rate\b/.test(normalized)) return "Unit Price";
  if (/(gst|tax|vat).*(%|rate)|(%|rate).*(gst|tax|vat)/.test(normalized)) return "GST Rate";
  if (/subtotal|net\s*amount/.test(normalized)) return "Subtotal";
  if (/sales\s*tax|tax\s*amount|gst\s*amount|vat\s*amount/.test(normalized)) return "Sales Tax";
  if (/shipping|freight|handling/.test(normalized)) return "Shipping";
  if (/total\s*due|grand\s*total|total\s*amount|amount\s*due/.test(normalized)) return "Total Due";
  if (/amount|line\s*total|total\s*price/.test(normalized)) return "Amount";
  return "";
}

function buildSpreadsheetItemLine(headers, row) {
  const description = findCellByHeader(headers, row, "Description");
  const quantity = findCellByHeader(headers, row, "Quantity");
  const unitPrice = findCellByHeader(headers, row, "Unit Price");
  const amount = findCellByHeader(headers, row, "Amount");

  if (!description || !amount || (!quantity && !unitPrice)) {
    return "";
  }

  return [description, quantity, "unit", unitPrice, amount]
    .filter(Boolean)
    .join(" ");
}

function findCellByHeader(headers, row, headerName) {
  const index = headers.findIndex((header) => header === headerName);
  return index === -1 ? "" : normalizeCellValue(row[index]);
}

function buildAuditFromText(rawText, metadata) {
  const normalizedText = normalizeText(rawText);
  const rawLines = normalizedText
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const lines = sanitizeLines(rawLines);

  const fields = extractInvoiceFields(lines, normalizedText);
  const sourceCurrency = detectSourceCurrency(normalizedText);
  const conversionRate = getUsdConversionRate(sourceCurrency);
  const extractedLineItems = extractLineItems(lines, metadata.source);
  const mergedSourceItems = mergeLineItems(metadata.seededLineItems || [], extractedLineItems);
  const lineItems = convertLineItemsToCurrency(mergedSourceItems, conversionRate);
  const totals = convertTotalsToCurrency(extractFlexibleTotals(lines, normalizedText), conversionRate);
  const discrepancies = [];

  const computedSubtotal = round2(
    lineItems.reduce((sum, item) => sum + sanitizeNumber(item.amount), 0)
  );
  const computedGst = round2(
    lineItems.reduce((sum, item) => sum + sanitizeNumber(item.gst_amount), 0)
  );
  const billedSubtotal = totals.subtotal ?? computedSubtotal;
  const billedGst = totals.totalGst ?? computedGst;
  const billedTotal = totals.totalAmount ?? round2(billedSubtotal + billedGst + sanitizeNumber(totals.shippingAmount));

  for (const item of lineItems) {
    const expectedAmount = round2(sanitizeNumber(item.quantity) * sanitizeNumber(item.unit_price));
    if (expectedAmount > 0 && Math.abs(expectedAmount - sanitizeNumber(item.amount)) > 1) {
      discrepancies.push({
        type: "calculation_error",
        severity: differenceSeverity(Math.abs(expectedAmount - sanitizeNumber(item.amount))),
        line_item: item.description,
        description: `${item.description}: quantity x unit price is ${formatMoney(expectedAmount, TARGET_CURRENCY)}, but billed amount is ${formatMoney(item.amount, TARGET_CURRENCY)}.`,
        billed_amount: item.amount,
        correct_amount: expectedAmount,
        overcharge: round2(item.amount - expectedAmount)
      });
    }

    if (item.gst_rate > 0) {
      const expectedGst = round2((sanitizeNumber(item.amount) * sanitizeNumber(item.gst_rate)) / 100);
      if (Math.abs(expectedGst - sanitizeNumber(item.gst_amount)) > 1) {
        discrepancies.push({
          type: "gst_mismatch",
          severity: differenceSeverity(Math.abs(expectedGst - sanitizeNumber(item.gst_amount))),
          line_item: item.description,
          description: `${item.description}: GST at ${item.gst_rate}% should be ${formatMoney(expectedGst, TARGET_CURRENCY)}, but billed GST is ${formatMoney(item.gst_amount, TARGET_CURRENCY)}.`,
          billed_amount: item.gst_amount,
          correct_amount: expectedGst,
          overcharge: round2(item.gst_amount - expectedGst)
        });
      }
    }
  }

  const duplicateMap = new Map();
  for (const item of lineItems) {
    const duplicateKey = `${item.description.toLowerCase()}|${item.amount}`;
    duplicateMap.set(duplicateKey, (duplicateMap.get(duplicateKey) || 0) + 1);
  }

  for (const [duplicateKey, count] of duplicateMap.entries()) {
    if (count > 1) {
      const [description, amount] = duplicateKey.split("|");
      discrepancies.push({
        type: "duplicate_charge",
        severity: count > 2 ? "high" : "medium",
        line_item: description,
        description: `The line item "${description}" appears ${count} times with the same billed amount.`,
        billed_amount: Number(amount) * count,
        correct_amount: Number(amount),
        overcharge: round2(Number(amount) * (count - 1))
      });
    }
  }

  if (lineItems.length > 0 && totals.subtotal != null && Math.abs(totals.subtotal - computedSubtotal) > 1) {
    discrepancies.push({
      type: "calculation_error",
      severity: differenceSeverity(Math.abs(totals.subtotal - computedSubtotal)),
      line_item: null,
      description: `Invoice subtotal is ${formatMoney(totals.subtotal, TARGET_CURRENCY)}, but extracted line items sum to ${formatMoney(computedSubtotal, TARGET_CURRENCY)}.`,
      billed_amount: totals.subtotal,
      correct_amount: computedSubtotal,
      overcharge: round2(totals.subtotal - computedSubtotal)
    });
  }

  const hasLineLevelGst = lineItems.some((item) => item.gst_rate > 0 || item.gst_amount > 0);
  if (hasLineLevelGst && totals.totalGst != null && Math.abs(totals.totalGst - computedGst) > 1) {
    discrepancies.push({
      type: "gst_mismatch",
      severity: differenceSeverity(Math.abs(totals.totalGst - computedGst)),
      line_item: null,
      description: `Invoice GST total is ${formatMoney(totals.totalGst, TARGET_CURRENCY)}, but line-item GST sums to ${formatMoney(computedGst, TARGET_CURRENCY)}.`,
      billed_amount: totals.totalGst,
      correct_amount: computedGst,
      overcharge: round2(totals.totalGst - computedGst)
    });
  }

  const expectedGrandTotal = round2(
    (totals.subtotal ?? computedSubtotal) + (totals.totalGst ?? computedGst) + sanitizeNumber(totals.shippingAmount)
  );
  if (totals.totalAmount != null && Math.abs(totals.totalAmount - expectedGrandTotal) > 1) {
    discrepancies.push({
      type: "calculation_error",
      severity: differenceSeverity(Math.abs(totals.totalAmount - expectedGrandTotal)),
      line_item: null,
      description: `Invoice total is ${formatMoney(totals.totalAmount, TARGET_CURRENCY)}, but subtotal + GST equals ${formatMoney(expectedGrandTotal, TARGET_CURRENCY)}.`,
      billed_amount: totals.totalAmount,
      correct_amount: expectedGrandTotal,
      overcharge: round2(totals.totalAmount - expectedGrandTotal)
    });
  }

  const vendorGstinValid = isValidGstin(fields.vendorGstin);
  if (fields.vendorGstin && !vendorGstinValid) {
    discrepancies.push({
      type: "other",
      severity: "low",
      line_item: null,
      description: `Vendor GSTIN "${fields.vendorGstin}" does not match the standard 15-character format.`,
      billed_amount: 0,
      correct_amount: 0,
      overcharge: 0
    });
  }

  const totalOvercharge = round2(
    discrepancies.reduce((sum, item) => sum + Math.max(0, sanitizeNumber(item.overcharge)), 0)
  );
  const correctTotal = round2((totals.totalAmount ?? billedTotal) - totalOvercharge);

  return {
    vendor_name: fields.vendorName || "Unknown Vendor",
    invoice_number: fields.invoiceNumber || "",
    invoice_date: fields.invoiceDate || "",
    due_date: fields.dueDate || "",
    vendor_gstin: fields.vendorGstin || null,
    buyer_gstin: fields.buyerGstin || null,
    line_items: lineItems,
    subtotal: round2(billedSubtotal),
    total_gst: round2(billedGst),
    total_amount: round2(billedTotal),
    currency: TARGET_CURRENCY,
    discrepancies,
    total_overcharge: totalOvercharge,
    correct_total: correctTotal,
    audit_summary: buildSummary(discrepancies, metadata.source),
    confidence_score: estimateConfidence(lineItems, totals, discrepancies, rawText),
    extraction_notes: buildExtractionNotes(metadata, rawText, lineItems.length)
  };
}

function extractInvoiceFields(lines, text) {
  return {
    vendorName: findVendorName(lines),
    invoiceNumber: matchText(text, [
      /invoice\s*(?:no|number|#)\s*[:\-]?\s*([A-Z0-9#\-\/]+)/i,
      /bill\s*(?:no|number|#)\s*[:\-]?\s*([A-Z0-9#\-\/]+)/i
    ]),
    invoiceDate: matchText(text, [
      /invoice\s*date\s*[:\-]?\s*([A-Za-z]{3,9}\s+[0-9]{1,2},\s+[0-9]{4})/i,
      /invoice\s*date\s*[:\-]?\s*([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{2,4})/i,
      /date\s*[:\-]?\s*([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{2,4})/i,
      /invoice\s*date\s*[:\-]?\s*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})/i,
      /date\s*[:\-]?\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{2,4})/i
    ]),
    dueDate: matchText(text, [
      /due\s*date\s*[:\-]?\s*([A-Za-z]{3,9}\s+[0-9]{1,2},\s+[0-9]{4})/i,
      /due[-\s]*date\s*[:\-]?\s*([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{2,4})/i,
      /due\s*date\s*[:\-]?\s*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{2,4})/i,
      /payment\s*due\s*[:\-]?\s*([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{2,4})/i
    ]),
    vendorGstin: matchText(text, [/gstin\s*[:\-]?\s*([0-9A-Z]{15})/i]),
    buyerGstin: matchNthText(text, /gstin\s*[:\-]?\s*([0-9A-Z]{15})/gi, 2)
  };
}

function extractLineItems(lines, source = "") {
  const stackedItems = extractStackedLineItems(lines);
  if (stackedItems.length) {
    return stackedItems;
  }

  const items = [];

  for (const line of lines) {
    if (items.length >= 30) {
      break;
    }

    const item = parseLineItem(line);
    if (item) {
      items.push(item);
    }
  }

  const contextualItems = extractContextualLineItems(lines);
  if (source === "spreadsheet") {
    return items.length ? items : contextualItems;
  }

  return mergeLineItems(items, contextualItems);
}

function parseLineItem(line) {
  const normalized = line.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return null;
  }

  if (/invoice|subtotal|total|gstin|taxable|balance|amount due|sales tax|shipping/i.test(normalized)) {
    return null;
  }

  let match = normalized.match(
    /^(.+?)\s+(\d+(?:\.\d+)?)\s+([A-Za-z]+)\s+(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*([0-9,.' ]+(?:[.,]\d+)?)\s+(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*([0-9,.' ]+(?:[.,]\d+)?)\s+(\d{1,2}(?:\.\d+)?)%?\s+(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*([0-9,.' ]+(?:[.,]\d+)?)$/i
  );

  if (match) {
    return {
      description: cleanupDescription(match[1]),
      hsn_code: findHsnInText(match[1]),
      quantity: toNumber(match[2]),
      unit: match[3],
      unit_price: toNumber(match[4]),
      amount: toNumber(match[5]),
      gst_rate: toNumber(match[6]),
      gst_amount: toNumber(match[7])
    };
  }

  match = normalized.match(
    /^(.+?)\s+(\d+(?:\.\d+)?)\s*[xX]\s*(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*([0-9,.' ]+(?:[.,]\d+)?)\s*=\s*(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*([0-9,.' ]+(?:[.,]\d+)?)(?:\s+GST\s+(\d{1,2}(?:\.\d+)?)%?\s+(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*([0-9,.' ]+(?:[.,]\d+)?))?$/i
  );

  if (match) {
    return {
      description: cleanupDescription(match[1]),
      hsn_code: findHsnInText(match[1]),
      quantity: toNumber(match[2]),
      unit: "unit",
      unit_price: toNumber(match[3]),
      amount: toNumber(match[4]),
      gst_rate: toNumber(match[5]),
      gst_amount: toNumber(match[6])
    };
  }

  match = normalized.match(
    /^(.+?)\s+(\d+(?:\.\d+)?)\s+([A-Za-z]+)\s+([0-9,.' ]+(?:[.,]\d+)?)\s+([0-9,.' ]+(?:[.,]\d+)?)$/i
  );

  if (match) {
    return {
      description: cleanupDescription(match[1]),
      hsn_code: findHsnInText(match[1]),
      quantity: toNumber(match[2]),
      unit: match[3],
      unit_price: toNumber(match[4]),
      amount: toNumber(match[5]),
      gst_rate: 0,
      gst_amount: 0
    };
  }

  match = normalized.match(
    /^(\d+(?:\.\d+)?)\s+(.+?)\s+([0-9,.' ]+(?:[.,]\d+)?)\s+([0-9,.' ]+(?:[.,]\d+)?)$/i
  );

  if (match && /[A-Za-z]/.test(match[2])) {
    return {
      description: cleanupDescription(match[2]),
      hsn_code: findHsnInText(match[2]),
      quantity: toNumber(match[1]),
      unit: "unit",
      unit_price: toNumber(match[3]),
      amount: toNumber(match[4]),
      gst_rate: 0,
      gst_amount: 0
    };
  }

  return null;
}

function extractTotals(lines, text) {
  const subtotalFromLines = findAmountAfterLabel(lines, ["Sub Total", "Subtotal"]);
  const taxFromLines = findAmountAfterLabel(lines, ["Tax", "Total GST", "GST Amount"]);
  const totalFromLines = findAmountAfterLabel(lines, ["Total Due", "Grand Total", "Total"]);
  const amountPattern = "([0-9][0-9,.' ]*(?:[.,][0-9]{1,2})?)";

  return {
    subtotal: subtotalFromLines ?? matchAmount(text, [
      new RegExp(`subtotal\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`taxable\\s*value\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ]),
    totalGst: taxFromLines ?? matchAmount(text, [
      new RegExp(`sales\\s*tax\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`total\\s*gst\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`(?:cgst|sgst|igst)[\\s\\S]{0,80}?total\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`gst\\s*amount\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ]),
    shippingAmount: matchAmount(text, [
      new RegExp(`shipping(?:\\s*&\\s*handling)?\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ]),
    totalAmount: totalFromLines ?? matchAmount(text, [
      new RegExp(`total\\s*due\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`grand\\s*total\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`invoice\\s*total\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`net\\s*amount\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`total\\s*amount\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ])
  };
}

function findVendorName(lines) {
  const fromIndex = lines.findIndex((line) => /^From:$/i.test(line));
  if (fromIndex !== -1 && lines[fromIndex + 1]) {
    return lines[fromIndex + 1];
  }

  const blacklist = /tax invoice|invoice|bill to|ship to|gstin|date|total|amount|phone:|comments|terms|lorem ipsum|quantity|unit price/i;
  for (const line of lines.slice(0, 20)) {
    const digitCount = (line.match(/\d/g) || []).length;
    if (
      line.length > 2 &&
      line.length < 80 &&
      digitCount <= 4 &&
      /[A-Za-z]/.test(line) &&
      !blacklist.test(line) &&
      !/^bpx/i.test(line)
    ) {
      return line;
    }
  }
  return "";
}

function matchText(text, patterns) {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) {
      return match[1].trim();
    }
  }
  return "";
}

function matchNthText(text, pattern, index) {
  const matches = [...text.matchAll(pattern)];
  return matches[index - 1]?.[1]?.trim() || "";
}

function matchAmount(text, patterns) {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) {
      return toNumber(match[1]);
    }
  }
  return null;
}

function toNumber(value) {
  if (value == null || value === "") {
    return 0;
  }
  const raw = String(value)
    .trim()
    .replace(/[^0-9,.\-'\s]/g, "")
    .replace(/'/g, "")
    .replace(/\s+/g, "");

  if (!raw) {
    return 0;
  }

  let normalized = raw;
  const hasComma = normalized.includes(",");
  const hasDot = normalized.includes(".");

  if (hasComma && hasDot) {
    const lastComma = normalized.lastIndexOf(",");
    const lastDot = normalized.lastIndexOf(".");
    if (lastComma > lastDot) {
      normalized = normalized.replace(/\./g, "").replace(",", ".");
    } else {
      normalized = normalized.replace(/,/g, "");
    }
  } else if (hasComma) {
    normalized = /,\d{1,2}$/.test(normalized)
      ? normalized.replace(/\./g, "").replace(",", ".")
      : normalized.replace(/,/g, "");
  } else if ((normalized.match(/\./g) || []).length > 1) {
    const parts = normalized.split(".");
    const decimal = parts.pop();
    normalized = `${parts.join("")}.${decimal}`;
  }

  return Number(normalized) || 0;
}

function sanitizeNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function round2(value) {
  return Math.round((sanitizeNumber(value) + Number.EPSILON) * 100) / 100;
}

function differenceSeverity(diff) {
  if (diff >= 500) {
    return "high";
  }
  if (diff >= 100) {
    return "medium";
  }
  return "low";
}

function detectCurrency(text) {
  if (/\$|\busd\b/i.test(text)) {
    return "USD";
  }
  if (/₹|\brs\.?\b|\binr\b/i.test(text)) {
    return "INR";
  }
  return "INR";
}

function formatMoney(value, currency = TARGET_CURRENCY) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(round2(value));
}

function getConversionRate(sourceCurrency, targetCurrency) {
  if (sourceCurrency === targetCurrency) {
    return 1;
  }

  if (sourceCurrency === "INR" && targetCurrency === "USD") {
    return INR_TO_USD_RATE;
  }

  return 1;
}

function convertLineItemsToCurrency(lineItems, conversionRate) {
  return lineItems.map((item) => ({
    ...item,
    unit_price: convertAmount(item.unit_price, conversionRate),
    amount: convertAmount(item.amount, conversionRate),
    gst_amount: convertAmount(item.gst_amount, conversionRate)
  }));
}

function convertTotalsToCurrency(totals, conversionRate) {
  return {
    subtotal: convertOptionalAmount(totals.subtotal, conversionRate),
    totalGst: convertOptionalAmount(totals.totalGst, conversionRate),
    totalAmount: convertOptionalAmount(totals.totalAmount, conversionRate)
  };
}

function convertOptionalAmount(value, conversionRate) {
  return value == null ? value : convertAmount(value, conversionRate);
}

function convertAmount(value, conversionRate) {
  return round2(sanitizeNumber(value) * conversionRate);
}

function detectSourceCurrency(text) {
  const detectors = [
    { currency: "GBP", pattern: /\bgbp\b|£|pounds?\b|sterling\b/i },
    { currency: "EUR", pattern: /\beur\b|€|euros?\b/i },
    { currency: "AUD", pattern: /\baud\b|australian dollars?\b|aud\$/i },
    { currency: "CAD", pattern: /\bcad\b|canadian dollars?\b|cad\$/i },
    { currency: "JPY", pattern: /\bjpy\b|¥|yen\b/i },
    { currency: "CNY", pattern: /\bcny\b|\brmb\b|yuan\b|renminbi\b/i },
    { currency: "SGD", pattern: /\bsgd\b|singapore dollars?\b|sgd\$/i },
    { currency: "AED", pattern: /\baed\b|dirhams?\b|uae dirhams?\b/i },
    { currency: "CHF", pattern: /\bchf\b|swiss francs?\b/i },
    { currency: "INR", pattern: /₹|â‚¹|\brs\.?\b|\binr\b|rupees?\b/i },
    { currency: "USD", pattern: /\busd\b|us dollars?\b|\$/i }
  ];

  for (const detector of detectors) {
    if (detector.pattern.test(text)) {
      return detector.currency;
    }
  }

  return "USD";
}

function getUsdConversionRate(sourceCurrency) {
  return USD_EXCHANGE_RATES[sourceCurrency] || 1;
}

function extractFlexibleTotals(lines, text) {
  const subtotalFromLines = findFlexibleAmountAfterLabel(lines, ["Sub Total", "Subtotal"]);
  const taxFromLines = findFlexibleAmountAfterLabel(lines, ["Sales Tax", "Tax", "Total GST", "GST Amount"]);
  const shippingFromLines = findFlexibleAmountAfterLabel(lines, ["Shipping & Handling", "Shipping"]);
  const totalFromLines = findFlexibleAmountAfterLabel(lines, ["Total Due", "Grand Total", "Total"]);
  const amountPattern = "([0-9][0-9,.' ]*(?:[.,][0-9]{1,2})?)";

  return {
    subtotal: subtotalFromLines ?? matchAmount(text, [
      new RegExp(`subtotal\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`taxable\\s*value\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ]),
    totalGst: taxFromLines ?? matchAmount(text, [
      new RegExp(`sales\\s*tax\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`total\\s*gst\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`(?:cgst|sgst|igst)[\\s\\S]{0,80}?total\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`gst\\s*amount\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ]),
    shippingAmount: shippingFromLines ?? matchAmount(text, [
      new RegExp(`shipping(?:\\s*&\\s*handling)?\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ]),
    totalAmount: totalFromLines ?? matchAmount(text, [
      new RegExp(`total\\s*due\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`grand\\s*total\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`invoice\\s*total\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`net\\s*amount\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i"),
      new RegExp(`total\\s*amount\\s*[:\\-]?\\s*${CURRENCY_TOKEN}\\s*${amountPattern}`, "i")
    ])
  };
}

function isFlexibleMoneyLine(line) {
  return /^(?:usd|inr|eur|gbp|aud|cad|jpy|cny|rmb|sgd|aed|chf|rs\.?|[$£€¥₹])?\s*-?[0-9][0-9,.' ]*(?:[.,][0-9]{1,2})?$/i.test(line.trim());
}

function findFlexibleAmountAfterLabel(lines, labels) {
  let result = null;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const matchedLabel = labels.find((label) => matchesLabelInLine(line, label));
    if (matchedLabel) {
      const inlinePattern = new RegExp(`${matchedLabel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}[^0-9-]*([0-9][0-9,.' ]*(?:[.,][0-9]{1,2})?)`, "i");
      const inlineMatch = line.match(inlinePattern);
      if (inlineMatch?.[1]) {
        result = toNumber(inlineMatch[1]);
        continue;
      }

      for (let j = i + 1; j < Math.min(i + 3, lines.length); j++) {
        if (isFlexibleMoneyLine(lines[j])) {
          result = toNumber(lines[j]);
        }
      }
    }
  }

  return result;
}

function matchesLabelInLine(line, label) {
  const escapedLabel = label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "\\s+");
  return new RegExp(`(?:^|\\b)${escapedLabel}(?:\\b|$)`, "i").test(line);
}

function isValidGstin(value) {
  if (!value) {
    return false;
  }
  return /^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$/i.test(value);
}

function buildSummary(discrepancies, source) {
  if (!discrepancies.length) {
    return `No material discrepancies were detected by the ${source} rule engine.`;
  }

  const high = discrepancies.filter((item) => item.severity === "high").length;
  const medium = discrepancies.filter((item) => item.severity === "medium").length;
  return `${discrepancies.length} discrepancy(s) detected${high ? `, including ${high} high-severity issue(s)` : ""}${medium ? ` and ${medium} medium-severity issue(s)` : ""}.`;
}

function estimateConfidence(lineItems, totals, discrepancies, rawText) {
  let score = 0.35;
  if (rawText.length > 400) {
    score += 0.15;
  }
  if (lineItems.length > 0) {
    score += 0.25;
  }
  if (totals.totalAmount != null) {
    score += 0.15;
  }
  if (discrepancies.length > 0) {
    score += 0.05;
  }
  return Math.min(0.95, round2(score));
}

function buildExtractionNotes(metadata, rawText, lineItemCount) {
  return `Source=${metadata.source}; mediaType=${metadata.mediaType}; extractedChars=${rawText.length}; parsedLineItems=${lineItemCount}. This backend uses local rules and text extraction only.`;
}

function buildUnreadableFallback(message) {
  return {
    vendor_name: "Unknown Vendor",
    invoice_number: "",
    invoice_date: "",
    due_date: "",
    vendor_gstin: null,
    buyer_gstin: null,
    line_items: [],
    subtotal: 0,
    total_gst: 0,
    total_amount: 0,
    currency: "USD",
    discrepancies: [
      {
        type: "other",
        severity: "medium",
        line_item: null,
        description: message,
        billed_amount: 0,
        correct_amount: 0,
        overcharge: 0
      }
    ],
    total_overcharge: 0,
    correct_total: 0,
    audit_summary: "The file was received, but not enough machine-readable text was available for a full rule-based audit.",
    confidence_score: 0.2,
    extraction_notes: message
  };
}

function buildImageFallback(mediaType) {
  return {
    vendor_name: "Unknown Vendor",
    invoice_number: "",
    invoice_date: "",
    due_date: "",
    vendor_gstin: null,
    buyer_gstin: null,
    line_items: [],
    subtotal: 0,
    total_gst: 0,
    total_amount: 0,
    currency: "USD",
    discrepancies: [
      {
        type: "other",
        severity: "medium",
        line_item: null,
        description: "Image uploads need OCR, which is not available in this no-key backend. Use a text-based PDF for best results.",
        billed_amount: 0,
        correct_amount: 0,
        overcharge: 0
      }
    ],
    total_overcharge: 0,
    correct_total: 0,
    audit_summary: "Image received, but OCR is not enabled in the local rule-based backend.",
    confidence_score: 0.15,
    extraction_notes: `Unsupported image media type for offline parsing: ${mediaType}`
  };
}

function normalizeText(text) {
  return text
    .replace(/\r/g, "\n")
    .replace(/[^\x09\x0A\x0D\x20-\x7E₹]/g, " ")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n");
}

function sanitizeLines(lines) {
  const trimmed = lines.filter((line) => isReadableInvoiceLine(line));
  const invoiceIndex = trimmed.findIndex((line) =>
    /^(Invoice|From:|Invoice Number|Tax Invoice)$/i.test(line)
  );
  const sliced = invoiceIndex === -1 ? trimmed : trimmed.slice(invoiceIndex);
  const paidIndex = sliced.findIndex((line) => /^Paid$/i.test(line));
  return paidIndex === -1 ? sliced : sliced.slice(0, paidIndex + 1);
}

function cleanupDescription(value) {
  return value.replace(/\s+/g, " ").replace(/^[^A-Za-z0-9]+/, "").trim();
}

function findHsnInText(text) {
  const match = text.match(/\b(\d{4,8})\b/);
  return match?.[1] || "";
}

function extractPdfText(buffer) {
  const { pdfText, chunks, positionedItems } = collectPdfDecodedContent(buffer);

  if (positionedItems.length) {
    const laidOutText = layoutPdfTextItems(positionedItems).trim();
    if (laidOutText) {
      return laidOutText;
    }
  }

  if (!chunks.length) {
    chunks.push(extractPdfStrings(pdfText, unicodeMap));
  }

  const extracted = chunks.join("\n").trim();
  if (extracted) {
    return extracted;
  }

  const utf8Text = buffer.toString("utf8");
  if (looksLikePlainText(utf8Text)) {
    return utf8Text;
  }

  return "";
}

function collectPdfDecodedContent(buffer) {
  const chunks = [];
  const positionedItems = [];
  const streamRegex = /stream\r?\n([\s\S]*?)endstream/g;
  const pdfText = buffer.toString("latin1");
  const unicodeMap = extractPdfUnicodeMap(pdfText);
  let match;

  while ((match = streamRegex.exec(pdfText)) !== null) {
    const streamContent = match[1];
    const streamBuffer = Buffer.from(streamContent, "latin1");
    const candidates = [streamBuffer];

    try {
      candidates.push(zlib.inflateSync(streamBuffer));
    } catch {}

    try {
      candidates.push(zlib.inflateRawSync(streamBuffer));
    } catch {}

    for (const candidate of candidates) {
      const candidateText = candidate.toString("latin1");
      const items = extractPdfTextItems(candidateText, unicodeMap);
      if (items.length) {
        positionedItems.push(...items);
      }

      const extracted = extractPdfStrings(candidateText, unicodeMap);
      if (extracted) {
        chunks.push(extracted);
      }
    }
  }

  return { pdfText, chunks, positionedItems, unicodeMap };
}

function extractStructuredPdfLineItems(buffer) {
  const { positionedItems } = collectPdfDecodedContent(buffer);
  if (!positionedItems.length) {
    return [];
  }

  const lines = groupPdfItemsByLine(positionedItems);
  const headerIndex = lines.findIndex((line) => {
    const text = composePdfLine(line.items);
    return /product/i.test(text) && /amount/i.test(text);
  });

  if (headerIndex === -1) {
    return [];
  }

  const items = [];
  const seen = new Set();

  for (let i = 0; i < lines.length; i++) {
    const productRow = parseStructuredPdfProductRow(lines[i]);
    if (!productRow) {
      continue;
    }

    let quantityRow = null;
    for (let offset = 1; offset <= 3; offset++) {
      quantityRow = parseStructuredPdfQuantityRow(lines[i + offset]);
      if (quantityRow) {
        break;
      }
    }

    quantityRow = quantityRow || parseStructuredPdfQuantityRow(lines[i]);
    if (!quantityRow) {
      continue;
    }

    const item = {
      description: productRow.description,
      hsn_code: quantityRow.code,
      quantity: quantityRow.quantity || 1,
      unit: "pcs",
      unit_price:
        quantityRow.unitPrice ||
        (quantityRow.quantity ? round2(productRow.amount / quantityRow.quantity) : 0),
      amount: productRow.amount,
      gst_rate: 0,
      gst_amount: 0
    };

    if (!item.description || item.amount <= 0) {
      continue;
    }

    const key = `${item.description}|${item.quantity}|${item.amount}`;
    if (!seen.has(key)) {
      seen.add(key);
      items.push(item);
    }
  }

  return items;
}

function groupPdfItemsByLine(items) {
  const sorted = [...items].sort((a, b) => {
    if (Math.abs(b.y - a.y) > 1.5) {
      return b.y - a.y;
    }
    return a.x - b.x;
  });

  const lines = [];
  for (const item of sorted) {
    const existingLine = lines.find((line) => Math.abs(line.y - item.y) <= 1.2);
    if (existingLine) {
      existingLine.items.push(item);
    } else {
      lines.push({ y: item.y, items: [item] });
    }
  }

  return lines.sort((a, b) => b.y - a.y);
}

function parseStructuredPdfProductRow(line) {
  if (!line?.items?.length) {
    return null;
  }

  const numberToken = line.items.find((item) => item.x < 110 && /^\d+\.$/.test(item.text.trim()));
  if (!numberToken) {
    return null;
  }

  const description = cleanupDescription(
    line.items
      .filter((item) => item.x >= 110 && item.x < 200 && /[A-Za-z]/.test(item.text))
      .map((item) => item.text)
      .join(" ")
  );
  const amount = findRightmostNumericItem(line.items, 280);

  if (!description || amount == null) {
    return null;
  }

  return {
    index: toNumber(numberToken.text),
    description,
    amount
  };
}

function parseStructuredPdfQuantityRow(line) {
  if (!line?.items?.length) {
    return null;
  }

  const quantityItem = line.items.find((item) => /\b\d+\s*pcs\.?/i.test(item.text));
  if (!quantityItem) {
    return null;
  }

  const quantityMatch = quantityItem.text.match(/(\d+)\s*pcs\.?/i);
  const codeTokens = line.items
    .filter((item) => item.x >= 110 && item.x < 180 && /^\d+$/.test(item.text.trim()))
    .map((item) => item.text.trim());
  const unitPrice = findRightmostNumericItem(
    line.items.filter((item) => item.x >= 280 && item.x < 380),
    280
  );

  return {
    code: codeTokens.length >= 3 ? `${codeTokens[0]}-${codeTokens[1]}-${codeTokens[2]}` : "",
    quantity: quantityMatch ? toNumber(quantityMatch[1]) : 0,
    unitPrice: unitPrice ?? 0
  };
}

function findRightmostNumericItem(items, minX = 0) {
  const candidate = [...items]
    .filter((item) => item.x >= minX && isFlexibleMoneyLine(item.text))
    .sort((a, b) => b.x - a.x)[0];

  return candidate ? toNumber(candidate.text) : null;
}

function extractPdfUnicodeMap(pdfText) {
  const unicodeMap = new Map();
  const streamRegex = /stream\r?\n([\s\S]*?)endstream/g;
  let match;

  while ((match = streamRegex.exec(pdfText)) !== null) {
    const streamContent = match[1];
    const streamBuffer = Buffer.from(streamContent, "latin1");
    const candidates = [streamBuffer];

    try {
      candidates.push(zlib.inflateSync(streamBuffer));
    } catch {}

    try {
      candidates.push(zlib.inflateRawSync(streamBuffer));
    } catch {}

    for (const candidate of candidates) {
      const candidateText = candidate.toString("latin1");
      if (!candidateText.includes("begincmap")) {
        continue;
      }

      mergePdfUnicodeMap(unicodeMap, candidateText);
    }
  }

  return unicodeMap;
}

function mergePdfUnicodeMap(targetMap, cmapText) {
  const bfcharRegex = /beginbfchar([\s\S]*?)endbfchar/g;
  let bfcharMatch;

  while ((bfcharMatch = bfcharRegex.exec(cmapText)) !== null) {
    const pairRegex = /<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>/g;
    let pairMatch;

    while ((pairMatch = pairRegex.exec(bfcharMatch[1])) !== null) {
      targetMap.set(pairMatch[1].toUpperCase(), decodeUtf16BeHex(pairMatch[2]));
    }
  }

  const bfrangeRegex = /beginbfrange([\s\S]*?)endbfrange/g;
  let bfrangeMatch;

  while ((bfrangeMatch = bfrangeRegex.exec(cmapText)) !== null) {
    const rangeRegex = /<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*(<([0-9A-Fa-f]+)>|\[([\s\S]*?)\])/g;
    let rangeMatch;

    while ((rangeMatch = rangeRegex.exec(bfrangeMatch[1])) !== null) {
      const start = parseInt(rangeMatch[1], 16);
      const end = parseInt(rangeMatch[2], 16);

      if (rangeMatch[4]) {
        const destinationStart = parseInt(rangeMatch[4], 16);
        for (let code = start; code <= end; code++) {
          const sourceHex = code.toString(16).toUpperCase().padStart(rangeMatch[1].length, "0");
          const destinationHex = (destinationStart + (code - start))
            .toString(16)
            .toUpperCase()
            .padStart(rangeMatch[4].length, "0");
          targetMap.set(sourceHex, decodeUtf16BeHex(destinationHex));
        }
      } else if (rangeMatch[5]) {
        const destinations = [...rangeMatch[5].matchAll(/<([0-9A-Fa-f]+)>/g)].map((entry) => entry[1]);
        for (let code = start; code <= end; code++) {
          const destinationHex = destinations[code - start];
          if (!destinationHex) {
            break;
          }

          const sourceHex = code.toString(16).toUpperCase().padStart(rangeMatch[1].length, "0");
          targetMap.set(sourceHex, decodeUtf16BeHex(destinationHex));
        }
      }
    }
  }
}

function decodeUtf16BeHex(hex) {
  const cleanHex = hex.length % 2 === 0 ? hex : `${hex}0`;
  let result = "";

  for (let i = 0; i < cleanHex.length; i += 4) {
    const unitHex = cleanHex.slice(i, i + 4).padEnd(4, "0");
    result += String.fromCharCode(parseInt(unitHex, 16));
  }

  return result.replace(/\u0000/g, "");
}

function extractPdfTextItems(text, unicodeMap) {
  const items = [];
  const textBlockRegex = /BT([\s\S]*?)ET/g;
  let blockMatch;

  while ((blockMatch = textBlockRegex.exec(text)) !== null) {
    const block = blockMatch[1];
    const matrixMatch = block.match(
      /(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+Tm/
    );

    const content = extractPdfBlockText(block, unicodeMap);
    if (!content.trim()) {
      continue;
    }

    items.push({
      x: matrixMatch ? Number(matrixMatch[5]) : 0,
      y: matrixMatch ? Number(matrixMatch[6]) : 0,
      text: content
    });
  }

  return items;
}

function extractPdfBlockText(block, unicodeMap) {
  const pieces = [];

  const arrayRegex = /\[(.*?)\]\s*TJ/gs;
  let arrayMatch;
  while ((arrayMatch = arrayRegex.exec(block)) !== null) {
    const text = extractPdfInlineStrings(arrayMatch[1], unicodeMap);
    if (text) {
      pieces.push(text);
    }
  }

  if (!pieces.length) {
    const directTextRegex = /(\((?:\\.|[^\\()])*\)|<[0-9A-Fa-f]+>)\s*Tj/g;
    let directMatch;
    while ((directMatch = directTextRegex.exec(block)) !== null) {
      const token = directMatch[1];
      const text = token.startsWith("(")
        ? unescapePdfText(token.slice(1, -1))
        : decodePdfHex(token.slice(1, -1), unicodeMap);

      if (text) {
        pieces.push(text);
      }
    }
  }

  return pieces.join("");
}

function extractPdfInlineStrings(value, unicodeMap) {
  const parts = [];
  const tokenRegex = /(\((?:\\.|[^\\()])*\)|<[0-9A-Fa-f]+>)/g;
  let tokenMatch;

  while ((tokenMatch = tokenRegex.exec(value)) !== null) {
    const token = tokenMatch[1];
    const text = token.startsWith("(")
      ? unescapePdfText(token.slice(1, -1))
      : decodePdfHex(token.slice(1, -1), unicodeMap);

    if (text) {
      parts.push(text);
    }
  }

  return parts.join("");
}

function layoutPdfTextItems(items) {
  const sorted = [...items].sort((a, b) => {
    if (Math.abs(b.y - a.y) > 1.5) {
      return b.y - a.y;
    }
    return a.x - b.x;
  });

  const lines = [];

  for (const item of sorted) {
    const existingLine = lines.find((line) => Math.abs(line.y - item.y) <= 1.2);
    if (existingLine) {
      existingLine.items.push(item);
    } else {
      lines.push({ y: item.y, items: [item] });
    }
  }

  return lines
    .sort((a, b) => b.y - a.y)
    .map((line) => composePdfLine(line.items))
    .filter(Boolean)
    .join("\n");
}

function composePdfLine(items) {
  const sorted = [...items].sort((a, b) => a.x - b.x);
  let line = "";
  let previous = null;

  for (const item of sorted) {
    const text = item.text.replace(/\u0000/g, "");
    if (!text) {
      continue;
    }

    const trimmed = text.trim();
    if (!trimmed) {
      if (!line.endsWith(" ")) {
        line += " ";
      }
      previous = item;
      continue;
    }

    if (
      previous &&
      item.x - previous.x > 18 &&
      !line.endsWith(" ") &&
      !/^[,.:;)\-]/.test(trimmed)
    ) {
      line += " ";
    }

    line += text;
    previous = item;
  }

  return line.replace(/\s+/g, " ").trim();
}

function extractPdfStrings(text, unicodeMap) {
  const matches = [];
  const literalRegex = /\(([^()]*)\)/g;
  let literal;

  while ((literal = literalRegex.exec(text)) !== null) {
    matches.push(unescapePdfText(literal[1]));
  }

  const hexRegex = /<([0-9A-Fa-f]{4,})>/g;
  let hex;
  while ((hex = hexRegex.exec(text)) !== null) {
    const decoded = decodePdfHex(hex[1], unicodeMap);
    if (decoded.trim()) {
      matches.push(decoded);
    }
  }

  return matches.join("\n");
}

function unescapePdfText(text) {
  return text
    .replace(/\\n/g, "\n")
    .replace(/\\r/g, "\n")
    .replace(/\\t/g, " ")
    .replace(/\\\(/g, "(")
    .replace(/\\\)/g, ")")
    .replace(/\\\\/g, "\\");
}

function decodePdfHex(hex, unicodeMap) {
  const normalized = hex.toUpperCase();

  if (unicodeMap?.size && normalized.length % 4 === 0) {
    const parts = [];
    for (let i = 0; i < normalized.length; i += 4) {
      const code = normalized.slice(i, i + 4);
      parts.push(unicodeMap.get(code) ?? Buffer.from(code, "hex").toString("latin1"));
    }
    return parts.join("");
  }

  const cleanHex = normalized.length % 2 === 0 ? normalized : `${normalized}0`;
  return Buffer.from(cleanHex, "hex").toString("utf8");
}

function looksLikePlainText(text) {
  const clean = text.replace(/[\r\n\t ]/g, "");
  if (!clean) {
    return false;
  }

  const printable = clean.match(/[A-Za-z0-9:.,/%₹\-]/g) || [];
  return printable.length / clean.length > 0.8;
}

function isReadableInvoiceLine(line) {
  if (!line || line.length < 1) {
    return false;
  }

  const clean = line.replace(/\s/g, "");
  if (!clean) {
    return false;
  }

  const readableChars = clean.match(/[A-Za-z0-9@&:#/,$.%\-()]/g) || [];
  return readableChars.length / clean.length > 0.55;
}

function isMoneyLine(line) {
  return /^(?:[$₹]|rs\.?|inr)?\s*-?[0-9,]+(?:\.\d{1,2})?$/.test(line.trim());
}

function isPercentLine(line) {
  return /^-?[0-9]+(?:\.\d+)?%$/.test(line.trim());
}

function findAmountAfterLabel(lines, labels) {
  let result = null;

  for (let i = 0; i < lines.length; i++) {
    if (labels.some((label) => lines[i].toLowerCase() === label.toLowerCase())) {
      for (let j = i + 1; j < Math.min(i + 3, lines.length); j++) {
        if (isFlexibleMoneyLine(lines[j])) {
          result = toNumber(lines[j]);
        }
      }
    }
  }

  return result;
}

function extractStackedLineItems(lines) {
  const headerIndex = lines.findIndex((line) => /^Hrs\/Qty$/i.test(line));
  if (headerIndex === -1) {
    return [];
  }

  const items = [];
  let i = headerIndex + 5;

  while (i < lines.length) {
    if (/^(Sub Total|Tax|Total|Paid)$/i.test(lines[i])) {
      break;
    }

    if (!/^\d+(?:\.\d+)?$/.test(lines[i])) {
      i += 1;
      continue;
    }

    const quantity = toNumber(lines[i]);
    let cursor = i + 1;
    const descriptionParts = [];

    while (cursor < lines.length && !isFlexibleMoneyLine(lines[cursor]) && !/^(Sub Total|Tax|Total|Paid)$/i.test(lines[cursor])) {
      descriptionParts.push(lines[cursor]);
      cursor += 1;
    }

    const unitPriceLine = lines[cursor];
    const adjustLine = lines[cursor + 1];
    const amountLine = lines[cursor + 2];

    if (!isFlexibleMoneyLine(unitPriceLine) || !amountLine || !isFlexibleMoneyLine(amountLine)) {
      i += 1;
      continue;
    }

    items.push({
      description: cleanupDescription(descriptionParts.join(" - ")) || "Line item",
      hsn_code: findHsnInText(descriptionParts.join(" ")),
      quantity,
      unit: "hrs",
      unit_price: toNumber(unitPriceLine),
      amount: toNumber(amountLine),
      gst_rate: 0,
      gst_amount: 0
    });

    i = cursor + (isPercentLine(adjustLine) ? 3 : 2);
  }

  return items;
}

function extractContextualLineItems(lines) {
  const items = [];
  const seen = new Set();

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (
      /invoice|subtotal|sales tax|shipping|total due|comments|terms|phone:|ship to|quantity description|unit price total|make all checks|thank you/i.test(line) ||
      /^\d+(?:\.\d+)?\s+/.test(line) ||
      /^bpx/i.test(line) ||
      /\+\d/.test(line) ||
      /pcs\./i.test(line)
    ) {
      continue;
    }

    const match = line.match(/^(.+?)\s+([0-9,.' ]+(?:[.,]\d+)?)\s+([0-9,.' ]+(?:[.,]\d+)?)$/);
    if (!match || !/[A-Za-z]/.test(match[1]) || (match[1].match(/\d/g) || []).length > 6) {
      continue;
    }

    const quantityLine =
      findNearbyQuantity(lines, i, -1) ||
      findNearbyQuantity(lines, i, 1) ||
      "";

    const quantity = quantityLine ? toNumber(quantityLine) : 0;
    const item = {
      description: cleanupDescription(match[1]),
      hsn_code: findHsnInText(match[1]),
      quantity: quantity || 1,
      unit: "unit",
      unit_price: toNumber(match[2]),
      amount: toNumber(match[3]),
      gst_rate: 0,
      gst_amount: 0
    };

    const key = `${item.description}|${item.quantity}|${item.amount}`;
    if (!seen.has(key)) {
      seen.add(key);
      items.push(item);
    }
  }

  return items;
}

function findNearbyQuantity(lines, index, direction) {
  for (let offset = 1; offset <= 3; offset++) {
    const line = lines[index + offset * direction] || "";
    if (!line) {
      continue;
    }

    if (/^bpx/i.test(line) || /phone:|ship to|subtotal|sales tax|shipping|total due/i.test(line)) {
      continue;
    }

    if (/^\d+(?:\.\d+)?$/.test(line.trim())) {
      return line.trim();
    }
  }

  return "";
}

function mergeLineItems(primaryItems, secondaryItems) {
  const merged = [];
  const seen = new Set();

  for (const item of [...primaryItems, ...secondaryItems]) {
    const key = `${item.description}|${item.quantity}|${item.amount}`;
    if (!seen.has(key)) {
      seen.add(key);
      merged.push(item);
    }
  }

  return merged;
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let raw = "";

    req.on("data", (chunk) => {
      raw += chunk;
      if (Buffer.byteLength(raw) > MAX_BODY_SIZE) {
        reject(withStatus(new Error("Request body is too large."), 413));
        req.destroy();
      }
    });

    req.on("end", () => {
      if (!raw) {
        return reject(withStatus(new Error("Request body is empty."), 400));
      }

      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(withStatus(new Error("Request body must be valid JSON."), 400));
      }
    });

    req.on("error", (error) => {
      reject(withStatus(error, 400));
    });
  });
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  res.end(JSON.stringify(payload));
}

function withStatus(error, statusCode) {
  error.statusCode = statusCode;
  return error;
}
