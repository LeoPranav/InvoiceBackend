# AuditBlox Backend

This backend keeps your existing `index.html` intact and replaces the external AI call with a local rule-based invoice checker that reports money values in USD.

## Setup

1. Start the server:

```powershell
node server.js
```

2. Open `http://localhost:3000`

## What it does

- Serves the current frontend from `index.html`
- Rewrites the browser request to a local backend route at runtime
- Accepts the same payload shape your existing frontend already sends
- Converts parsed invoice amounts to USD using configurable exchange-rate env vars
- Extracts text from text-based PDFs and checks:
- Extracts rows from `.xlsx` and `.csv` spreadsheets and runs the same audit rules
- line item math
- GST math
- subtotal and total consistency
  - duplicate charges
  - GSTIN format
- Returns results in the same response shape the frontend already expects

## Current limitations

- Best with text-based PDFs
- `.xlsx` and `.csv` are supported
- Scanned PDFs with no embedded text may produce limited results
- Legacy binary `.xls` files may work only if they contain plain-text/tabular content
- Image uploads are accepted but cannot be fully audited because this backend does not include OCR
- This is a deterministic rule engine, not an AI document understanding system

## Health check

- `GET /health` returns backend status and confirms the server is running

## Notes

- The `index.html` file on disk is not modified
- No API key is required
- You can tune the currency-to-USD conversion factors in `.env`
