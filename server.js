const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const HUBSPOT_TOKEN     = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// =============================
// CONFIG — Tuned for Anthropic Tier 4 + Render $7
// =============================
const CONCURRENCY        = 50;   // 50 parallel Claude calls — safe under Tier 4 (4000 RPM)
const PROCESS_INTERVAL   = 100;  // Check queue every 100ms for fast draining
const HUBSPOT_CONCURRENCY = 20;  // Parallel HubSpot write-backs
const MAX_RETRIES        = 3;

// =============================
// FREE EMAIL DOMAINS — skip these
// =============================
const FREE_EMAIL_DOMAINS = new Set([
  'gmail.com', 'googlemail.com',
  'yahoo.com', 'yahoo.co.uk', 'yahoo.co.in', 'yahoo.fr', 'yahoo.de',
  'hotmail.com', 'hotmail.co.uk', 'hotmail.fr',
  'outlook.com', 'outlook.co.uk',
  'live.com', 'live.co.uk', 'msn.com',
  'icloud.com', 'me.com', 'mac.com',
  'aol.com', 'protonmail.com', 'proton.me',
  'zoho.com', 'mail.com', 'yandex.com', 'yandex.ru',
  'gmx.com', 'gmx.de', 'gmx.net', 'web.de',
  'comcast.net', 'att.net', 'verizon.net', 'sbcglobal.net',
  'cox.net', 'charter.net', 'earthlink.net', 'bellsouth.net',
  'inbox.com', 'qq.com', '163.com', 'sina.com',
  'rediffmail.com', 'tutanota.com', 'fastmail.com'
]);

// =============================
// VALID INDUSTRY CATEGORIES
// =============================
const INDUSTRY_CATEGORIES = [
  'Technology & Software',
  'Healthcare & Life Sciences',
  'Financial Services & Fintech',
  'Real Estate & Construction',
  'Marketing, Media & Advertising',
  'Energy, Industrial & Manufacturing',
  'Professional Services & Consulting',
  'Nonprofit, Government & Public Sector',
  'Retail, Food & Consumer Goods',
  'Education',
  'Other'
];

// =============================
// DOMAIN CACHE — avoid duplicate Claude calls for same domain
// =============================
const domainCache = new Map(); // domain -> { companyName, industry, industryCategory }

// =============================
// QUEUE & STATS
// =============================
let queue    = [];
let inFlight = 0;
let hsInFlight = 0;

let stats = {
  processed: 0,
  skipped:   0,
  failed:    0,
  cacheHits: 0
};

// Reset enriched count every 30 minutes
setInterval(() => {
  stats.processed = 0;
  console.log('🔄 Enriched count reset');
}, 30 * 60 * 1000);

// =============================
// HEALTH CHECK
// =============================
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    queue: queue.length,
    inFlight,
    hsInFlight,
    cacheSize: domainCache.size,
    stats
  });
});

// =============================
// DASHBOARD
// =============================
app.get('/dashboard', (req, res) => {
  const rate = stats.processed + stats.skipped + stats.failed;
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>TPG Contact Enrichment</title>
      <meta http-equiv="refresh" content="5">
      <style>
        body { font-family: monospace; background: #0f0f0f; color: #23cfb0; padding: 40px; }
        h1 { font-size: 18px; margin-bottom: 6px; color: #fff; }
        .sub { font-size: 12px; color: #444; margin-bottom: 30px; }
        .grid { display: flex; gap: 50px; margin-bottom: 40px; flex-wrap: wrap; }
        .stat { font-size: 56px; font-weight: bold; line-height: 1; }
        .label { font-size: 12px; color: #555; margin-top: 8px; }
        .green { color: #a2cf23; } .teal { color: #23cfb0; }
        .orange { color: #f0a500; } .red { color: #e05252; }
        .grey { color: #333; } .blue { color: #238acf; }
        .section { margin-bottom: 32px; }
        .section-title { font-size: 11px; color: #444; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 14px; border-bottom: 1px solid #1a1a1a; padding-bottom: 6px; }
        .footer { font-size: 12px; color: #333; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
      </style>
    </head>
    <body>
      <h1>TPG Contact Enrichment &mdash; Monitor</h1>
      <div class="sub">Anthropic Tier 4 &nbsp;·&nbsp; Concurrency: ${CONCURRENCY} &nbsp;·&nbsp; Cache: ${domainCache.size} domains</div>

      <div class="section">
        <div class="section-title">Queue</div>
        <div class="grid">
          <div><div class="stat ${queue.length > 0 ? 'orange' : 'grey'}">${queue.length}</div><div class="label">waiting</div></div>
          <div><div class="stat teal">${inFlight}</div><div class="label">claude in-flight</div></div>
          <div><div class="stat blue">${hsInFlight}</div><div class="label">hubspot writes</div></div>
        </div>
      </div>

      <div class="section">
        <div class="section-title">Session Totals</div>
        <div class="grid">
          <div><div class="stat green">${stats.processed}</div><div class="label">enriched</div></div>
          <div><div class="stat teal">${stats.skipped}</div><div class="label">skipped</div></div>
          <div><div class="stat blue">${stats.cacheHits}</div><div class="label">cache hits</div></div>
          <div><div class="stat ${stats.failed > 0 ? 'red' : 'grey'}">${stats.failed}</div><div class="label">failed</div></div>
        </div>
      </div>

      <div class="footer">Last refreshed: ${new Date().toLocaleTimeString()} &nbsp;·&nbsp; Auto-refreshes every 5 seconds</div>
    </body>
    </html>
  `);
});

// =============================
// ENQUEUE ENDPOINT
// =============================
app.post('/enrich', (req, res) => {
  const { contactId, email, company } = req.body;

  if (!contactId || !email) {
    return res.status(400).json({ error: 'contactId and email are required' });
  }

  const domain = email.split('@')[1]?.toLowerCase();
  if (!domain) {
    return res.status(400).json({ error: 'Invalid email format' });
  }

  if (FREE_EMAIL_DOMAINS.has(domain)) {
    stats.skipped++;
    console.log(`⏭️  Skipped: ${email}`);
    return res.status(200).json({ status: 'skipped', reason: 'personal_email_domain' });
  }

  queue.push({ contactId, email, domain, company: company || '', retries: 0 });
  res.status(200).json({ status: 'queued', position: queue.length });
});

// =============================
// WORKER LOOP
// =============================
setInterval(() => {
  while (inFlight < CONCURRENCY && queue.length > 0) {
    const job = queue.shift();
    inFlight++;
    processJob(job);
  }
}, PROCESS_INTERVAL);

// =============================
// PROCESS ONE CONTACT
// =============================
async function processJob(job) {
  try {
    let result;

    // Use cache if we've already looked up this domain
    if (domainCache.has(job.domain)) {
      result = domainCache.get(job.domain);
      stats.cacheHits++;
      console.log(`💾 Cache hit: ${job.domain}`);
    } else {
      result = await runClaude(job);
      domainCache.set(job.domain, result);
    }

    const { companyName, industry, industryCategory } = result;

    await writeToHubSpot(job.contactId, companyName, industry, industryCategory);

    stats.processed++;
    console.log(`✅ ${job.email} → "${companyName}" / "${industry}" / "${industryCategory}"`);

  } catch (err) {
    console.error(`❌ Failed: ${job.email} — ${err.message}`);

    if (err.response?.status === 429) {
      // Rate limited — back off and requeue
      setTimeout(() => queue.push(job), 2000);
    } else {
      job.retries = (job.retries || 0) + 1;
      if (job.retries <= MAX_RETRIES) {
        queue.push(job);
      } else {
        stats.failed++;
        console.error(`💀 Permanently failed: ${job.email}`);
      }
    }
  } finally {
    inFlight--;
  }
}

// =============================
// CLAUDE — IDENTIFY COMPANY + INDUSTRY
// =============================
async function runClaude(job) {
  const prompt = `You are a B2B data enrichment assistant.

EMAIL DOMAIN: ${job.domain}
KNOWN COMPANY NAME: ${job.company || 'Not provided'}

TASK 1 — COMPANY NAME:
Based on the email domain, identify the company name. Use the name the company is most commonly known by in the business world. If the company is more widely recognized by its acronym or shortened name, use that instead of the full legal name. Examples: "IBM" not "International Business Machines", "CAS" not "Chemical Abstracts Service", "KPMG" not "Klynveld Peat Marwick Goerdeler", "HP" not "Hewlett-Packard", "3M" not "Minnesota Mining and Manufacturing". If the full name is equally or more recognized, use that instead. Do not say "Unknown".

TASK 2 — INDUSTRY:
Based on the email domain, write a concise, specific industry label (3-6 words). Examples: "Cloud Infrastructure Software", "Commercial Real Estate Brokerage", "Digital Marketing Agency", "Community Hospital System". Be specific.

TASK 3 — INDUSTRY CATEGORY:
Map the industry to EXACTLY one of these categories (copy the label exactly as written):
- Technology & Software
- Healthcare & Life Sciences
- Financial Services & Fintech
- Real Estate & Construction
- Marketing, Media & Advertising
- Energy, Industrial & Manufacturing
- Professional Services & Consulting
- Nonprofit, Government & Public Sector
- Retail, Food & Consumer Goods
- Education
- Other

RESPOND IN THIS EXACT FORMAT, nothing else:
COMPANY: <company name>
INDUSTRY: <specific industry label>
CATEGORY: <exact category from the list>`;

  const res = await axios.post(
    'https://api.anthropic.com/v1/messages',
    {
      model: 'claude-haiku-4-5-20251001', // Haiku — faster and cheaper for structured lookups
      max_tokens: 100,
      temperature: 0,
      system: 'You are a precise B2B data enrichment assistant. Respond only in the exact format requested. No preamble or explanation.',
      messages: [{ role: 'user', content: prompt }]
    },
    {
      headers: {
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      timeout: 15000
    }
  );

  const text = res.data?.content?.find(p => p.type === 'text')?.text || '';

  const companyMatch  = text.match(/^COMPANY:\s*(.+)$/mi);
  const industryMatch = text.match(/^INDUSTRY:\s*(.+)$/mi);
  const categoryMatch = text.match(/^CATEGORY:\s*(.+)$/mi);

  const companyName    = companyMatch  ? companyMatch[1].trim()  : '';
  const industry       = industryMatch ? industryMatch[1].trim() : 'Unknown';
  let industryCategory = categoryMatch ? categoryMatch[1].trim() : 'Other';

  if (!INDUSTRY_CATEGORIES.includes(industryCategory)) {
    industryCategory = 'Other';
  }

  return { companyName, industry, industryCategory };
}

// =============================
// WRITE BACK TO HUBSPOT
// =============================
async function writeToHubSpot(contactId, companyName, industry, industryCategory) {
  const properties = {
    industry,
    industry_category: industryCategory
  };

  if (companyName) properties.company = companyName;

  // Retry HubSpot writes up to 3 times
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      hsInFlight++;
      await axios.patch(
        `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
        { properties },
        {
          headers: {
            Authorization: `Bearer ${HUBSPOT_TOKEN}`,
            'Content-Type': 'application/json'
          },
          timeout: 10000
        }
      );
      return;
    } catch (err) {
      if (attempt === 3) throw err;
      await sleep(500 * attempt);
    } finally {
      hsInFlight--;
    }
  }
}

// =============================
// HELPERS
// =============================
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =============================
// START
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Contact Enrichment running on port ${PORT}`);
  console.log(`⚡ Concurrency: ${CONCURRENCY} | Interval: ${PROCESS_INTERVAL}ms`);
});
