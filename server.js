const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

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
// QUEUE & STATS
// =============================
let queue = [];
let inFlight = 0;
const CONCURRENCY = 5;
const PROCESS_INTERVAL_MS = 500;

let stats = { processed: 0, skipped: 0, failed: 0 };

// Reset processed count every 30 minutes
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
    stats
  });
});

// =============================
// DASHBOARD
// =============================
app.get('/dashboard', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>TPG Contact Enrichment</title>
      <meta http-equiv="refresh" content="5">
      <style>
        body { font-family: monospace; background: #0f0f0f; color: #23cfb0; padding: 40px; }
        h1 { font-size: 18px; margin-bottom: 30px; color: #fff; }
        .grid { display: flex; gap: 60px; margin-bottom: 40px; }
        .stat { font-size: 64px; font-weight: bold; line-height: 1; }
        .label { font-size: 13px; color: #555; margin-top: 8px; }
        .green { color: #a2cf23; } .teal { color: #23cfb0; }
        .orange { color: #f0a500; } .red { color: #e05252; } .grey { color: #333; }
        .footer { font-size: 12px; color: #333; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
      </style>
    </head>
    <body>
      <h1>TPG Contact Enrichment &mdash; Monitor</h1>
      <div class="grid">
        <div><div class="stat ${queue.length > 0 ? 'orange' : 'grey'}">${queue.length}</div><div class="label">waiting in queue</div></div>
        <div><div class="stat teal">${inFlight}</div><div class="label">in-flight</div></div>
        <div><div class="stat green">${stats.processed}</div><div class="label">enriched</div></div>
        <div><div class="stat teal">${stats.skipped}</div><div class="label">skipped (personal email)</div></div>
        <div><div class="stat ${stats.failed > 0 ? 'red' : 'grey'}">${stats.failed}</div><div class="label">failed</div></div>
      </div>
      <div class="footer">Last refreshed: ${new Date().toLocaleTimeString()} &nbsp;·&nbsp; Auto-refreshes every 5 seconds</div>
    </body>
    </html>
  `);
});

// =============================
// ENQUEUE ENDPOINT
// Called from HubSpot workflow custom code step
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
    console.log(`⏭️  Skipped personal email: ${email}`);
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
}, PROCESS_INTERVAL_MS);

// =============================
// PROCESS ONE CONTACT
// =============================
async function processJob(job) {
  try {
    console.log(`🔍 Enriching: ${job.email}`);

    const { companyName, industry, industryCategory } = await runClaude(job);

    await writeToHubSpot(job.contactId, companyName, industry, industryCategory);

    stats.processed++;
    console.log(`✅ ${job.email} → "${companyName}" / "${industry}" / "${industryCategory}"`);

  } catch (err) {
    console.error(`❌ Failed: ${job.email} — ${err.message}`);

    if (err.response?.status === 429) {
      queue.push(job); // rate limited — requeue
    } else {
      job.retries = (job.retries || 0) + 1;
      if (job.retries <= 2) {
        queue.push(job);
      } else {
        stats.failed++;
      }
    }
  } finally {
    inFlight--;
  }
}

// =============================
// CLAUDE — IDENTIFY INDUSTRY
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
COMPANY: <full company name>
INDUSTRY: <specific industry label>
CATEGORY: <exact category from the list>`;

  const res = await axios.post(
    'https://api.anthropic.com/v1/messages',
    {
      model: 'claude-sonnet-4-20250514',
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
      timeout: 20000
    }
  );

  const text = res.data?.content?.find(p => p.type === 'text')?.text || '';

  const companyMatch   = text.match(/^COMPANY:\s*(.+)$/mi);
  const industryMatch  = text.match(/^INDUSTRY:\s*(.+)$/mi);
  const categoryMatch  = text.match(/^CATEGORY:\s*(.+)$/mi);

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

  // Only write company if HubSpot field is blank — don't overwrite existing data
  if (companyName) properties.company = companyName;

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
}

// =============================
// START
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Contact Enrichment service running on port ${PORT}`);
});
