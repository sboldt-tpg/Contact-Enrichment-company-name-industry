const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const HUBSPOT_TOKEN     = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// =============================
// CONFIG — Tuned for Anthropic Tier 4 + Render $7
// =============================
const CONCURRENCY         = 25;   // Conservative — avoids Tier 4 rate limits
const PROCESS_INTERVAL    = 100;  // Check queue every 100ms
const HUBSPOT_CONCURRENCY = 20;   // Parallel HubSpot write-backs
const MAX_RETRIES         = 3;

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
// domainCache stores: { companyName, industry } — the raw enrichment data
// industryCategory is always derived fresh from whatever industry we end up using
// =============================
const domainCache = new Map();

// =============================
// QUEUE & STATS
// =============================
let queue    = [];
let inFlight = 0;
let hsInFlight = 0;

let stats = {
  processed:    0,
  skipped:      0,
  failed:       0,
  cacheHits:    0,
  companyKept:  0,  // had company already — skipped that lookup
  industryKept: 0   // had industry already — skipped that lookup
};

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
        .yellow { color: #f0d500; }
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
          <div><div class="stat teal">${stats.skipped}</div><div class="label">skipped (personal email)</div></div>
          <div><div class="stat blue">${stats.cacheHits}</div><div class="label">cache hits</div></div>
          <div><div class="stat yellow">${stats.companyKept}</div><div class="label">company pre-filled</div></div>
          <div><div class="stat yellow">${stats.industryKept}</div><div class="label">industry pre-filled</div></div>
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
  // Accept company and industry from the workflow so we know what's already filled
  const { contactId, email, company, industry, industry_category } = req.body;

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

  queue.push({
    contactId,
    email,
    domain,
    existingCompany:          company           ? company.trim()           : '',
    existingIndustry:         industry          ? industry.trim()          : '',
    existingIndustryCategory: industry_category ? industry_category.trim() : '',
    retries: 0
  });

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
    // Determine what we actually need to look up
    const needsCompany  = !job.existingCompany;
    const needsIndustry = !job.existingIndustry;

    let companyName      = job.existingCompany;
    let industry         = job.existingIndustry;

    if (!needsCompany)  stats.companyKept++;
    if (!needsIndustry) stats.industryKept++;

    // Only call Claude if we need at least one of the two fields
    if (needsCompany || needsIndustry) {
      if (domainCache.has(job.domain)) {
        // Pull from cache — take only the fields we actually need
        const cached = domainCache.get(job.domain);
        if (needsCompany  && cached.companyName) companyName = cached.companyName;
        if (needsIndustry && cached.industry)    industry    = cached.industry;
        stats.cacheHits++;
        console.log(`💾 Cache hit: ${job.domain} (company=${!needsCompany ? 'kept' : 'from cache'}, industry=${!needsIndustry ? 'kept' : 'from cache'})`);
      } else {
        // Ask Claude only for what's missing
        const result = await runClaude(job, needsCompany, needsIndustry);
        if (needsCompany)  companyName = result.companyName;
        if (needsIndustry) industry    = result.industry;

        // Cache the raw enrichment data for this domain
        domainCache.set(job.domain, {
          companyName: needsCompany  ? result.companyName : '',
          industry:    needsIndustry ? result.industry    : ''
        });
      }
    }

    // Industry category — skip if already filled, otherwise derive from industry
    const industryCategory = job.existingIndustryCategory
      ? job.existingIndustryCategory
      : await getIndustryCategory(industry, job.domain);

    const categoryChanged = !job.existingIndustryCategory;
    await writeToHubSpot(job.contactId, needsCompany ? companyName : null, needsIndustry ? industry : null, categoryChanged ? industryCategory : null);

    stats.processed++;
    console.log(`✅ ${job.email} → company="${needsCompany ? companyName : '(kept)'}" / industry="${needsIndustry ? industry : '(kept)'}" / category="${industryCategory}"`);

  } catch (err) {
    console.error(`❌ Failed: ${job.email} — ${err.message}`);

    if (err.response?.status === 429) {
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
// Only asks for the fields that are actually missing
// =============================
async function runClaude(job, needsCompany, needsIndustry) {
  // Build the prompt dynamically based on what we need
  const tasks = [];

  if (needsCompany) {
    tasks.push(`TASK: COMPANY NAME
Based on the email domain, identify the company name. Use the name the company is most commonly known by in the business world. Prefer commonly known acronyms over full legal names (e.g. "IBM" not "International Business Machines", "HP" not "Hewlett-Packard", "KPMG" not "Klynveld Peat Marwick Goerdeler"). Do not say "Unknown".`);
  }

  if (needsIndustry) {
    tasks.push(`TASK: INDUSTRY
Based on the email domain, write a concise, specific industry label (3–6 words). Examples: "Cloud Infrastructure Software", "Commercial Real Estate Brokerage", "Digital Marketing Agency", "Community Hospital System". Be specific.`);
  }

  const responseFormat = [];
  if (needsCompany)  responseFormat.push('COMPANY: <company name>');
  if (needsIndustry) responseFormat.push('INDUSTRY: <specific industry label>');

  const prompt = `You are a B2B data enrichment assistant.

EMAIL DOMAIN: ${job.domain}

${tasks.join('\n\n')}

RESPOND IN THIS EXACT FORMAT, nothing else:
${responseFormat.join('\n')}`;

  const res = await axios.post(
    'https://api.anthropic.com/v1/messages',
    {
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 80,
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

  return {
    companyName: companyMatch  ? companyMatch[1].trim()  : '',
    industry:    industryMatch ? industryMatch[1].trim() : 'Unknown'
  };
}

// =============================
// CLAUDE — CATEGORIZE INDUSTRY
// Separate, lightweight call — always runs so category stays in sync
// =============================
async function getIndustryCategory(industry, domain) {
  const prompt = `You are a B2B data enrichment assistant.

INDUSTRY: ${industry}
EMAIL DOMAIN: ${domain}

Map this industry to EXACTLY one of these categories (copy the label exactly as written):
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
CATEGORY: <exact category from the list>`;

  try {
    const res = await axios.post(
      'https://api.anthropic.com/v1/messages',
      {
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 30,
        temperature: 0,
        system: 'You are a precise B2B data categorization assistant. Respond only in the exact format requested.',
        messages: [{ role: 'user', content: prompt }]
      },
      {
        headers: {
          'x-api-key': ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json'
        },
        timeout: 10000
      }
    );

    const text = res.data?.content?.find(p => p.type === 'text')?.text || '';
    const match = text.match(/^CATEGORY:\s*(.+)$/mi);
    const category = match ? match[1].trim() : 'Other';
    return INDUSTRY_CATEGORIES.includes(category) ? category : 'Other';
  } catch {
    return 'Other';
  }
}

// =============================
// WRITE BACK TO HUBSPOT
// Only writes fields that were actually changed
// =============================
async function writeToHubSpot(contactId, companyName, industry, industryCategory) {
  const properties = {};

  if (companyName      !== null) properties.company           = companyName;
  if (industry         !== null) properties.industry          = industry;
  if (industryCategory !== null) properties.industry_category = industryCategory;

  // Nothing to write — all fields were pre-filled
  if (Object.keys(properties).length === 0) return;

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
