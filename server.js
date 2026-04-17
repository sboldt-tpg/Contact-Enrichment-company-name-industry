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
// PLACEHOLDER VALUES — treat these as blank and re-enrich
// Lowercase comparison — values are lowercased before checking
// =============================
const PLACEHOLDER_COMPANY_VALUES = new Set([
  'company placeholder',
  'n/a',
  'na',
  'unknown',
  'none',
  'not provided',
  '-',
  '--',
  'tbd',
  'test'
]);

const PLACEHOLDER_INDUSTRY_VALUES = new Set([
  'services',       // too vague to be useful
  'other',
  'n/a',
  'na',
  'unknown',
  'none',
  'not provided',
  '-',
  '--',
  'tbd',
  'test'
]);

// Helper — returns true if the value is missing or a known placeholder
function isBlank(value, placeholderSet) {
  if (!value || !value.trim()) return true;
  return placeholderSet.has(value.trim().toLowerCase());
}

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
// DOMAIN CACHE
// Stores only what was actually fetched from Claude for a given domain.
// Fields are only set if Claude was actually asked for them — never '' as a placeholder.
// Structure: { companyName?: string, industry?: string }
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
  companyKept:  0,
  industryKept: 0,
  categoryKept: 0
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
        .sub { font-size: 12px; color: #888; margin-bottom: 30px; }
        .grid { display: flex; gap: 50px; margin-bottom: 40px; flex-wrap: wrap; }
        .stat { font-size: 56px; font-weight: bold; line-height: 1; }
        .label { font-size: 12px; color: #999; margin-top: 8px; }
        .green { color: #a2cf23; } .teal { color: #23cfb0; }
        .orange { color: #f0a500; } .red { color: #e05252; }
        .grey { color: #aaa; } .blue { color: #238acf; }
        .yellow { color: #f0d500; }
        .section { margin-bottom: 32px; }
        .section-title { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 2px; margin-bottom: 14px; border-bottom: 1px solid #1a1a1a; padding-bottom: 6px; }
        .footer { font-size: 12px; color: #666; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
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
          <div><div class="stat yellow">${stats.categoryKept}</div><div class="label">category pre-filled</div></div>
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

  // Treat placeholder/junk values as blank so they get re-enriched
  const cleanCompany  = isBlank(company,  PLACEHOLDER_COMPANY_VALUES)  ? '' : company.trim();
  const cleanIndustry = isBlank(industry, PLACEHOLDER_INDUSTRY_VALUES) ? '' : industry.trim();
  const cleanCategory = !industry_category || !industry_category.trim() ? '' : industry_category.trim();

  if (cleanCompany  !== (company  || '').trim()) console.log(`🧹 Placeholder company cleared: "${company}" for ${email}`);
  if (cleanIndustry !== (industry || '').trim()) console.log(`🧹 Placeholder industry cleared: "${industry}" for ${email}`);

  queue.push({
    contactId,
    email,
    domain,
    existingCompany:          cleanCompany,
    existingIndustry:         cleanIndustry,
    existingIndustryCategory: cleanCategory,
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
    const needsCompany  = !job.existingCompany;
    const needsIndustry = !job.existingIndustry;
    const needsCategory = !job.existingIndustryCategory;

    if (!needsCompany)  stats.companyKept++;
    if (!needsIndustry) stats.industryKept++;
    if (!needsCategory) stats.categoryKept++;

    let companyName      = job.existingCompany;
    let industry         = job.existingIndustry;
    let industryCategory = job.existingIndustryCategory;

    // ── Step 1: Resolve company + industry (from cache or Claude) ──
    if (needsCompany || needsIndustry) {
      const cached = domainCache.get(job.domain);

      // Check if cache can satisfy everything we need
      const cacheHasCompany  = cached && cached.companyName;
      const cacheHasIndustry = cached && cached.industry;
      const cacheCoversNeeds = (!needsCompany || cacheHasCompany) && (!needsIndustry || cacheHasIndustry);

      if (cached && cacheCoversNeeds) {
        // Cache fully covers what we need
        if (needsCompany)  companyName = cached.companyName;
        if (needsIndustry) industry    = cached.industry;
        stats.cacheHits++;
        console.log(`💾 Cache hit: ${job.domain}`);
      } else {
        // Need to ask Claude — only ask for fields not already in cache
        const askCompany  = needsCompany  && !cacheHasCompany;
        const askIndustry = needsIndustry && !cacheHasIndustry;

        const result = await runClaude(job, askCompany, askIndustry);

        // Apply Claude results
        if (askCompany)  companyName = result.companyName;
        if (askIndustry) industry    = result.industry;

        // Pull remaining needed fields from cache if available
        if (needsCompany  && !askCompany  && cacheHasCompany)  companyName = cached.companyName;
        if (needsIndustry && !askIndustry && cacheHasIndustry) industry    = cached.industry;

        // Update cache — only store fields we actually fetched; never overwrite with ''
        const updatedCache = { ...(cached || {}) };
        if (askCompany  && result.companyName) updatedCache.companyName = result.companyName;
        if (askIndustry && result.industry)    updatedCache.industry    = result.industry;
        domainCache.set(job.domain, updatedCache);
      }
    }

    // ── Step 2: Resolve industry category ──
    if (needsCategory) {
      const industryForCategorization = industry || job.existingIndustry;
      if (industryForCategorization) {
        industryCategory = await getIndustryCategory(industryForCategorization, job.domain);
      } else {
        industryCategory = 'Other';
        console.warn(`⚠️  No industry available for categorization: ${job.domain} — defaulting to Other`);
      }
    }

    // ── Step 3: Write only changed fields to HubSpot ──
    await writeToHubSpot(
      job.contactId,
      needsCompany  && companyName      ? companyName      : null,
      needsIndustry && industry         ? industry         : null,
      needsCategory && industryCategory ? industryCategory : null
    );

    stats.processed++;
    console.log(
      `✅ ${job.email} → ` +
      `company="${needsCompany  ? companyName      : '(kept)'}" / ` +
      `industry="${needsIndustry ? industry         : '(kept)'}" / ` +
      `category="${needsCategory ? industryCategory : '(kept)'}"`
    );

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
    industry:    industryMatch ? industryMatch[1].trim() : ''
  };
}

// =============================
// CLAUDE — CATEGORIZE INDUSTRY
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
// Only writes fields passed as non-null with a non-empty value
// =============================
async function writeToHubSpot(contactId, companyName, industry, industryCategory) {
  const properties = {};

  if (companyName)      properties.company           = companyName;
  if (industry)         properties.industry          = industry;
  if (industryCategory) properties.industry_category = industryCategory;

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
