const fs = require('fs');
const path = require('path');
const { sendTipsEmail } = require('../lib/mailer');

const ROOT_DIR = path.resolve(__dirname, '../..');
const CONFIG_PATH = path.join(ROOT_DIR, 'config', 'app.config.json');
const OUTPUT_DIR = path.join(ROOT_DIR, 'output');

function loadConfig() {
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function todayDateISO() {
  const d = new Date();
  d.setDate(d.getDate());
  return d.toISOString().slice(0, 10);
}

async function main() {
  const config = loadConfig();
  const targetDate = todayDateISO();
  const filePath = path.join(OUTPUT_DIR, `tips-${targetDate}.json`);

  if (!fs.existsSync(filePath)) {
    throw new Error(`Tips file not found: ${filePath}`);
  }

  const tipsFile = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  if (tipsFile.date !== targetDate) {
    throw new Error(`Tips file date mismatch. Expected ${targetDate}, got ${tipsFile.date}`);
  }
  await sendTipsEmail(config, tipsFile);
  console.log(`Email sent for ${targetDate}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
