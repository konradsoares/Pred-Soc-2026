const fs = require('fs');
const path = require('path');
const { sendTipsEmail } = require('../lib/mailer');

const ROOT_DIR = path.resolve(__dirname, '../..');
const CONFIG_PATH = path.join(ROOT_DIR, 'config', 'app.config.json');
const OUTPUT_DIR = path.join(ROOT_DIR, 'output');

const WINDOW = process.argv[2] || process.env.PREDICTION_WINDOW || 'daily';

function loadConfig() {
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function todayDateISO() {
  return new Date().toISOString().slice(0, 10);
}

async function main() {
  const config = loadConfig();
  const targetDate = process.argv[3] || todayDateISO();

  const filePath = path.join(OUTPUT_DIR, `tips-${targetDate}-${WINDOW}.json`);

  if (!fs.existsSync(filePath)) {
    throw new Error(`Tips file not found: ${filePath}`);
  }

  const tipsFile = JSON.parse(fs.readFileSync(filePath, 'utf8'));

  if (tipsFile.date !== targetDate) {
    throw new Error(`Tips file date mismatch. Expected ${targetDate}, got ${tipsFile.date}`);
  }

  if (tipsFile.window !== WINDOW) {
    throw new Error(`Tips file window mismatch. Expected ${WINDOW}, got ${tipsFile.window}`);
  }

  await sendTipsEmail(config, tipsFile);
  console.log(`Email sent for ${targetDate} / ${WINDOW}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
