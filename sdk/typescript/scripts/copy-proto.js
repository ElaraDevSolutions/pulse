const fs = require('fs');
const path = require('path');

const src = path.resolve(__dirname, '../src/proto/pulse.proto');
const destDir = path.resolve(__dirname, '../dist/proto');
const dest = path.join(destDir, 'pulse.proto');

if (!fs.existsSync(src)) {
  console.error('source proto not found:', src);
  process.exit(1);
}

if (!fs.existsSync(destDir)) fs.mkdirSync(destDir, { recursive: true });
fs.copyFileSync(src, dest);
console.log('copied proto to', dest);
