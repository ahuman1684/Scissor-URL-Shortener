const ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
const MACHINE_ID = parseInt(process.env.MACHINE_ID || '1'); // 0-1023
const EPOCH = 1700000000000n; // custom epoch (Nov 2023)

let sequence = 0n;
let lastTimestamp = -1n;

function generateId() {
  let timestamp = BigInt(Date.now()) - EPOCH;
  if (timestamp === lastTimestamp) {
    sequence = (sequence + 1n) & 0xFFFn;
    if (sequence === 0n) {
      while (BigInt(Date.now()) - EPOCH <= lastTimestamp) {}
      timestamp = BigInt(Date.now()) - EPOCH;
    }
  } else {
    sequence = 0n;
  }
  lastTimestamp = timestamp;
  const id = (timestamp << 22n) | (BigInt(MACHINE_ID) << 12n) | sequence;
  return toBase62(id);
}

function toBase62(num) {
  let result = '';
  while (num > 0n) {
    result = ALPHABET[Number(num % 62n)] + result;
    num = num / 62n;
  }
  return result || '0';
}

module.exports = { generateId };
