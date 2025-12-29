// Jest runs this file in Node (not ts-jest). Use the built output in `dist`.
const mod = require('../dist/index');

module.exports = async () => {
  try {
    if (mod && mod.shutdownAll) {
      await mod.shutdownAll();
    }
  } catch (err) {
    // ignore teardown errors
  }
};
