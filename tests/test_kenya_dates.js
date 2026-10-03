'use strict';
const assert = require('node:assert/strict');
const dates = require('../docs/kenya/dates.js');
const sample = require('../docs/kenya/data.json');

assert.equal(dates.valid('2024-02-29'), true);
assert.equal(dates.valid('2025-02-29'), false);
assert.equal(dates.valid('2025-04-31'), false);
assert.equal(dates.valid(null), false);
assert.deepEqual(dates.days('2024-02-28', '2024-03-01'), ['2024-02-28', '2024-02-29', '2024-03-01']);

// A URL first observed before the selection still qualifies if seen again inside it.
const repeated = {days: ['2025-01-01', '2025-01-10', '2025-01-10', '2025-01-20']};
assert.equal(dates.matches(repeated, '2025-01-10', '2025-01-10'), true);
assert.equal(dates.matches(repeated, '2025-01-11', '2025-01-19'), false);
assert.equal(dates.first(repeated, '2025-01-02', '2025-01-20'), '2025-01-10');
assert.equal([repeated].filter(r => dates.matches(r, '2025-01-01', '2025-01-31')).length, 1);
assert.deepEqual(dates.histogram([repeated, {days: ['2025-01-10']}], ['2025-01-10', '2025-01-11']), [
  {day: '2025-01-10', count: 2}, {day: '2025-01-11', count: 0}
]);

// Reconcile the browser's date shortcuts against the saved January pilot.
for (const [start, end, total, outlets, capital] of [
  ['2025-01-01', '2025-01-31', 429, 17, 88],
  ['2025-01-01', '2025-01-07', 63, 11, 16],
  ['2025-01-25', '2025-01-31', 101, 9, 21],
  ['2025-01-15', '2025-01-15', 20, 10, 2]
]) {
  const rows = sample.articles.filter(r => dates.matches(r, start, end));
  assert.equal(rows.length, total);
  assert.equal(new Set(rows.map(r => r.outlet)).size, outlets);
  assert.equal(rows.filter(r => r.outlet === 'capitalfm.co.ke').length, capital);
}
console.log('Kenya date filtering and pilot totals pass.');
