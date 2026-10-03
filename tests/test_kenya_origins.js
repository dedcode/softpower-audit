'use strict';
const assert=require('node:assert/strict');
const {matches}=require('../docs/kenya/origins.js');
const data=require('../docs/kenya/data.json');

// Missing ccTLD evidence must not become evidence of agreement.
assert.equal(matches({estimated_country:'KE',domain_country:''},'both'),false);
assert.equal(matches({estimated_country:'KE',domain_country:''},'gdelt'),true);
// Each individual method stands on its own when the two disagree.
const conflict={estimated_country:'US',domain_country:'KE'};
assert.equal(matches(conflict,'gdelt'),false);
assert.equal(matches(conflict,'cctld'),true);
assert.equal(matches(conflict,'both'),false);
assert.equal(matches({},'both'),false);

for(const [method,urls,websites,pairUrls] of [['gdelt',429,17,54],['cctld',334,7,46],['both',334,7,46]]){
 const rows=data.articles.filter(r=>matches(r,method));
 assert.equal(rows.length,urls);
 assert.equal(new Set(rows.map(r=>r.outlet)).size,websites);
 assert.equal(rows.filter(r=>r.only_pair).length,pairUrls);
 assert.deepEqual([...new Set(rows.map(r=>r.outlet))].sort(),data.outlets.filter(r=>matches(r,method)).map(r=>r.outlet).sort());
}
console.log('Kenya origin filters pass: missing evidence, disagreement, and saved pilot totals.');
