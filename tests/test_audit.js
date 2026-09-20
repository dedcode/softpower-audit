const assert=require('node:assert/strict');
const A=require('../docs/audit.js');
assert.equal(A.bucket('2025-01-05','week'),'2024-12-30');
assert.equal(A.bucket('2025-01-06','week'),'2025-01-06');
const data={start:'2025-01-01',end:'2025-02-28',interval:'month',timeline:[
  {period:'2025-01-01',group:'Chinese',articles:100,active_outlets:5},
  {period:'2025-01-01',group:'Local',articles:30,active_outlets:3},
  {period:'2025-02-01',group:'Chinese',articles:100,active_outlets:5}
]};
const p=A.periods(data,['2025-02-15']);
assert.equal(p[1].groups.Local.articles,0);
assert.equal(p[1].available_days,27);
const result=A.assess(p,3,20,80);
assert.equal(result.complete,1);assert.equal(result.passing,1);assert.equal(result.excluded,1);assert.equal(result.percent,100);
const noGaps=A.assess(A.periods(data,[]),3,20,80);
assert.equal(noGaps.percent,50);assert.equal(noGaps.meets,false);
const daily=A.periods({...data,start:'2025-01-01',end:'2025-01-02',interval:'day',timeline:[]},['2025-01-01','2025-01-02']);
assert.equal(A.assess(daily,1,1,50).percent,null);
assert.equal(A.periods({...data,start:'2024-02-01',end:'2024-02-29',timeline:[]},[])[0].expected_days,29);
assert(A.csv([{a:'=SUM(1)',b:'x,"y'}],['a','b']).includes("'=SUM(1)"));
console.log('Audit tests passed: calendar grouping, leap years, source gaps, absent groups, thresholds, and safe CSV.');
