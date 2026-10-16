const assert=require('node:assert/strict');
const {createEngine}=require('../docs/kenya-cache.js');
const fields=['day','url','outlet','estimated_country','domain_country','countries','specific_location_countries','places'];
const rows=[
 ['2025-01-01','https://a.ke/full','a.ke','KE','KE',['CH','KE'],['KE'],['Nairobi']],
 ['2025-02-01','https://a.ke/full','a.ke','KE','KE',['CH','KE','US'],[],[]],
 ['2025-02-02','https://b.ke/partial','b.ke','KE','KE',['CH'],[],[]],
 ['2025-02-03','https://c.ke/pending','c.ke','KE','KE',['CH','KE'],['KE'],['Mombasa']],
 ['2025-02-04','https://c.ke/failed','c.ke','KE','KE',['CH'],[],[]]
];
const engine=createEngine({country:'KE',fields,rows}),s={country:'KE',start:'2025-01-01',end:'2025-12-31',mode:'broad',origin:'gdelt'};
assert.equal(engine.query('/browse',{...s,downloaded:true}).count,0);
engine.setDownloads([['https://a.ke/full','saved'],['https://b.ke/partial','partial'],['https://c.ke/failed','failed']]);
const all=engine.query('/stories',s).articles;
assert.equal(all.length,4);assert.equal(all.find(r=>r.url==='https://a.ke/full').download_status,'saved');
assert.equal(all.find(r=>r.url==='https://b.ke/partial').download_status,'partial');
assert.equal(all.find(r=>r.url==='https://c.ke/pending').download_status,null);
const selected={...s,downloaded:true},overview=engine.query('/browse',selected);
assert.equal(overview.count,2);assert.equal(overview.websites,2);
assert.deepEqual(overview.outlets.map(r=>r.outlet),['a.ke','b.ke']);
assert.deepEqual(overview.timeline,[{month:'2025-01-01',count:1},{month:'2025-02-01',count:2}]);
assert.equal(engine.query('/stories',selected).articles.find(r=>r.outlet==='a.ke').last_observed,'2025-02-01');
const pair={...selected,mode:'only_pair',place:true};
assert.equal(engine.query('/browse',pair).count,1);
assert.equal(engine.query('/stories',pair).articles[0].last_observed,'2025-01-01');
assert.equal(engine.query('/browse',{...selected,start:'2025-02-01',outlet:'a.ke'}).count,1);
engine.setDownloads([['https://c.ke/pending','saved']]);
assert.equal(engine.query('/browse',selected).count,1); // Refresh invalidates cached selection.
assert.equal(engine.query('/stories',selected).articles[0].outlet,'c.ke');
assert.equal(engine.query('/browse',{...s,downloaded:false}).count,4);
const many=createEngine({country:'KE',fields,rows:Array.from({length:45},(_,i)=>['2025-01-01','https://a.ke/'+i,'a.ke','KE','KE',['CH'],[],[]])});
many.setDownloads(Array.from({length:43},(_,i)=>['https://a.ke/'+i,'saved']));
assert.equal(many.query('/browse',selected).count,43);
assert.equal(many.query('/stories',{...selected,page:2}).articles.length,3);
console.log('Downloaded filter passed: saved/partial status, daily geography, dates, counts, timeline, refresh and pagination.');
