const assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path');
const {createEngine}=require('../docs/kenya-cache.js');
const fields=['day','url','outlet','estimated_country','domain_country','countries','specific_location_countries','places'];
const engine=createEngine({fields,rows:[
 ['2025-01-01','https://a.ke/1','a.ke','KE','KE',['CH','KE'],['KE'],['Nairobi']],
 ['2025-01-02','https://a.ke/1','a.ke','KE','KE',['CH','KE','US'],[],[]],
 ['2025-01-03','https://b.com/2','b.com','KE',null,['CH'],[],[]],
 ['2025-01-03','https://c.ke/3','c.ke','US','KE',['CH','KE'],['KE'],['Mombasa']]
]});
const s={country:'KE',start:'2025-01-01',end:'2025-01-31',mode:'broad',origin:'gdelt',place:false,outlet:''};
assert.equal(engine.query('/browse',s).count,2); // Same URL across days counted once.
assert.equal(engine.query('/browse',s).timeline[0].count,2);
assert.equal(engine.query('/stories',s).articles.find(r=>r.outlet==='a.ke').last_observed,'2025-01-02');
const pair={...s,mode:'only_pair',place:true};
assert.equal(engine.query('/browse',pair).count,1);
assert.equal(engine.query('/stories',pair).articles[0].last_observed,'2025-01-01'); // Latest QUALIFYING day.
assert.equal(engine.query('/browse',{...s,origin:'cctld'}).count,2); // Includes non-KE estimate with KE ccTLD.
assert.equal(engine.query('/browse',{...s,origin:'both'}).count,1);
const chosen=engine.query('/browse',{...s,outlet:'a.ke'});assert.equal(chosen.count,1);assert.equal(chosen.outlets.length,2);
assert.equal(engine.query('/browse',{...s,start:'2025-02-01',end:'2025-02-28'}).count,0);
assert.deepEqual(engine.query('/article-places',{day:'2025-01-01',url:'https://a.ke/1'}).places,['Nairobi']);
const dir=path.join(__dirname,'../docs/data/kenya');
const manifest=JSON.parse(fs.readFileSync(path.join(dir,'manifest.json')));
const payload=JSON.parse(fs.readFileSync(path.join(dir,manifest.json)));assert.equal(payload.rows.length,manifest.rows);
const full=createEngine(payload);
const all={...s,start:'2015-01-01',end:'2025-12-31'};
assert.equal(full.query('/browse',all).count,85993);assert.equal(full.query('/browse',all).websites,194);
assert.equal(full.query('/browse',s).count,429);assert.equal(full.query('/browse',{...s,mode:'only_pair'}).count,54);
assert.equal(full.query('/browse',{...s,mode:'only_pair',place:true}).count,39);
assert.equal(full.query('/browse',{...s,origin:'cctld'}).count,334);
assert.equal(full.query('/browse',{...s,origin:'both'}).count,334);
assert.equal(full.query('/stories',all).articles.length,20);
const first=new Set(full.query('/stories',all).articles.map(r=>r.url));assert(full.query('/stories',{...all,page:1}).articles.every(r=>!first.has(r.url)));
console.log('Kenya cache passed: complete counts, daily evidence, all origin methods, geography, dates, pagination and local places.');
