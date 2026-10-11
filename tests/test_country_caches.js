const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto');
const {createEngine}=require('../docs/kenya-cache.js');
const fields=['day','url','outlet','estimated_country','domain_country','countries','specific_location_countries','places'];
for(const [country,folder,place] of [['CI','chile','Santiago'],['RP','philippines','Manila']]){
 const fixture=createEngine({country,fields,rows:[
 ['2025-01-01','https://local/1','local',country,country,['CH',country],[country],[place]],
 ['2025-01-02','https://local/1','local',country,country,['CH',country,'US'],[],[]],
 ['2025-01-03','https://local/2','local',country,null,['CH'],[],[]],
 ['2025-01-03','https://other/3','other','US',country,['CH',country],[country],[place]]
 ]});
 const s={country,start:'2015-01-01',end:'2025-12-31',origin:'gdelt',mode:'broad',place:false};
 assert.equal(fixture.query('/browse',s).count,2);
 assert.equal(fixture.query('/browse',{...s,mode:'only_pair',place:true}).count,1);
 assert.equal(fixture.query('/browse',{...s,origin:'both'}).count,1);
 assert.equal(fixture.query('/browse',{...s,origin:'cctld'}).count,2);
 assert.deepEqual(fixture.query('/article-places',{day:'2025-01-01',url:'https://local/1'}).places,[place]);
 const dir=path.join(__dirname,'../docs/data',folder),m=JSON.parse(fs.readFileSync(path.join(dir,'manifest.json'))),rows=[];
 for(const part of m.parts){
  const raw=fs.readFileSync(path.join(dir,part.json));assert(raw.length<5*1024*1024);
  assert.equal(crypto.createHash('sha256').update(raw).digest('hex'),part.sha256);
  assert.deepEqual(zlib.gunzipSync(fs.readFileSync(path.join(dir,part.gzip))),raw);
  const p=JSON.parse(raw);assert.equal(p.country,country);assert.equal(p.rows.length,part.rows);for(const r of p.rows)rows.push(r);
 }
 assert.equal(rows.length,m.rows);const engine=createEngine({country,fields,rows});
 const all=engine.query('/browse',s);assert.equal(all.count,m.gdelt_unique_urls);
 for(const origin of ['gdelt','cctld','both']){
  const qualifying=rows.filter(r=>(origin!=='gdelt'||r[3]===country)&&(origin!=='cctld'||r[4]===country)&&(origin!=='both'||r[3]===country&&r[4]===country));
  assert.equal(engine.query('/browse',{...s,origin}).count,new Set(qualifying.map(r=>r[1])).size);
  const pair=qualifying.filter(r=>r[5].includes(country)&&r[5].length===2&&r[6].includes(country));
  assert.equal(engine.query('/browse',{...s,origin,mode:'only_pair',place:true}).count,new Set(pair.map(r=>r[1])).size);
 }
 console.log(folder,JSON.stringify({rows:m.rows,urls:all.count,websites:all.websites,compressedBytes:m.parts.reduce((n,p)=>n+p.gzip_bytes,0)}));
}
