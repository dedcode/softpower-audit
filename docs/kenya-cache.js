(function(root){
 'use strict';
 function createEngine(payload){
  const country=payload.country||'KE';
  const rows=payload.rows.map(values=>Object.fromEntries(payload.fields.map((field,i)=>[field,values[i]])));
  let lastKey,lastResult,downloads=new Map();
  function select(s){
   const key=JSON.stringify([s.start,s.end,s.mode,s.origin,!!s.place,s.outlet||'',!!s.downloaded]);
   if(key===lastKey)return lastResult;
   const byURL=new Map(),months=new Map();
   for(const r of rows){
    if(s.downloaded&&!downloads.has(r.url))continue;
    if(s.origin==='gdelt'&&r.estimated_country!==country||s.origin==='cctld'&&r.domain_country!==country||s.origin==='both'&&(r.estimated_country!==country||r.domain_country!==country))continue;
    if(s.mode!=='broad'&&!r.countries.includes(country))continue;
    if(s.mode==='only_pair'&&r.countries.length!==2)continue;
    if(s.place&&!r.specific_location_countries.includes(country))continue;
    if(!s.outlet||r.outlet===s.outlet){const month=r.day.slice(0,7)+'-01';if(!months.has(month))months.set(month,new Set());months.get(month).add(r.url);}
    if(r.day<s.start||r.day>s.end)continue;
    const previous=byURL.get(r.url);
    if(!previous)byURL.set(r.url,{...r,download_status:downloads.get(r.url)||null,first_observed:r.day,last_observed:r.day});
    else{
     const first=previous.first_observed<r.day?previous.first_observed:r.day;
     if(r.day>previous.last_observed)byURL.set(r.url,{...r,download_status:downloads.get(r.url)||null,first_observed:first,last_observed:r.day});
     else previous.first_observed=first;
    }
   }
   const counts=new Map(),selected=[];
   for(const r of byURL.values()){counts.set(r.outlet,(counts.get(r.outlet)||0)+1);if(!s.outlet||r.outlet===s.outlet)selected.push(r);}
   selected.sort((a,b)=>a.last_observed===b.last_observed?(a.url<b.url?-1:a.url>b.url?1:0):(a.last_observed>b.last_observed?-1:1));
   const outlets=[...counts].map(([outlet,count])=>({outlet,count})).sort((a,b)=>b.count-a.count||a.outlet.localeCompare(b.outlet));
   lastKey=key;lastResult={articles:selected,overview:{outlets,count:selected.length,websites:new Set(selected.map(r=>r.outlet)).size,timeline:[...months].map(([month,urls])=>({month,count:urls.size})).sort((a,b)=>a.month.localeCompare(b.month))}};
   return lastResult;
  }
  return {setDownloads(entries){downloads=new Map(entries.filter(([,status])=>status==='saved'||status==='partial'));lastKey=undefined;},query(path,s){
   if(path==='/article-places'){const r=rows.find(r=>r.day===s.day&&r.url===s.url);return {places:r?r.places:[]};}
   const result=select(s);
   if(path==='/browse')return result.overview;
   if(path==='/stories')return {articles:result.articles.slice(Number(s.page||0)*20,(Number(s.page||0)+1)*20)};
   throw Error('Unsupported country cache request.');
  }};
 }
 const sources={KE:'kenya',CI:'chile',RP:'philippines'},engines=new Map();
 async function ensure(country){
  if(!sources[country])throw Error('Choose a cached source country.');
  if(!engines.has(country))engines.set(country,(async()=>{
   const base=new URL('data/'+sources[country]+'/',document.baseURI),response=await fetch(new URL('manifest.json',base),{cache:'no-cache'});
   if(!response.ok)throw Error('The country data cache could not be loaded. Please retry.');
   const manifest=await response.json(),compressed=typeof DecompressionStream!=='undefined';
   if(manifest.country&&manifest.country!==country)throw Error('Wrong country cache.');
   const parts=manifest.parts||[manifest],rows=[];let fields;
   let store;try{store=await caches.open('china-news-countries-v2');}catch{}
   for(const part of parts){
    const url=new URL(compressed?part.gzip:part.json,base).href;
    let result;try{result=store&&await store.match(url);}catch{}
    if(!result){result=await fetch(url);if(!result.ok)throw Error('The complete country data could not be downloaded. Please retry.');if(store)try{await store.put(url,result.clone());}catch{}}
    const decoded=compressed?await new Response(result.body.pipeThrough(new DecompressionStream('gzip'))).json():await result.json();
    if(decoded.rows.length!==part.rows)throw Error('The country cache is incomplete. Please refresh.');
    if(fields&&JSON.stringify(fields)!==JSON.stringify(decoded.fields))throw Error('Inconsistent country cache fields.');
    fields=decoded.fields;for(const row of decoded.rows)rows.push(row);
   }
   if(rows.length!==manifest.rows)throw Error('The country cache is incomplete. Please refresh.');
   return createEngine({country,fields,rows});
  })().catch(e=>{engines.delete(country);throw e;}));
  return engines.get(country);
 }
 const api={createEngine,async setDownloads(country,entries){(await ensure(country)).setDownloads(entries);},async request(path,s){return (await ensure(s.country||'KE')).query(path,s);}};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.KenyaCache=api;
})(typeof window==='undefined'?globalThis:window);
