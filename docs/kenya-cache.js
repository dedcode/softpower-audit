(function(root){
 'use strict';
 function createEngine(payload){
  const rows=payload.rows.map(values=>Object.fromEntries(payload.fields.map((field,i)=>[field,values[i]])));
  let lastKey,lastResult;
  function select(s){
   const key=JSON.stringify([s.start,s.end,s.mode,s.origin,!!s.place,s.outlet||'']);
   if(key===lastKey)return lastResult;
   const byURL=new Map(),months=new Map();
   for(const r of rows){
    if(s.origin==='gdelt'&&r.estimated_country!=='KE'||s.origin==='cctld'&&r.domain_country!=='KE'||s.origin==='both'&&(r.estimated_country!=='KE'||r.domain_country!=='KE'))continue;
    if(s.mode!=='broad'&&!r.countries.includes('KE'))continue;
    if(s.mode==='only_pair'&&r.countries.length!==2)continue;
    if(s.place&&!r.specific_location_countries.includes('KE'))continue;
    if(!s.outlet||r.outlet===s.outlet){const month=r.day.slice(0,7)+'-01';if(!months.has(month))months.set(month,new Set());months.get(month).add(r.url);}
    if(r.day<s.start||r.day>s.end)continue;
    const previous=byURL.get(r.url);
    if(!previous)byURL.set(r.url,{...r,first_observed:r.day,last_observed:r.day});
    else{
     const first=previous.first_observed<r.day?previous.first_observed:r.day;
     if(r.day>previous.last_observed)byURL.set(r.url,{...r,first_observed:first,last_observed:r.day});
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
  return {query(path,s){
   if(path==='/article-places'){const r=rows.find(r=>r.day===s.day&&r.url===s.url);return {places:r?r.places:[]};}
   const result=select(s);
   if(path==='/browse')return result.overview;
   if(path==='/stories')return {articles:result.articles.slice(Number(s.page||0)*20,(Number(s.page||0)+1)*20)};
   throw Error('Unsupported Kenya cache request.');
  }};
 }
 let enginePromise;
 async function ensure(){
  if(!enginePromise)enginePromise=(async()=>{
   const base=new URL('data/kenya/',document.baseURI),response=await fetch(new URL('manifest.json',base));
   if(!response.ok)throw Error('The Kenya data cache could not be loaded. Please retry.');
   const manifest=await response.json(),compressed=typeof DecompressionStream!=='undefined';
   const url=new URL(compressed?manifest.gzip:manifest.json,base).href;
   let store,cached;
   try{store=await caches.open('china-news-kenya-v1');cached=await store.match(url);}catch{}
   let result=cached;
   if(!result){result=await fetch(url);if(!result.ok)throw Error('The complete Kenya data could not be downloaded. Please retry.');if(store)try{await store.put(url,result.clone());}catch{}}
   const decoded=compressed?await new Response(result.body.pipeThrough(new DecompressionStream('gzip'))).json():await result.json();
   if(decoded.rows.length!==manifest.rows)throw Error('The Kenya cache is incomplete. Please refresh.');
   return createEngine(decoded);
  })().catch(e=>{enginePromise=undefined;throw e;});
  return enginePromise;
 }
 const api={createEngine,async request(path,s){return (await ensure()).query(path,s);}};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.KenyaCache=api;
})(typeof window==='undefined'?globalThis:window);
