(function(root){
 'use strict';
 const snapshots=new Map();
 async function ensure(country){
  const cached=snapshots.get(country);
  if(cached&&Date.now()-cached.at<60000)return cached.promise;
  const entry={at:Date.now(),promise:(async()=>{
   const response=await fetch(root.AUDIT_CONFIG.api+'/downloads?country='+country);
   if(!response.ok)throw Error('Download status is temporarily unavailable. Please retry.');
   const result=await response.json();
   if(result.country!==country||!Array.isArray(result.entries)||result.entries.some(r=>!Array.isArray(r)||typeof r[0]!=='string'||!['saved','partial'].includes(r[1])))throw Error('Download status could not be read. Please retry.');
   await root.KenyaCache.setDownloads(country,result.entries);
   return result;
  })()};
  snapshots.set(country,entry);
  try{return await entry.promise;}catch(e){if(snapshots.get(country)===entry)snapshots.delete(country);throw e;}
 }
 root.DownloadedArticles={ensure};
})(window);
