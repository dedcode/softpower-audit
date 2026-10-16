'use strict';
const $=id=>document.getElementById(id),params=new URLSearchParams(location.search),country=params.get('country')||'KE',url=params.get('url')||'';
const back=new URL('../',location.href),backParams=new URLSearchParams(params.get('back')||'');
for(const key of ['country','source','start','end','mode','country-source','place','downloaded'])if(backParams.has(key))back.searchParams.set(key,backParams.get(key));
back.searchParams.set('country',country);$('back').href=back.href;
function safeURL(value){try{const parsed=new URL(value);return ['http:','https:'].includes(parsed.protocol)&&!parsed.username&&!parsed.password?parsed.href:null;}catch{return null;}}
async function load(){
 $('status').hidden=false;$('status').textContent='Loading saved text…';$('retry').hidden=true;
 try{
  if(!['KE','CI','RP'].includes(country)||!safeURL(url))throw Error('Choose an article from the audit results.');
  const query=new URLSearchParams({country,url}),response=await fetch(window.AUDIT_CONFIG.api+'/downloaded-article?'+query);
  if(!response.ok){let detail;try{detail=(await response.json()).detail;}catch{}throw Error(detail||'The downloaded article could not be opened.');}
  const data=await response.json();
  $('outlet').textContent=data.outlet;$('quality').textContent=data.status==='partial'?'Partial text':'Full text';
  $('collected').textContent=data.collected_at?'Collected '+new Date(data.collected_at).toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}):'';
  $('title').textContent=data.title||'Article from '+data.outlet;document.title=$('title').textContent+' · China news audit';
  const original=safeURL(data.url);$('original').hidden=!original;if(original)$('original').href=original;
  $('partial-note').hidden=data.status!=='partial';
  // Stored publisher content is always text; scripts and markup never execute.
  const paragraphs=data.text.split(/\n\s*\n/);$('text').replaceChildren(...paragraphs.map(text=>{const p=document.createElement('p');p.textContent=text;return p;}));
  $('article').hidden=false;$('status').hidden=true;
 }catch(e){$('status').textContent=e.message;$('retry').hidden=false;}
}
$('retry').onclick=load;load();
