'use strict';
const $=id=>document.getElementById(id),esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),fmt=new Intl.NumberFormat('en');
const candidate=new URLSearchParams(location.search).get('country')||'KE',country=/^[A-Z]{2}$/.test(candidate)?candidate:'KE';let busy=false;
const labels={saved:'Full text*',partial:'Partial text',exhausted:'Failed',failed:'Failed',deferred:'Pending',retrying:'Retrying',pending:'Pending'};
function render(d){
 $('title').textContent='Article collection · '+country;
 if(d.state==='not_started'){$('run').textContent='No collection has started.';return;}
 $('details').hidden=false;$('run').textContent=d.phase==='pilot'?'24-URL pilot':'Article collection';
 const c=d.counts||{},success=c.saved||0,partial=c.partial||0,failed=(c.failed||0)+(c.exhausted||0),finished=success+partial+failed,remaining=Math.max(0,(d.total||0)-finished);
 const running=d.state==='running',active=d.active_stages||[];
 $('state').textContent=remaining?(running?'Running':d.state==='queued'?'Starting':'Stopped')+' · '+fmt.format(remaining)+' remaining':'Finished';
 $('updated').textContent='Updated '+new Date(d.updated_at).toLocaleString();$('progress').value=d.total?100*finished/d.total:0;
 $('completion').textContent=remaining?`${fmt.format(finished)} of ${fmt.format(d.total)} URLs have a final result`:`All ${fmt.format(d.total)} URLs have a final result`;
 $('cards').innerHTML=[['Full text*',success],['Partial text',partial],['Failed',failed]].map(([label,n])=>`<div class="card"><strong>${fmt.format(n)}</strong><span>${label}</span></div>`).join('');
 const rows=(d.url_results||[]).map(r=>({...r}));
 for(const a of active){if(!rows.some(r=>r.outlet===a.outlet&&r.status==='retrying'))rows.push({outlet:a.outlet,url:a.url||'',status:a.stage==='Retrying automatically'?'retrying':'pending',stages:[{stage:a.stage,status:'running'}]});}
 $('articles').innerHTML=rows.map(r=>`<tr><td>${esc(r.outlet)}</td><td>${r.url?`<a href="${esc(r.url)}" target="_blank" rel="noopener">${esc(decodeLabel(r.url))}</a>`:'Retrieving article…'}</td><td>${esc(labels[r.status]||r.status)}<details><summary>Details</summary><ol>${(r.stages||[]).map(s=>`<li>${esc(s.stage)} — ${esc(s.status)}${s.reason?' · '+esc(s.reason):''}</li>`).join('')}</ol></details></td></tr>`).join('');
 $('notice').textContent=d.error||(running&&Date.now()-Date.parse(d.updated_at)>120000?'Progress has not updated recently.':'');
}
function decodeLabel(url){try{const p=new URL(url).pathname.split('/').filter(Boolean).pop()||url;return decodeURIComponent(p).replace(/[-_]/g,' ').slice(0,160);}catch{return url;}}
async function refresh(){if(busy||document.hidden)return;busy=true;try{const r=await fetch(window.AUDIT_CONFIG.api+'/extraction-status?country='+country,{cache:'no-store'});if(!r.ok)throw Error('Progress is unavailable. Please retry shortly.');render(await r.json());}catch(e){$('notice').textContent=e.message;}finally{busy=false;}}
refresh();setInterval(refresh,15000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
