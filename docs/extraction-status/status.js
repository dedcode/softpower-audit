'use strict';
const $=id=>document.getElementById(id),esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),fmt=new Intl.NumberFormat('en');
const candidate=new URLSearchParams(location.search).get('country')||'KE',country=/^[A-Z]{2}$/.test(candidate)?candidate:'KE';let busy=false;
function render(d){
 $('title').textContent='Article collection · '+country;
 if(d.state==='not_started'){$('run').textContent='No collection has started.';return;}
 $('details').hidden=false;$('run').textContent=d.phase==='pilot'?`${fmt.format(d.total)}-URL pilot`:'Article collection';
 const c=d.counts||{},success=c.saved||0,partial=c.partial||0,failed=(c.failed||0)+(c.exhausted||0),finished=success+partial+failed,remaining=Math.max(0,(d.total||0)-finished);
 const running=d.state==='running';
 $('state').textContent=remaining?(running?'Running':d.state==='queued'?'Starting':'Stopped')+' · '+fmt.format(remaining)+' remaining':'Finished';
 $('updated').textContent='Updated '+new Date(d.updated_at).toLocaleString();$('progress').value=d.total?100*finished/d.total:0;
 $('completion').textContent=remaining?`${fmt.format(finished)} of ${fmt.format(d.total)} URLs have a final result`:`All ${fmt.format(d.total)} URLs have a final result`;
 $('cards').innerHTML=[['Full text*',success],['Partial text',partial],['Failed',failed]].map(([label,n])=>`<div class="card"><strong>${fmt.format(n)}</strong><span>${label}</span></div>`).join('');
 const expanded=new Set([...document.querySelectorAll('#websites details[open]')].map(x=>x.dataset.outlet));
 $('websites').innerHTML=(d.domains||[]).map(w=>{
 const full=w.saved||0,part=w.partial||0,fail=(w.failed||0)+(w.exhausted||0),done=full+part+fail,total=done+(w.pending||0)+(w.downloading||0);
 const active=(d.active_stages||[]).filter(a=>a.outlet===w.outlet);
 const status=done===total?'Finished':w.downloading?'Collecting':running?'Waiting':'Stopped';
 return `<details data-outlet="${esc(w.outlet)}" ${expanded.has(w.outlet)?'open':''}><summary><strong>${esc(w.outlet)}</strong><span>${status}</span><span>${fmt.format(done)} / ${fmt.format(total)}</span></summary><progress max="${total||1}" value="${done}"></progress><p>${fmt.format(full)} full text · ${fmt.format(part)} partial · ${fmt.format(fail)} failed</p><p class="muted">${active.length?active.map(a=>esc(a.stage)).join(' · '):done===total?'All URLs have a final result.':fmt.format(w.pending||0)+' URLs waiting.'}</p></details>`;
 }).join('');
 $('notice').textContent=d.error||(running&&Date.now()-Date.parse(d.updated_at)>120000?'Progress has not updated recently.':'');
}
async function refresh(){if(busy||document.hidden)return;busy=true;try{const r=await fetch(window.AUDIT_CONFIG.api+'/extraction-status?country='+country,{cache:'no-store'});if(!r.ok)throw Error('Progress is unavailable. Please retry shortly.');render(await r.json());}catch(e){$('notice').textContent=e.message;}finally{busy=false;}}
refresh();setInterval(refresh,15000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
