'use strict';
const $=id=>document.getElementById(id),esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),fmt=new Intl.NumberFormat('en');
const candidate=new URLSearchParams(location.search).get('country')||'KE',country=/^[A-Z]{2}$/.test(candidate)?candidate:'KE';let busy=false;
function render(d){
 $('title').textContent='Article collection · '+(country==='KE'?'Kenya':country);
 if(d.state==='not_started'){$('run').textContent='No collection has started.';return;}
 $('details').hidden=false;$('run').textContent=d.phase==='pilot'?`${fmt.format(d.total)}-URL pilot`:`${fmt.format(d.total)} URLs · ${fmt.format((d.domains||[]).length)} websites`;
 const c=d.counts||{},success=c.saved||0,partial=c.partial||0,failed=(c.failed||0)+(c.exhausted||0),finished=success+partial+failed,remaining=Math.max(0,(d.total||0)-finished);
 const running=d.state==='running',recovering=d.state==='recovering_memory';
 const stale=running&&Date.now()-Date.parse(d.updated_at)>120000;
 $('state').textContent=remaining?(recovering?'Recovering automatically':stale?'Worker not reporting':running?'Collection running':['queued','starting'].includes(d.state)?'Collection starting':['failed','recovery_failed'].includes(d.state)?'Worker needs attention':'Collection paused'):(failed||partial?'Collection finished with incomplete results':'Collection finished');
 $('banner').className='status-banner '+(remaining?(stale?'warning':running?'':d.state==='queued'?'waiting':'warning'):(failed||partial?'warning':'complete'));
 $('activity').textContent=running&&!stale?`${fmt.format(d.downloading||0)} active` : '';
 $('percentage').textContent=(d.total?100*finished/d.total:0).toFixed(1)+'%';
 $('website-count').textContent=fmt.format((d.domains||[]).length);
 $('updated').textContent='Updated '+new Date(d.updated_at).toLocaleString();$('progress').value=d.total?100*finished/d.total:0;
 $('completion').textContent=remaining?`${fmt.format(finished)} of ${fmt.format(d.total)} URLs have a final result`:`All ${fmt.format(d.total)} URLs have a final result`;
 $('cards').innerHTML=[['Full text',success],['Partial text',partial],['Failed',failed]].map(([label,n])=>`<div class="card"><strong>${fmt.format(n)}</strong><span>${label}</span></div>`).join('');
 const expanded=new Set([...document.querySelectorAll('#websites details[open]')].map(x=>x.dataset.outlet));
 $('websites').innerHTML=(d.domains||[]).map(w=>{
 const full=w.saved||0,part=w.partial||0,fail=(w.failed||0)+(w.exhausted||0),done=full+part+fail,total=done+(w.pending||0)+(w.downloading||0);
 const active=(d.active_stages||[]).filter(a=>a.outlet===w.outlet);
 const complete=done===total,type=complete?(fail||part?'warning':'complete'):stale?'warning':w.downloading?'collecting':running?'waiting':'warning';
 const status=complete?(fail||part?'Finished · incomplete':'Finished'):stale?'No recent update':w.downloading?'Collecting':recovering?'Recovering':running?'Waiting':'Paused';
 return `<details data-outlet="${esc(w.outlet)}" ${expanded.has(w.outlet)?'open':''}><summary><strong class="website-name">${esc(w.outlet)}</strong><span class="website-status ${type}"><i class="dot" aria-hidden="true"></i>${status}</span><span class="website-progress"><span>${fmt.format(done)} / ${fmt.format(total)} URLs</span><progress aria-label="${esc(w.outlet)} collection progress" max="${total||1}" value="${done}"></progress></span></summary><div class="website-detail"><p><strong>${fmt.format(full)}</strong> full text · <strong>${fmt.format(part)}</strong> partial · <strong>${fmt.format(fail)}</strong> failed</p><p>${active.length?active.map(a=>esc(a.stage)).join(' · '):complete?'All URLs have a final result.':fmt.format(w.pending||0)+' URLs waiting.'}</p></div></details>`;
 }).join('');
 $('notice').textContent=d.error||(stale?'Progress has not updated recently. The worker may need attention.':remaining&&!running&&!recovering&&!['queued','starting'].includes(d.state)?'Collection has stopped before all URLs were completed. Saved progress is retained; the worker needs to be resumed.':'');
}
async function refresh(){if(busy||document.hidden)return;busy=true;try{const r=await fetch(window.AUDIT_CONFIG.api+'/extraction-status?country='+country,{cache:'no-store'});if(!r.ok)throw Error('Progress is unavailable. Please retry shortly.');render(await r.json());}catch(e){$('notice').textContent=e.message;}finally{busy=false;}}
refresh();setInterval(refresh,15000);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
