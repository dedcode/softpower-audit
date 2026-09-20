'use strict';
const $=id=>document.getElementById(id), groups=Audit.groups;
const colors={'Chinese':'#cf684c','Local':'#13887d','Third country':'#7585a5','Unknown or uncertain':'#b8a27a'};
const prettyGroup={'Chinese':'Chinese outlets','Local':'Local outlets','Third country':'Third-country outlets','Unknown or uncertain':'Uncertain origin'};
const fmt=new Intl.NumberFormat('en'), short=new Intl.NumberFormat('en',{notation:'compact',maximumFractionDigits:1});
const dateLabel=s=>new Date(s+'T00:00:00Z').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let ref,data,series=[],websitePages={Local:0,Chinese:0},timelinePage=0,page=0,sortKey='articles',sortAsc=false,requestId=0;
const labels=code=>code?(ref.labels[code]||`Unrecognized code: ${code}`):'Unresolved';
const api=window.AUDIT_CONFIG.api.replace(/\/$/,'');
const urlParams=new URLSearchParams(location.search);
for(const id of ['start','end','interval','min-outlets','min-articles','min-coverage'])if(urlParams.has(id))$(id).value=urlParams.get(id);
function setStatus(message,error=false){$('status').textContent=message;$('status').classList.toggle('error',error);$('status').hidden=!message;}
async function getJSON(url){const res=await fetch(url);let body;try{body=await res.json();}catch{throw new Error('The data service returned an unreadable response. Please retry shortly.');}if(!res.ok)throw new Error(typeof body.detail==='string'?body.detail:'The selection could not be loaded. Please check the filters.');return body;}
function shareURL(){const u=new URL(location.href);u.search='';for(const id of ['country','start','end','interval','min-outlets','min-articles','min-coverage'])u.searchParams.set(id,$(id).value);return u;}
async function load(){
  if(!$('selection').reportValidity())return;
  if($('start').value>$('end').value){setStatus('The start date must come before the end date.',true);return;}
  const id=++requestId;$('apply').disabled=true;$('apply').textContent='Loading coverage…';$('result').hidden=true;
  setStatus('Loading this selection from the daily dataset. A first request may take a minute…');
  const query=new URLSearchParams(Object.fromEntries(['country','start','end','interval'].map(k=>[k,$(k).value])));
  try{
    const isSnapshot=['ZA','CI','RP'].includes(query.get('country'))&&query.get('start')==='2015-01-01'&&query.get('end')==='2025-12-31'&&query.get('interval')==='month';
    const received=await (isSnapshot?getJSON('./data/'+query.get('country')+'.json').catch(()=>getJSON(api+'/audit?'+query)):getJSON(api+'/audit?'+query));
    if(id!==requestId)return;data=received;
    series=Audit.periods(data,ref.missing_source_dates);page=0;timelinePage=0;websitePages={Local:0,Chinese:0};
    $('pair-title').innerHTML='China <span>×</span> '+esc(labels(data.country));
    $('period-label').textContent=`${dateLabel(data.start)} – ${dateLabel(data.end)}`;
    $('grouping-label').textContent=({day:'Daily',week:'Weekly · Monday start',month:'Monthly',year:'Yearly'})[data.interval]+' time breakdown';
    const available=series.reduce((s,p)=>s+p.available_days,0),missing=series.reduce((s,p)=>s+p.expected_days-p.available_days,0);
    $('available-label').textContent=`${fmt.format(available)} source days available`;
    $('gap-banner').hidden=!missing;$('gap-banner').textContent=`${fmt.format(missing)} source ${missing===1?'day is':'days are'} missing in this selection. Periods with missing source days are incomplete and excluded from the research-criteria check.`;
    $('result').hidden=false;setStatus('');
    renderWebsites();renderTimeline();renderAssessment();renderTable();
    history.replaceState(null,'',shareURL());
    document.querySelectorAll('[data-country]').forEach(b=>b.classList.toggle('active',b.dataset.country===data.country));
  }catch(error){if(id===requestId)setStatus(error.message,true);}
  finally{if(id===requestId){$('apply').disabled=false;$('apply').innerHTML='Explore coverage <span aria-hidden="true">→</span>';}}
}
function renderWebsites(){
  const local=data.summary.Local,china=data.summary.Chinese;
  $('pair-balance').innerHTML=`<strong>${fmt.format(local.active_outlets)} local websites</strong> and <strong>${fmt.format(china.active_outlets)} Chinese websites</strong> in this selection. Origin assignments are provisional. Review country clues in the details below.`;
  const query=$('website-search').value.trim().toLowerCase();
  $('website-lists').innerHTML=['Local','Chinese'].map(g=>{
    const summary=data.summary[g],all=data.outlets.filter(r=>r.group===g&&r.outlet).sort((a,b)=>b.articles-a.articles||a.outlet.localeCompare(b.outlet)),rows=all.filter(r=>r.outlet.toLowerCase().includes(query));
    const pages=Math.max(1,Math.ceil(rows.length/10));websitePages[g]=Math.min(websitePages[g],pages-1);
    const offset=websitePages[g]*10,max=all[0]?.articles||1;
    return `<article class="website-panel" style="--group-color:${colors[g]}" aria-label="${prettyGroup[g]}">
      <div class="website-heading"><h3>${g==='Local'?esc(labels(data.country))+' · local websites':'China · Chinese websites'}</h3><div class="website-stats"><div><strong>${fmt.format(summary.active_outlets)}</strong><span>unique websites</span></div><div><strong>${fmt.format(summary.articles)}</strong><span>coverage volume</span></div></div></div>
      <div class="rank-heading"><span>Website · most coverage first</span><span>Coverage volume</span></div>
      <ol class="website-ranking" start="${offset+1}">`+rows.slice(offset,offset+10).map((r,i)=>`<li><div class="website-row"><span class="rank-number">${offset+i+1}</span><span class="website-domain" title="${esc(r.outlet)}">${esc(r.outlet)}</span><strong>${fmt.format(r.articles)}</strong></div><div class="website-bar"><span style="width:${100*r.articles/max}%"></span></div><div class="website-days">Coverage on ${fmt.format(r.active_days)} days</div></li>`).join('')+`</ol>${rows.length?'':'<p class="empty">'+(all.length?'No websites match your search.':'No identified websites in this group.')+'</p>'}
      <div class="website-pagination"><span>${rows.length?`${offset+1}–${Math.min(offset+10,rows.length)} of ${fmt.format(rows.length)}`:'0'} websites${query?' matching':''}</span><div><button class="secondary" data-website-group="${g}" data-step="-1" aria-label="Previous ${g.toLowerCase()} websites" ${websitePages[g]===0?'disabled':''}>←</button><button class="secondary" data-website-group="${g}" data-step="1" aria-label="Next ${g.toLowerCase()} websites" ${websitePages[g]===pages-1?'disabled':''}>→</button><button class="text-button" data-export-group="${g}">Export ${g.toLowerCase()} ↓</button></div></div>
      ${summary.missing_domain_articles?`<p class="missing-domain">${fmt.format(summary.missing_domain_articles)} additional observations have no website name. Included in volume, excluded from the website count.</p>`:''}
    </article>`;
  }).join('');
  $('website-lists').querySelectorAll('[data-step]').forEach(b=>b.onclick=()=>{websitePages[b.dataset.websiteGroup]+=Number(b.dataset.step);renderWebsites();});
  $('website-lists').querySelectorAll('[data-export-group]').forEach(b=>b.onclick=()=>exportOutlets(data.outlets.filter(r=>r.group===b.dataset.exportGroup),b.dataset.exportGroup.toLowerCase()));
}
function renderTimeline(){
  if(!data)return;
  const pages=Math.max(1,Math.ceil(series.length/24));timelinePage=Math.min(timelinePage,pages-1);
  $('timeline-body').innerHTML=series.slice(timelinePage*24,(timelinePage+1)*24).map(p=>`<tr><td>${esc(p.period)}</td>${['Local','Chinese'].map(g=>`<td class="numeric">${p.available_days?fmt.format(p.groups[g].active_outlets):'—'}</td><td class="numeric">${p.available_days?fmt.format(p.groups[g].articles):'—'}</td>`).join('')}<td class="${p.available_days<p.expected_days?'incomplete':''}">${p.available_days}/${p.expected_days} days${p.available_days<p.expected_days?' · incomplete':''}</td></tr>`).join('');
  $('timeline-page').textContent=`Page ${timelinePage+1} of ${pages} · ${fmt.format(series.length)} periods`;
  $('timeline-previous').disabled=timelinePage===0;$('timeline-next').disabled=timelinePage===pages-1;
}
function renderAssessment(){
  if(!data)return;
  const mo=Math.max(1,Number($('min-outlets').value)||1),ma=Math.max(1,Number($('min-articles').value)||1),required=Math.min(100,Math.max(1,Number($('min-coverage').value)||1));
  const a=Audit.assess(series,mo,ma,required);
  $('assessment-badge').textContent=a.percent===null?'Insufficient source coverage':a.meets?'Meets your criteria':'Needs closer review';$('assessment-badge').classList.toggle('good',a.meets);
  $('assessment-value').textContent=a.percent===null?'—':Math.round(a.percent)+'%';
  $('assessment-text').textContent=`${fmt.format(a.passing)} of ${fmt.format(a.complete)} complete periods have at least ${mo} outlets and ${fmt.format(ma)} observations in each of the Chinese and local groups.`;
  $('assessment-exclusion').textContent=`${a.excluded} incomplete source periods excluded. Your required share: ${required}%. Green tiles meet both groups’ minimums.`;
  $('coverage-strip').innerHTML=series.map(p=>{const gap=p.available_days<p.expected_days,pass=!gap&&a.passes(p);return `<div class="period-tile ${gap?'gap':pass?'pass':''}" title="${esc(p.period)}: ${gap?'source incomplete':pass?'meets both minimums':'below minimums'}" aria-label="${esc(p.period)}: ${gap?'source incomplete':pass?'meets both minimums':'below minimums'}"></div>`;}).join('');
  const complete=series.filter(p=>p.available_days===p.expected_days),localMissing=complete.filter(p=>p.groups.Local.articles===0).length,chinaMissing=complete.filter(p=>p.groups.Chinese.articles===0).length;
  $('assessment-findings').innerHTML=`<span><strong>${localMissing}</strong> complete periods without local coverage</span><span><strong>${chinaMissing}</strong> without Chinese coverage</span><span><strong>${fmt.format(data.summary['Unknown or uncertain'].articles)}</strong> observations with uncertain outlet origin</span>`;
  if(data.country===$('country').value&&data.start===$('start').value&&data.end===$('end').value&&data.interval===$('interval').value)history.replaceState(null,'',shareURL());
}
function filteredOutlets(){const q=$('search').value.toLowerCase(),group=$('table-group').value;return data.outlets.filter(r=>(!group||r.group===group)&&(!q||[r.outlet,labels(r.country),labels(r.estimate),labels(r.domain_country),r.basis,r.group].some(v=>String(v).toLowerCase().includes(q)))).sort((a,b)=>{const x=a[sortKey]??'',y=b[sortKey]??'',c=typeof x==='number'?x-y:String(x).localeCompare(String(y));return (sortAsc?c:-c)||String(a.outlet).localeCompare(String(b.outlet));});}
function renderTable(){
  if(!data)return;const list=filteredOutlets(),pages=Math.max(1,Math.ceil(list.length/50));page=Math.min(page,pages-1);
  $('table-total').textContent=`${fmt.format(list.length)} website entries`;
  const basis={estimate_domain_agree:'Estimate and domain agree',estimate_only:'Estimate only',domain_only:'Domain clue only',conflict:'Country clues disagree',unknown:'No country clues'};
  $('outlet-body').innerHTML=list.slice(page*50,(page+1)*50).map(r=>`<tr><td>${esc(r.outlet||'(unresolved domain)')}</td><td><span class="group-pill ${r.group.split(' ')[0]}">${esc(r.group==='Unknown or uncertain'?'Uncertain':r.group)}</span></td><td>${esc(labels(r.country))}<small>Estimate: ${esc(labels(r.estimate))} · domain: ${esc(labels(r.domain_country))}</small></td><td class="numeric">${fmt.format(r.articles)}</td><td class="numeric">${fmt.format(r.active_days)}</td><td class="basis ${r.basis==='conflict'?'conflict':''}">${esc(basis[r.basis]||r.basis)}</td></tr>`).join('')||'<tr><td colspan="6" class="empty">No outlets match these filters.</td></tr>';
  $('page-label').textContent=`Page ${page+1} of ${fmt.format(pages)} · 50 entries per page`;$('previous').disabled=page===0;$('next').disabled=page>=pages-1;
}
function download(name,rows,cols){const blob=new Blob(['\uFEFF'+Audit.csv(rows,cols)],{type:'text/csv;charset=utf-8'}),url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;a.style.display='none';document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),30000);}
$('selection').onsubmit=e=>{e.preventDefault();load();};
$('all-years').onclick=()=>{$('start').value='2015-01-01';$('end').value='2025-12-31';};
document.querySelectorAll('[data-country]').forEach(b=>b.onclick=()=>{$('country').value=b.dataset.country;load();});
$('website-search').oninput=()=>{websitePages={Local:0,Chinese:0};renderWebsites();};
$('timeline-previous').onclick=()=>{timelinePage--;renderTimeline();};$('timeline-next').onclick=()=>{timelinePage++;renderTimeline();};
for(const id of ['min-outlets','min-articles','min-coverage'])$(id).oninput=renderAssessment;
for(const id of ['search','table-group'])$(id).oninput=()=>{page=0;renderTable();};
document.querySelectorAll('[data-sort]').forEach(b=>b.onclick=()=>{sortAsc=sortKey===b.dataset.sort?!sortAsc:true;sortKey=b.dataset.sort;page=0;document.querySelectorAll('[data-sort]').forEach(x=>{x.textContent=x.textContent.replace(/[↕↑↓]/,'').trim()+' '+(x===b?(sortAsc?'↑':'↓'):'↕');x.parentElement.setAttribute('aria-sort',x===b?(sortAsc?'ascending':'descending'):'none');});renderTable();});
$('previous').onclick=()=>{page--;renderTable();};$('next').onclick=()=>{page++;renderTable();};
function exportOutlets(outlets,group='all'){const rows=outlets.map(r=>({target_country:labels(data.country),start:data.start,end:data.end,outlet:r.outlet,group:r.group,publisher_country:labels(r.country),estimated_country:labels(r.estimate),domain_country:labels(r.domain_country),classification_basis:r.basis,daily_url_observations:r.articles,active_days:r.active_days}));download(`china-${data.country}-${group}-websites-${data.start}-${data.end}.csv`,rows,['target_country','start','end','outlet','group','publisher_country','estimated_country','domain_country','classification_basis','daily_url_observations','active_days']);}
$('export-outlets').onclick=()=>exportOutlets(filteredOutlets());
$('export-timeline').onclick=()=>{const rows=series.flatMap(p=>groups.map(g=>({target_country:labels(data.country),period:p.period,interval:data.interval,group:g,daily_url_observations:p.available_days?p.groups[g].articles:null,active_outlets:p.available_days?p.groups[g].active_outlets:null,available_source_days:p.available_days,selected_calendar_days:p.expected_days})));download(`china-${data.country}-${data.interval}-timeline.csv`,rows,['target_country','period','interval','group','daily_url_observations','active_outlets','available_source_days','selected_calendar_days']);};
$('share').onclick=async()=>{try{await navigator.clipboard.writeText(shareURL().href);$('share').textContent='Link copied';}catch{$('share').textContent='Copy the address bar link';}setTimeout(()=>$('share').textContent='Copy view link',2500);};
async function init(){try{if(!api)throw new Error('The data service URL has not been configured yet.');ref=await getJSON('./data/catalog.json');const countries=Object.entries(ref.labels).filter(([c])=>c!=='CH'&&ref.target_codes.includes(c)).sort((a,b)=>a[1].localeCompare(b[1]));$('country').innerHTML=countries.map(([c,name])=>`<option value="${esc(c)}">${esc(name)} (${c})</option>`).join('');$('country').value=countries.some(([c])=>c===urlParams.get('country'))?urlParams.get('country'):'ZA';await load();}catch(error){setStatus(error.message,true);}}
init();
