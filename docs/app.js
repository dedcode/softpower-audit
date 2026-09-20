'use strict';
const $=id=>document.getElementById(id), groups=Audit.groups;
const colors={'Chinese':'#cf684c','Local':'#13887d','Third country':'#7585a5','Unknown or uncertain':'#b8a27a'};
const prettyGroup={'Chinese':'Chinese outlets','Local':'Local outlets','Third country':'Third-country outlets','Unknown or uncertain':'Uncertain origin'};
const fmt=new Intl.NumberFormat('en'), short=new Intl.NumberFormat('en',{notation:'compact',maximumFractionDigits:1});
const dateLabel=s=>new Date(s+'T00:00:00Z').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let ref,data,series=[],metric='articles',page=0,sortKey='articles',sortAsc=false,requestId=0;
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
    series=Audit.periods(data,ref.missing_source_dates);page=0;
    $('pair-title').innerHTML='China <span>×</span> '+esc(labels(data.country));
    $('period-label').textContent=`${dateLabel(data.start)} – ${dateLabel(data.end)}`;
    $('grouping-label').textContent=({day:'Daily',week:'Weekly · Monday start',month:'Monthly',year:'Yearly'})[data.interval]+' view';
    const available=series.reduce((s,p)=>s+p.available_days,0),missing=series.reduce((s,p)=>s+p.expected_days-p.available_days,0);
    $('available-label').textContent=`${fmt.format(available)} source days available`;
    $('gap-banner').hidden=!missing;$('gap-banner').textContent=`${fmt.format(missing)} source ${missing===1?'day is':'days are'} missing in this selection. Shaded periods are incomplete and excluded from the research-criteria check.`;
    $('result').hidden=false;setStatus('');
    renderCards();renderChart();renderAssessment();renderConcentration();renderTable();
    history.replaceState(null,'',shareURL());
    document.querySelectorAll('[data-country]').forEach(b=>b.classList.toggle('active',b.dataset.country===data.country));
  }catch(error){if(id===requestId)setStatus(error.message,true);}
  finally{if(id===requestId){$('apply').disabled=false;$('apply').innerHTML='Explore coverage <span aria-hidden="true">→</span>';}}
}
function renderCards(){
  $('cards').innerHTML=groups.map(g=>{const s=data.summary[g];return `<article class="card" style="--group-color:${colors[g]}"><div class="card-title">${prettyGroup[g]}</div><div class="card-value" title="${fmt.format(s.articles)}">${short.format(s.articles)}</div><div class="card-caption">daily URL observations</div><div class="card-bottom"><strong>${fmt.format(s.active_outlets)}</strong> outlets <span aria-hidden="true">·</span> <strong>${fmt.format(s.active_days)}</strong> days seen</div></article>`;}).join('');
}
function renderChart(){
  if(!data)return;
  const host=$('chart'),W=Math.max(host.clientWidth,290),H=265,L=48,R=12,T=18,B=36,w=W-L-R,h=H-T-B;
  const visible=$('extra-groups').checked?groups:groups.slice(0,2),n=series.length;
  const max=Math.max(1,...series.flatMap(p=>visible.map(g=>p.groups[g][metric]))),mag=10**Math.floor(Math.log10(max)),top=Math.ceil(max/mag)*mag;
  const x=i=>L+(i+.5)*w/n,y=v=>T+h-(v/top)*h;
  let svg=`<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${metric==='articles'?'Daily URL observations':'Distinct active outlets'} by ${data.interval}"><title>Coverage by outlet origin; use the timeline export for exact values.</title>`;
  series.forEach((p,i)=>{if(p.available_days<p.expected_days)svg+=`<rect x="${L+i*w/n}" y="${T}" width="${Math.max(w/n,1)}" height="${h}" fill="#f5ecda"/>`;});
  for(let t=0;t<=4;t++){const v=top*t/4;svg+=`<line x1="${L}" x2="${W-R}" y1="${y(v)}" y2="${y(v)}" stroke="#e8ede9"/><text x="${L-10}" y="${y(v)+3}" text-anchor="end" font-size="10" fill="#7a8988">${short.format(v)}</text>`;}
  const ticks=[...new Set(Array.from({length:Math.min(n,W<400?3:6)},(_,i)=>Math.round(i*(n-1)/(Math.min(n,W<400?3:6)-1||1))))];
  for(const i of ticks){const d=new Date(series[i].period+'T00:00:00Z'),label=data.interval==='year'?String(d.getUTCFullYear()):d.toLocaleDateString('en-GB',{month:'short',year:'2-digit',...(data.interval==='day'?{day:'numeric'}:{}),timeZone:'UTC'});svg+=`<text x="${x(i)}" y="${H-9}" text-anchor="${i===0?'start':i===n-1?'end':'middle'}" font-size="10" fill="#7a8988">${label}</text>`;}
  for(const g of visible){let path='',drawing=false;series.forEach((p,i)=>{if(p.available_days===0){drawing=false;return;}path+=(drawing?'L':'M')+x(i).toFixed(2)+','+y(p.groups[g][metric]).toFixed(2)+' ';drawing=true;});svg+=`<path d="${path}" fill="none" stroke="${colors[g]}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>`;if(n===1&&series[0].available_days)svg+=`<circle cx="${x(0)}" cy="${y(series[0].groups[g][metric])}" r="4" fill="${colors[g]}"/>`;}
  svg+=`<line id="crosshair" x1="0" x2="0" y1="${T}" y2="${T+h}" stroke="#7d9390" stroke-dasharray="3 3" visibility="hidden"/><rect id="chart-hit" x="${L}" y="${T}" width="${w}" height="${h}" fill="transparent"/></svg>`;
  host.querySelector('svg')?.remove();host.insertAdjacentHTML('afterbegin',svg);
  const hit=$('chart-hit'),tip=$('tooltip');
  hit.onpointermove=e=>{const bounds=host.getBoundingClientRect(),pos=e.clientX-bounds.left,i=Math.max(0,Math.min(n-1,Math.floor((pos-L)/w*n))),p=series[i];$('crosshair').setAttribute('x1',x(i));$('crosshair').setAttribute('x2',x(i));$('crosshair').setAttribute('visibility','visible');tip.innerHTML=`<strong>${dateLabel(p.period)}</strong>`+visible.map(g=>`<div><span>${prettyGroup[g]}</span><b>${p.available_days?fmt.format(p.groups[g][metric]):'No source data'}</b></div>`).join('')+`<small>${p.available_days}/${p.expected_days} selected source days available</small>`;tip.style.display='block';tip.style.left=Math.max(0,Math.min(W-245,pos+12))+'px';};
  hit.onpointerleave=()=>{tip.style.display='none';$('crosshair').setAttribute('visibility','hidden');};
  document.querySelectorAll('.extra-legend').forEach(el=>el.hidden=!$('extra-groups').checked);
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
function renderConcentration(){
  $('concentration').innerHTML=groups.slice(0,2).map(g=>{const s=data.summary[g],top=data.outlets.filter(r=>r.group===g&&r.outlet).slice(0,5),share=s.articles?100*s.largest_outlet_articles/s.articles:0;
    return `<div><div class="concentration-title">${prettyGroup[g]}<span>Largest five outlets</span></div>`+(top.length?top.map(r=>`<div class="bar-row"><div class="bar-label"><span title="${esc(r.outlet)}">${esc(r.outlet)}</span><span>${(100*r.articles/s.articles).toFixed(1)}%</span></div><div class="bar-track"><div class="bar-fill" style="width:${100*r.articles/s.articles}%;background:${colors[g]}"></div></div>`).join(''):'<div class="empty">No identified outlets in this group.</div>')+`<p class="concentration-note">${top.length?`The largest outlet supplies <strong>${share.toFixed(1)}%</strong> of this group’s observations. ${share>=70?'Coverage is heavily concentrated in one outlet.':'Check whether the remaining outlets provide meaningful independent coverage.'}`:'There is no identified outlet coverage to assess.'}${s.missing_domain_articles?` ${fmt.format(s.missing_domain_articles)} observations lack a usable domain.`:''}</p></div>`;}).join('');
}
function filteredOutlets(){const q=$('search').value.toLowerCase(),group=$('table-group').value;return data.outlets.filter(r=>(!group||r.group===group)&&(!q||[r.outlet,labels(r.country),labels(r.estimate),labels(r.domain_country),r.basis,r.group].some(v=>String(v).toLowerCase().includes(q)))).sort((a,b)=>{const x=a[sortKey]??'',y=b[sortKey]??'',c=typeof x==='number'?x-y:String(x).localeCompare(String(y));return (sortAsc?c:-c)||String(a.outlet).localeCompare(String(b.outlet));});}
function renderTable(){
  if(!data)return;const list=filteredOutlets(),pages=Math.max(1,Math.ceil(list.length/50));page=Math.min(page,pages-1);
  $('table-total').textContent=`${fmt.format(list.length)} outlet entries`;
  const basis={estimate_domain_agree:'Estimate and domain agree',estimate_only:'Estimate only',domain_only:'Domain clue only',conflict:'Country clues disagree',unknown:'No country clues'};
  $('outlet-body').innerHTML=list.slice(page*50,(page+1)*50).map(r=>`<tr><td>${esc(r.outlet||'(unresolved domain)')}</td><td><span class="group-pill ${r.group.split(' ')[0]}">${esc(r.group==='Unknown or uncertain'?'Uncertain':r.group)}</span></td><td>${esc(labels(r.country))}<small>Estimate: ${esc(labels(r.estimate))} · domain: ${esc(labels(r.domain_country))}</small></td><td class="numeric">${fmt.format(r.articles)}</td><td class="numeric">${fmt.format(r.active_days)}</td><td class="basis ${r.basis==='conflict'?'conflict':''}">${esc(basis[r.basis]||r.basis)}</td></tr>`).join('')||'<tr><td colspan="6" class="empty">No outlets match these filters.</td></tr>';
  $('page-label').textContent=`Page ${page+1} of ${fmt.format(pages)} · 50 entries per page`;$('previous').disabled=page===0;$('next').disabled=page>=pages-1;
}
function download(name,rows,cols){const blob=new Blob(['\uFEFF'+Audit.csv(rows,cols)],{type:'text/csv;charset=utf-8'}),url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=name;a.style.display='none';document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),30000);}
$('selection').onsubmit=e=>{e.preventDefault();load();};
$('all-years').onclick=()=>{$('start').value='2015-01-01';$('end').value='2025-12-31';};
document.querySelectorAll('[data-country]').forEach(b=>b.onclick=()=>{$('country').value=b.dataset.country;load();});
document.querySelectorAll('[data-metric]').forEach(b=>b.onclick=()=>{metric=b.dataset.metric;document.querySelectorAll('[data-metric]').forEach(x=>{x.classList.toggle('active',x===b);x.setAttribute('aria-pressed',String(x===b));});renderChart();});
$('extra-groups').onchange=renderChart;
for(const id of ['min-outlets','min-articles','min-coverage'])$(id).oninput=renderAssessment;
for(const id of ['search','table-group'])$(id).oninput=()=>{page=0;renderTable();};
document.querySelectorAll('[data-sort]').forEach(b=>b.onclick=()=>{sortAsc=sortKey===b.dataset.sort?!sortAsc:true;sortKey=b.dataset.sort;page=0;document.querySelectorAll('[data-sort]').forEach(x=>{x.textContent=x.textContent.replace(/[↕↑↓]/,'').trim()+' '+(x===b?(sortAsc?'↑':'↓'):'↕');x.parentElement.setAttribute('aria-sort',x===b?(sortAsc?'ascending':'descending'):'none');});renderTable();});
$('previous').onclick=()=>{page--;renderTable();};$('next').onclick=()=>{page++;renderTable();};
$('export-outlets').onclick=()=>{const rows=filteredOutlets().map(r=>({target_country:labels(data.country),start:data.start,end:data.end,outlet:r.outlet,group:r.group,publisher_country:labels(r.country),estimated_country:labels(r.estimate),domain_country:labels(r.domain_country),classification_basis:r.basis,daily_url_observations:r.articles,active_days:r.active_days}));download(`china-${data.country}-outlets-${data.start}-${data.end}.csv`,rows,['target_country','start','end','outlet','group','publisher_country','estimated_country','domain_country','classification_basis','daily_url_observations','active_days']);};
$('export-timeline').onclick=()=>{const rows=series.flatMap(p=>groups.map(g=>({target_country:labels(data.country),period:p.period,interval:data.interval,group:g,daily_url_observations:p.available_days?p.groups[g].articles:null,active_outlets:p.available_days?p.groups[g].active_outlets:null,available_source_days:p.available_days,selected_calendar_days:p.expected_days})));download(`china-${data.country}-${data.interval}-timeline.csv`,rows,['target_country','period','interval','group','daily_url_observations','active_outlets','available_source_days','selected_calendar_days']);};
$('share').onclick=async()=>{try{await navigator.clipboard.writeText(shareURL().href);$('share').textContent='Link copied';}catch{$('share').textContent='Copy the address bar link';}setTimeout(()=>$('share').textContent='Copy view link',2500);};
let resizeTimer;window.addEventListener('resize',()=>{clearTimeout(resizeTimer);resizeTimer=setTimeout(renderChart,120);});
async function init(){try{if(!api)throw new Error('The data service URL has not been configured yet.');ref=await getJSON('./data/catalog.json');const countries=Object.entries(ref.labels).filter(([c])=>c!=='CH'&&ref.target_codes.includes(c)).sort((a,b)=>a[1].localeCompare(b[1]));$('country').innerHTML=countries.map(([c,name])=>`<option value="${esc(c)}">${esc(name)} (${c})</option>`).join('');$('country').value=countries.some(([c])=>c===urlParams.get('country'))?urlParams.get('country'):'ZA';await load();}catch(error){setStatus(error.message,true);}}
init();
