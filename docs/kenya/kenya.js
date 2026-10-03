'use strict';
const $=id=>document.getElementById(id),fmt=new Intl.NumberFormat('en'),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const params=new URLSearchParams(location.search);let data,reviews=new Map(),source=params.get('source')||'',page=0,visible=[],dateDays=[],dateStart,dateEnd;
$('mode').value=['broad','both','only_pair'].includes(params.get('mode'))?params.get('mode'):'broad';$('place').checked=params.get('place')==='1';$('reviewed').checked=params.get('reviewed')==='1';
function label(code){return data.labels[code]||code;}
function urlLabel(r){if(reviews.has(r.url))return reviews.get(r.url).title;try{const parts=new URL(r.url).pathname.split('/').filter(Boolean),last=parts.at(-1)||'';let value=decodeURIComponent(last).replace(/\.(s?html?|aspx?|php)$/i,'').replace(/[-_]+/g,' ');return value.length>12?value:'Open article on '+r.outlet;}catch{return 'Open article on '+r.outlet;}}
function syncURL(){const u=new URL(location.href);u.search='';if(source)u.searchParams.set('source',source);if($('mode').value!=='broad')u.searchParams.set('mode',$('mode').value);for(const k of ['place','reviewed'])if($(k).checked)u.searchParams.set(k,'1');if(dateStart!==data.start)u.searchParams.set('start',dateStart);if(dateEnd!==data.end)u.searchParams.set('end',dateEnd);history.replaceState(null,'',u);}
function filterPool(){const mode=$('mode').value;return data.articles.filter(r=>r[mode]&&(!$('place').checked||r.specific_kenya)&&(!$('reviewed').checked||reviews.has(r.url)));}
function render(){const candidates=filterPool();renderTimeline(candidates.filter(r=>!source||r.outlet===source));const pool=candidates.filter(r=>KenyaDates.matches(r,dateStart,dateEnd)),counts=new Map();for(const r of pool)counts.set(r.outlet,(counts.get(r.outlet)||0)+1);$('matched').textContent=`${fmt.format(pool.length)} URLs across ${counts.size} of ${data.outlets.length} websites`;
 const ranked=[...data.outlets].sort((a,b)=>(counts.get(b.outlet)||0)-(counts.get(a.outlet)||0)||a.outlet.localeCompare(b.outlet)),max=Math.max(1,...counts.values());
 $('source-list').innerHTML=`<button class="source-choice" data-source="" aria-pressed="${!source}"><span><span class="source-name">All Kenyan sources</span><b>${fmt.format(pool.length)}</b></span></button>`+ranked.map(r=>{const n=counts.get(r.outlet)||0;return `<button class="source-choice" data-source="${esc(r.outlet)}" aria-pressed="${source===r.outlet}"><span><span class="source-name">${esc(r.outlet)}</span><b>${fmt.format(n)}</b></span><span class="source-track" aria-hidden="true"><i style="width:${100*n/max}%"></i></span></button>`;}).join('');
 $('source-list').querySelectorAll('[data-source]').forEach(b=>b.onclick=()=>{source=b.dataset.source;page=0;render();});
 visible=pool.filter(r=>!source||r.outlet===source).sort((a,b)=>KenyaDates.first(b,dateStart,dateEnd).localeCompare(KenyaDates.first(a,dateStart,dateEnd))||a.url.localeCompare(b.url));
 $('articles-title').textContent=source||'All Kenyan sources';$('article-count').textContent=`${fmt.format(visible.length)} matching article ${visible.length===1?'URL':'URLs'}`;$('active-source').hidden=!source;$('export').disabled=!visible.length;
 const info=data.outlets.find(r=>r.outlet===source);$('origin-note').hidden=!info;
 if(info){const basis={estimate_domain_agree:'The geographic estimate and domain clue both point to Kenya.',estimate_only:'Assigned Kenya from the GDELT geographic estimate only.',domain_only:'Assigned Kenya from its country-domain clue only.'};$('origin-note').textContent=(basis[info.basis]||'Assigned Kenya by the existing lookup.')+' This does not verify ownership, headquarters or authorship.';}
 renderArticles();syncURL();
}
function renderArticles(){const pages=Math.max(1,Math.ceil(visible.length/20));page=Math.min(page,pages-1);
 $('article-list').innerHTML=visible.slice(page*20,(page+1)*20).map(r=>{const review=reviews.get(r.url);return `<article class="story"><div class="story-meta"><strong>${esc(r.outlet)}</strong><span>Observed ${esc(KenyaDates.first(r,dateStart,dateEnd))}</span></div><h3><a href="${esc(r.url)}" target="_blank" rel="noopener noreferrer">${esc(urlLabel(r))} ↗</a></h3><p class="story-url">${esc(r.url)}</p><div class="story-tags"><span>${r.both?'China + Kenya detected':'China detected · Kenya not detected'}</span>${r.only_pair?'<span>Only the pair</span>':''}${r.specific_kenya?'<span>Specific Kenyan place</span>':''}${review?'<span class="subtle">Has review note</span>':''}</div><details><summary>Detected places${review?' & article review':''}</summary><p><strong>Countries:</strong> ${r.countries.map(c=>esc(label(c))).join(', ')}<br><strong>Kenyan places:</strong> ${esc(r.kenyan_places.join('; ')||'None detected')}</p>${review?`<div class="review-note"><strong>${esc(review.verdict)}</strong><p>${esc(review.explanation)}</p><p>${esc(review.access_note)}. Reviewed ${esc(review.reviewed_at)}.</p></div>`:''}</details></article>`;}).join('')||'<div class="empty">No articles match this selection. Choose another website or reset the filters.</div>';
 $('page-label').textContent=`Page ${page+1} of ${pages} · up to 20 URLs per page`;$('previous').disabled=page===0;$('next').disabled=page>=pages-1;
}
const prettyDate=d=>new Date(d+'T00:00:00Z').toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric',timeZone:'UTC'});
function initDates(){
 dateDays=KenyaDates.days(data.start,data.end);
 const clamp=d=>d<data.start?data.start:d>data.end?data.end:d;
 dateStart=KenyaDates.valid(params.get('start'))?clamp(params.get('start')):data.start;
 dateEnd=KenyaDates.valid(params.get('end'))?clamp(params.get('end')):data.end;
 if(dateStart>dateEnd){dateStart=data.start;dateEnd=data.end;}
 for(const id of ['date-start','date-end']){$(id).min=data.start;$(id).max=data.end;}
 for(const id of ['range-start','range-end'])$(id).max=dateDays.length-1;
}
function renderTimeline(rows){
 const counts=KenyaDates.histogram(rows,dateDays),max=Math.max(1,...counts.map(r=>r.count));
 // Keep the full sample distribution visible while the range changes.
 $('date-histogram').innerHTML=counts.map(r=>`<div class="date-bin ${r.day>=dateStart&&r.day<=dateEnd?'selected':''}" style="--bar-height:${100*r.count/max}%" title="${esc(prettyDate(r.day))}: ${r.count} article URLs"><span></span></div>`).join('');
 $('date-histogram').setAttribute('aria-label',`Daily URL counts for ${source||'all Kenyan sources'}, ${prettyDate(data.start)} to ${prettyDate(data.end)}. Selected ${prettyDate(dateStart)} to ${prettyDate(dateEnd)}.`);
 const start=dateDays.indexOf(dateStart),end=dateDays.indexOf(dateEnd),den=Math.max(1,dateDays.length-1);
 $('range-start').value=start;$('range-end').value=end;
 $('range-start').setAttribute('aria-valuetext',prettyDate(dateStart));$('range-end').setAttribute('aria-valuetext',prettyDate(dateEnd));
 $('range-start').style.zIndex=start===dateDays.length-1?'5':'3';
 $('selected-track').style.left=`${100*start/den}%`;$('selected-track').style.width=`${100*(end-start)/den}%`;
 $('date-start').value=dateStart;$('date-end').value=dateEnd;$('date-error').hidden=true;
 $('selected-dates').textContent=`${prettyDate(dateStart)} – ${prettyDate(dateEnd)}`;
}
function setDates(start,end){dateStart=start;dateEnd=end;page=0;render();}
$('range-start').oninput=()=>setDates(dateDays[Math.min(Number($('range-start').value),dateDays.indexOf(dateEnd))],dateEnd);
$('range-end').oninput=()=>setDates(dateStart,dateDays[Math.max(Number($('range-end').value),dateDays.indexOf(dateStart))]);
$('clear-dates').onclick=()=>setDates(data.start,data.end);
$('date-form').onsubmit=e=>{e.preventDefault();const start=$('date-start').value,end=$('date-end').value;if(!KenyaDates.valid(start)||!KenyaDates.valid(end)||start<data.start||end>data.end||start>end){$('date-error').textContent='Choose an ordered date range within '+prettyDate(data.start)+' and '+prettyDate(data.end)+'.';$('date-error').hidden=false;return;}setDates(start,end);};
for(const id of ['mode','place','reviewed'])$(id).addEventListener('input',()=>{page=0;render();});
$('clear-source').onclick=()=>{source='';page=0;render();};$('reset').onclick=()=>{source='';page=0;$('mode').value='broad';$('place').checked=false;$('reviewed').checked=false;dateStart=data.start;dateEnd=data.end;render();};$('previous').onclick=()=>{page--;renderArticles();};$('next').onclick=()=>{page++;renderArticles();};
$('export').onclick=()=>{const columns=['outlet','url','first_observed','selected_start','selected_end','china_and_kenya','only_pair','specific_kenyan_place','countries','kenyan_places'],rows=visible.map(r=>({outlet:r.outlet,url:r.url,first_observed:KenyaDates.first(r,dateStart,dateEnd),selected_start:dateStart,selected_end:dateEnd,china_and_kenya:r.both,only_pair:r.only_pair,specific_kenyan_place:r.specific_kenya,countries:r.countries.map(label).join('; '),kenyan_places:r.kenyan_places.join('; ')}));const url=URL.createObjectURL(new Blob(['\uFEFF'+Audit.csv(rows,columns)],{type:'text/csv;charset=utf-8'})),a=document.createElement('a');a.href=url;a.download=`kenyan-sources-china-${dateStart}-to-${dateEnd}.csv`;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),30000);};
async function init(){try{const response=await fetch('./data.json');if(!response.ok)throw Error('The saved Kenya data could not be loaded. Please refresh.');data=await response.json();reviews=new Map(data.reviews.map(r=>[r.url,r]));if(!data.outlets.some(r=>r.outlet===source))source='';initDates();render();$('content').hidden=false;$('status').hidden=true;}catch(e){$('status').textContent=e.message;$('status').classList.add('error');}}
init();
