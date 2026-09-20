'use strict';
const $=id=>document.getElementById(id),fmt=new Intl.NumberFormat('en'),modes=['broad','both','only_pair','only_pair_specific'];
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let payload,outletPage=0,articlePage=0;
function link(url,label){return /^https?:\/\//i.test(url)?`<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(label)}</a>`:esc(label);}
function pagination(prefix,total,page,size){const pages=Math.max(1,Math.ceil(total/size));$(prefix+'-page').textContent=`${fmt.format(total)} results · page ${page+1} of ${pages}`;$(prefix+'-prev').disabled=page===0;$(prefix+'-next').disabled=page>=pages-1;}
function renderSummary(){const unit=$('unit').value;
 $('unit-help').textContent=unit==='unique_urls'?'Each URL counts once during January. Location mentions are combined across its matching records. Different URLs carrying the same story are not merged.':'Each URL counts once per day. These counts match the daily observation measure used by the main dashboard.';
 $('summary-body').innerHTML=['Local','Chinese','Third country','Unknown or uncertain'].map(g=>`<tr><td>${esc(g==='Local'?'Local · Kenya':g)}</td>${modes.map(m=>{const x=payload.summary[unit][g][m];return `<td><strong>${fmt.format(x.count)}</strong><small>${fmt.format(x.websites)} ${x.websites===1?'website':'websites'}</small></td>`;}).join('')}</tr>`).join('');
 $('reconciliation').textContent=`All ${fmt.format(payload.summary.daily_count)} broad daily observations match the existing extraction, website by website. This month also contains ${fmt.format(payload.summary.url_count)} distinct URLs; the two counting units happen to agree.`;
 $('outlet-download').href='outlets_'+unit+'.csv';renderOutlets();
}
function renderOutlets(){const q=$('outlet-search').value.toLowerCase().trim(),g=$('origin').value;const rows=payload.outlets[$('unit').value].filter(r=>(!g||r.group===g)&&(!q||(r.outlet||'').toLowerCase().includes(q))).sort((a,b)=>b.broad-a.broad||(a.outlet||'').localeCompare(b.outlet||''));outletPage=Math.min(outletPage,Math.max(0,Math.ceil(rows.length/30)-1));
 $('outlet-total').textContent=fmt.format(rows.length)+' websites';
 $('outlet-body').innerHTML=rows.slice(outletPage*30,(outletPage+1)*30).map(r=>`<tr><td>${esc(r.outlet||'Unresolved domain')}</td><td>${esc(r.group)}</td>${modes.map(m=>`<td class="numeric">${fmt.format(r[m])}</td>`).join('')}</tr>`).join('')||'<tr><td colspan="6">No matching websites.</td></tr>';pagination('outlet',rows.length,outletPage,30);
}
function renderArticles(){const g=$('article-origin').value,m=$('article-mode').value,q=$('article-search').value.toLowerCase().trim();const rows=payload.articles.filter(r=>(!g||r.outlet_group===g)&&r[m]&&(!q||[r.url,...r.kenyan_places].join(' ').toLowerCase().includes(q)));articlePage=Math.min(articlePage,Math.max(0,Math.ceil(rows.length/20)-1));
 $('article-body').innerHTML=rows.slice(articlePage*20,(articlePage+1)*20).map(r=>`<tr><td>${link(r.url,r.outlet||r.url)}<small>${esc(r.url)}</small></td><td>${r.countries.map(c=>esc(payload.labels[c]||c)).join(', ')}</td><td>${esc(r.kenyan_places.join('; ')||'None detected')}</td><td>${esc(r.days[0])}</td></tr>`).join('')||'<tr><td colspan="4">No matching articles.</td></tr>';pagination('article',rows.length,articlePage,20);
}
function renderReviews(){const reviews=payload.reviews.records;
 $('findings').innerHTML='<ul>'+payload.reviews.findings.map(t=>'<li>'+esc(t)+'</li>').join('')+'</ul>';
 $('review-method').textContent=payload.reviews.method;
 const card=r=>`<article class="review-card"><div class="review-label">${esc(r.outlet_group)} · ${esc(r.stratum.replaceAll('_',' '))}</div><h3>${link(r.url,r.title||r.outlet)}</h3><p><strong>${esc(r.verdict)}</strong> — ${esc(r.explanation)}</p>${r.excerpt?`<blockquote>${esc(r.excerpt)}</blockquote>`:''}<small>${esc(r.access_note)} · Countries detected: ${r.countries.map(c=>esc(payload.labels[c]||c)).join(', ')}${r.kenyan_places.length?' · Kenyan places: '+esc(r.kenyan_places.join('; ')):''}</small></article>`;
 $('review-cards').innerHTML=reviews.filter(r=>r.featured).map(card).join('');
 $('other-reviews').innerHTML=reviews.filter(r=>!r.featured).map(card).join('');
}
async function init(){try{const r=await fetch('pilot.json');if(!r.ok)throw Error('Could not load the pilot data.');payload=await r.json();renderSummary();renderArticles();renderReviews();$('pilot-result').hidden=false;$('load-status').hidden=true;}catch(e){$('load-status').textContent=e.message;}}
$('unit').onchange=()=>{outletPage=0;renderSummary();};for(const id of ['origin','outlet-search'])$(id).oninput=()=>{outletPage=0;renderOutlets();};for(const id of ['article-origin','article-mode','article-search'])$(id).oninput=()=>{articlePage=0;renderArticles();};
$('outlet-prev').onclick=()=>{outletPage--;renderOutlets();};$('outlet-next').onclick=()=>{outletPage++;renderOutlets();};$('article-prev').onclick=()=>{articlePage--;renderArticles();};$('article-next').onclick=()=>{articlePage++;renderArticles();};init();
