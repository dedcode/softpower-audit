(function(root){
 'use strict';
 const DAY=86400000;
 function valid(value){if(!/^\d{4}-\d{2}-\d{2}$/.test(value||''))return false;const d=new Date(value+'T00:00:00Z');return Number.isFinite(+d)&&d.toISOString().slice(0,10)===value;}
 function days(start,end){const result=[];for(let t=Date.parse(start+'T00:00:00Z');t<=Date.parse(end+'T00:00:00Z');t+=DAY)result.push(new Date(t).toISOString().slice(0,10));return result;}
 function matches(row,start,end){return row.days.some(d=>d>=start&&d<=end);}
 function first(row,start,end){return row.days.filter(d=>d>=start&&d<=end).sort()[0];}
 function histogram(rows,dates){const counts=new Map(dates.map(d=>[d,0]));for(const r of rows)for(const d of new Set(r.days))if(counts.has(d))counts.set(d,counts.get(d)+1);return dates.map(day=>({day,count:counts.get(day)}));}
 const api={valid,days,matches,first,histogram};if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.KenyaDates=api;
})(typeof window==='undefined'?globalThis:window);
