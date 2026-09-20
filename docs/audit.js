(function(root) {
  'use strict';
  const groups = ['Chinese','Local','Third country','Unknown or uncertain'];
  const iso = d => d.toISOString().slice(0,10);
  function bucket(day, interval) {
    const d = new Date(day+'T00:00:00Z');
    if (interval==='week') d.setUTCDate(d.getUTCDate()-((d.getUTCDay()+6)%7));
    if (interval==='month') d.setUTCDate(1);
    if (interval==='year') { d.setUTCMonth(0); d.setUTCDate(1); }
    return iso(d);
  }
  function periods(data, missingDates) {
    const missing=new Set(missingDates), map=new Map();
    for(let d=new Date(data.start+'T00:00:00Z');iso(d)<=data.end;d.setUTCDate(d.getUTCDate()+1)) {
      const day=iso(d), key=bucket(day,data.interval);
      if(!map.has(key)) map.set(key,{period:key,expected_days:0,available_days:0,groups:Object.fromEntries(groups.map(g=>[g,{articles:0,active_outlets:0,active_days:0}]))});
      const p=map.get(key);p.expected_days++;if(!missing.has(day))p.available_days++;
    }
    for(const r of data.timeline) {const p=map.get(r.period);if(p)p.groups[r.group]=r;}
    return [...map.values()];
  }
  function assess(series, minOutlets, minArticles, required) {
    const complete=series.filter(p=>p.available_days===p.expected_days);
    const passes=p=>['Chinese','Local'].every(g=>p.groups[g].active_outlets>=minOutlets&&p.groups[g].articles>=minArticles);
    const passing=complete.filter(passes).length, percent=complete.length?100*passing/complete.length:null;
    return {complete:complete.length,excluded:series.length-complete.length,passing,percent,meets:percent!==null&&percent>=required,passes};
  }
  function csv(rows,columns) {
    const escape=v=>{let s=v===null||v===undefined?'':String(v);if(typeof v==='string'&&/^[=+@\-\t\r]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};
    return [columns.map(escape).join(','),...rows.map(r=>columns.map(c=>escape(r[c])).join(','))].join('\r\n');
  }
  const api={groups,bucket,periods,assess,csv};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.Audit=api;
})(typeof window==='undefined'?globalThis:window);
