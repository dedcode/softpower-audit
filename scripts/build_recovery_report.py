import json,gzip,hashlib,html as escape
from pathlib import Path
from lxml import html
p=Path('deployment-private/recovery-pilot');fp=p/'followup';rows=json.loads((p/'recovery.json').read_text());by={r['url']:r for r in rows}
lookups=json.loads((p/'archive-lookups.json').read_text());archive=json.loads((fp/'archive-results.json').read_text())
for ar in archive:
 source=next(x for x in lookups if x.get('result',{}).get('archived_snapshots',{}).get('closest',{}).get('url','').replace('http://web.archive.org/','https://web.archive.org/')==ar['url'])
 r=by[source['url']];text=gzip.decompress((fp/ar['text_uri'].split('local-recovery/')[1]).read_bytes()).decode()
 if r['outlet']=='coastweek.com':
  tree=html.fromstring(gzip.decompress((fp/ar['raw_uri'].split('local-recovery/')[1]).read_bytes()))
  cells=[' '.join(e.text_content().split()) for e in tree.xpath('//td')];text=min((x for x in cells if x.startswith('NAIROBI (Xinhua)') and len(x)>1000),key=len)
 partial=r['outlet']=='kenyastar.com'
 r.update(method='archived publisher page',assessment='partial' if partial else 'full_text_candidate',reason='Archived preview ends with ellipsis' if partial else 'Article opening and ending reviewed in archived HTML; historical snapshot may differ from observation date',recovery_url=ar['url'],snapshot=source['result']['archived_snapshots']['closest']['timestamp'],characters=len(text),text_file=r['article_id']+'.txt')
 (p/r['text_file']).write_text(text)
for ar in json.loads((fp/'results.json').read_text()):
 r=next(r for r in rows if (r['outlet']=='businessdailyafrica.com' and 'JKIA' in r['url']) or False) if 'businessdaily' in ar['url'] else next(r for r in rows if 'war-torn-myanmar' in r['url'])
 text=gzip.decompress((fp/ar['text_uri'].split('local-recovery/')[1]).read_bytes()).decode()
 if 'businessdaily' in ar['url']:text=text[text.index('The government has opened'):];assessment='partial';reason='Current publisher URL found; subscription preview only'
 else:assessment='alternative_full_text_candidate';reason='Accessible republication credits The Conversation; opening matches preview, but not a verified identical Standard edition'
 r.update(method='current publisher URL' if assessment=='partial' else 'credited republication',assessment=assessment,reason=reason,recovery_url=ar['url'],characters=len(text),text_file=r['article_id']+'.txt');(p/r['text_file']).write_text(text)
(p/'recovery.json').write_text(json.dumps(rows,indent=2))
from collections import Counter
counts=dict(Counter(r['assessment'] for r in rows));print(counts)
body='<h1>Full-text recovery pilot</h1><p>Same 24 URLs. No LLM API calls. '+escape.escape(str(counts))+'</p><p>Full-text candidates have coherent article bodies; completeness against the historical original is not guaranteed. An alternative republication is kept separate. Eleven remain unresolved. Archive availability was checked for six unresolved URLs, not all URLs.</p>'
body+='<table><tr><th>Website / original URL</th><th>Result</th><th>Method</th><th>Characters</th><th>Explanation</th></tr>'
for r in rows:
 body+='<tr><td><a href="'+escape.escape(r['url'],quote=True)+'">'+escape.escape(r['outlet'])+'</a></td><td>'+escape.escape(r['assessment'])+'</td><td>'+escape.escape(r['method'])+'</td><td>'+str(r['characters'])+'</td><td>'+escape.escape(r['reason'])+ (' <a href="'+escape.escape(r['recovery_url'],quote=True)+'">Recovery source</a>' if r.get('recovery_url') else '')+'</td></tr>'
body+='</table><p>The original crawl counters remain the historical first-pass results. This is a separate recovery assessment. Full texts and response bodies remain private.</p>'
page='<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pilot text recovery</title><style>body{font:16px system-ui;max-width:1200px;margin:40px auto;padding:16px;color:#203b3a}table{border-collapse:collapse;width:100%}td,th{padding:12px;border-bottom:1px solid #ddd;text-align:left}a{color:#126a62}</style>'+body
Path('docs/extraction-status/recovery-pilot.html').write_text(page)
(p/'report.html').write_text(page)
