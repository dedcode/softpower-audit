"""Build the Kenya-only page from the completed January pilot; no remote queries."""
import csv,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
pilot=json.loads((ROOT/'docs/pilot/pilot.json').read_text())
articles=[r for r in pilot['articles'] if r['outlet_group']=='Local']
assert all(r['outlet_country']=='KE' and 'CH' in r['countries'] for r in articles)
assert len(articles)==len({r['url'] for r in articles})==429
outlets=[r for r in pilot['outlets']['unique_urls'] if r['group']=='Local']
assert len(outlets)==17 and sum(r['broad'] for r in outlets)==len(articles)
output=ROOT/'docs/kenya';output.mkdir(exist_ok=True)
(output/'data.json').write_text(json.dumps(dict(start='2025-01-01',end='2025-01-31',articles=articles,outlets=outlets,labels=pilot['labels']),ensure_ascii=False,separators=(',',':')))
with (output/'articles.csv').open('w',newline='') as f:
 columns=['outlet','url','first_observed','china_and_kenya','only_pair','specific_kenyan_place','countries','kenyan_places','classification_basis']
 writer=csv.DictWriter(f,fieldnames=columns);writer.writeheader()
 for r in articles:writer.writerow(dict(outlet=r['outlet'],url=r['url'],first_observed=r['days'][0],china_and_kenya=r['both'],only_pair=r['only_pair'],specific_kenyan_place=r['specific_kenya'],countries='; '.join(r['countries']),kenyan_places='; '.join(r['kenyan_places']),classification_basis=r['classification_basis']))
print(f'Kenya page: {len(articles)} URLs, {len(outlets)} websites.')
