"""Offline article-body recovery for the original pilot; no LLM calls."""
import hashlib,json,re
from pathlib import Path
from lxml import html
ROOT=Path(__file__).resolve().parents[1]
P=ROOT/'deployment-private/recovery-pilot'
SELECTORS={'capitalfm.co.ke':'post-content','tuko.co.ke':'post__content','kenyastar.com':'banner-text','the-star.co.ke':'story-content','standardmedia.co.ke':'tbl_art'}
def clean(node):
 for x in node.xpath('.//script|.//style|.//nav|.//aside|.//*[contains(@class,"related") or contains(@class,"std-banner") or contains(@class,"cfm-yml") or contains(@class,"social")]'):
  if x.getparent() is not None:x.drop_tree()
 paragraphs=[]
 for x in node.xpath('.//p|.//h2|.//h3|.//li'):
  if x.xpath('ancestor::p|ancestor::li'):continue
  text=' '.join(x.text_content().split())
  if any(v in text.lower() for v in ['get full access for','subscribe now','unlimited access to all premium','ad-free browsing','mobile-optimised reading','weekly newsletters']):continue
  if not text or any(v in text.lower() for v in ['search option is now available at tuko','for subscriptions on news from china daily','for more visit china daily']):continue
  if text not in paragraphs:paragraphs.append(text)
 return '\n\n'.join(paragraphs)
def recover(r,body):
 if not body.strip():return {'title':'','text':'','method':'none','assessment':'missing','reason':'Empty response body'}
 t=html.fromstring(body);title=' '.join(t.xpath('string(//h1)').split());result={'title':title,'text':'','method':'none','assessment':'missing','reason':r.get('error') or r['status']}
 if r['http_status']!=200:return result
 if r['outlet']=='citizentv.co.ke' or (r['outlet']=='businessdailyafrica.com' and '/bd/economy/' not in r['url']):
  return {**result,'reason':'Homepage/listing instead of requested article'}
 cls=SELECTORS.get(r['outlet']);nodes=t.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," '+cls+' ")]') if cls else []
 if nodes:
  result.update(text=clean(nodes[0]),method='publisher article-body selector')
 elif r['outlet']=='businessdailyafrica.com':
  import trafilatura
  result.update(text=trafilatura.extract(body,include_comments=False,favor_precision=True) or '',method='precision extraction')
 if r['outlet'] in ('standardmedia.co.ke','businessdailyafrica.com'):
  result.update(assessment='partial',reason='Preview only; paywall or subscription restriction; no access bypass attempted')
 elif len(result['text'])>=400:
  result.update(assessment='full_text_candidate',reason='Article body isolated; completeness requires comparison to publisher page')
 else:result.update(assessment='missing',reason='No substantial article body recovered')
 return result
if __name__=='__main__':
 rows={r['url']:r for r in json.loads((P/'results.json').read_text())};outputs=[]
 for item in json.loads((P/'inputs.json').read_text()):
  url=item['url'];aid=hashlib.sha256(url.encode()).hexdigest();r=rows.get(url);f=P/(aid+'.html')
  rec=recover(r,f.read_bytes()) if r and f.exists() else {'text':'','title':'','method':'none','assessment':'missing','reason':r.get('error') or r['status'] if r else 'Deferred after site pause'}
  text=rec.pop('text');out={**item,**rec,'article_id':aid,'previous_status':r['status'] if r else 'pending','characters':len(text),'text_file':aid+'.txt' if text else None}
  if text:(P/(aid+'.txt')).write_text(text)
  outputs.append(out)
 (P/'recovery.json').write_text(json.dumps(outputs,indent=2))
 from collections import Counter
 print(dict(Counter(r['assessment'] for r in outputs)))
 for r in outputs:
  if r['characters']:print(r['outlet'],r['characters'],r['assessment'],r['title'][:75])
