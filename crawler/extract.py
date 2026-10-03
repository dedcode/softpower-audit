"""Deterministic extraction and conservative quality gates. No AI services."""
import json,re
from urllib.parse import urlsplit,urljoin
from lxml import html
import trafilatura
VERSION='toolbox-2'
CLASSES=('post-content','post__content','banner-text','story-content','tbl_art','article-body','entry-content')
PROMO=('get full access for','subscribe now','unlimited access to all premium','ad-free browsing','mobile-optimised reading','weekly newsletters','search option is now available','for subscriptions on news from china daily','for more visit china daily','stand with the standard','the standard group plc')
def walk(value):
 if isinstance(value,dict):
  yield value
  for v in value.values():yield from walk(v)
 elif isinstance(value,list):
  for v in value:yield from walk(v)
def normalized(text):return ' '.join(text.split())
def clean(node):
 node=html.fromstring(html.tostring(node))
 for e in node.xpath('.//script|.//style|.//nav|.//aside|.//*[contains(@class,"related") or contains(@class,"std-banner") or contains(@class,"cfm-yml") or contains(@class,"social")]'):
  if e.getparent() is not None:e.drop_tree()
 lines=[]
 for e in node.xpath('.//p|.//h2|.//h3|.//li'):
  if e.xpath('ancestor::p|ancestor::li'):continue
  text=normalized(e.text_content())
  if text and not any(x in text.lower() for x in PROMO) and text not in lines:lines.append(text)
 return '\n\n'.join(lines)
def extract(body,url):
 result={'text':'','quality':'missing','method':None,'reason':'No article body','candidates':[],'title':'','discovered':[]}
 if not body.strip():return result
 try:tree=html.fromstring(body)
 except Exception:return result
 title=normalized(tree.xpath('string(//h1)') or tree.xpath('string(//title)'));result['title']=title
 lowtitle=title.lower()
 if any(x in lowtitle for x in ('just a moment','access denied','attention required','captcha','page not found','404 not found')):
  result['reason']='Challenge or error page';return result
 schemas=[]
 for e in tree.xpath('//script[@type="application/ld+json"]'):
  try:schemas.extend(x for x in walk(json.loads(e.text_content())) if any(t in str(x.get('@type','')) for t in ('NewsArticle','Article','BlogPosting')))
  except (ValueError,TypeError):pass
 visible_tree=html.fromstring(html.tostring(tree))
 for e in visible_tree.xpath('//script|//style|//nav|//footer'):
  if e.getparent() is not None:e.drop_tree()
 visible=normalized(visible_tree.text_content()).lower()
 paywall=any(x.get('isAccessibleForFree') in (False,'false','False') for x in schemas) or any(s in visible for s in ('get full access for','renew in to keep enjoying','subscribe to read this article'))
 canonical=tree.xpath('//link[@rel="canonical"]/@href')
 for link in canonical+tree.xpath('//meta[@property="og:url"]/@content'):
  dest=urljoin(url,link)
  if dest!=url and urlsplit(dest).hostname==urlsplit(url).hostname and len(urlsplit(dest).path)>5:result['discovered'].append(dest)
 candidates=[]
 # Do not use hidden schema bodies on explicitly restricted pages.
 if not paywall:
  for s in schemas:
   if isinstance(s.get('articleBody'),str):candidates.append(('structured articleBody',normalized(html.fromstring('<div>'+s['articleBody']+'</div>').text_content()),True))
 for cls in CLASSES:
  nodes=tree.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," '+cls+' ")]')
  for node in nodes[:2]:
   # banner-text is a known single-article body on Kenya Star, not a generic marker.
   if cls=='banner-text' and 'kenyastar.com' not in url:continue
   candidates.append(('article selector: '+cls,clean(node),True))
 for node in tree.xpath('//*[@itemprop="articleBody"]')[:2]:candidates.append(('microdata articleBody',clean(node),True))
 # Older table-based sites can have a single substantial article cell.
 cells=[normalized(e.text_content()) for e in tree.xpath('//td[not(.//td)]')]
 if cells and max(map(len,cells))>1000:
  text=max(cells,key=len)
  if not any(x in text.lower() for x in PROMO):candidates.append(('table article cell',text,True))
 for precise in (True,False):
  text=trafilatura.extract(body,url=url,include_comments=False,include_tables=False,favor_precision=precise) or ''
  candidates.append(('precision parser' if precise else 'fallback parser',text,bool(schemas)))
 for method,text,anchored in candidates:
  text='\n\n'.join(line for line in text.splitlines() if line.strip() and not any(x in line.lower() for x in PROMO))
  truncated=bool(re.search(r'(?:\.\.\.|…|read more|continue reading)\s*$',text,re.I))
  quality='candidate' if len(text)>=400 and anchored and not paywall and not truncated else 'partial' if len(text)>=100 and (anchored or schemas) else 'missing'
  reason='Article body passes structural checks; not human-verified' if quality=='candidate' else 'Subscription preview' if paywall else 'Truncated or insufficient article body'
  result['candidates'].append({'method':method,'characters':len(text),'quality':quality})
  score=({'candidate':3,'partial':2,'missing':0}[quality],(3 if method.startswith('structured') else 2 if anchored and 'parser' not in method else 1),len(text))
  if score>result.get('_score',(-1,0,0)):result.update(text=text if quality!='missing' else '',quality=quality,method=method,reason=reason,_score=score)
 result.pop('_score',None);result['paywall']=paywall
 # News listings can have many Article cards; no matching headline/body means no success.
 if len(tree.xpath('//article'))>8 and not any(c['quality']=='candidate' and c['method'].startswith(('article selector','microdata','structured')) for c in result['candidates']):
  result.update(quality='missing',text='',reason='Listing page, not a single article')
 return result
