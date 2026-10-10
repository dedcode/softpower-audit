"""Deterministic extraction and conservative quality gates. No AI services."""
import json,re
from urllib.parse import urlsplit,urljoin
from lxml import html
import trafilatura
from extractor_version import VERSION
CLASSES=('post-content','post__content','banner-text','story-content','tbl_art','article-body','entry-content','body-copy','article-content__content')
PROMO=('get full access for','subscribe now','unlimited access to all premium','ad-free browsing','mobile-optimised reading','weekly newsletters','search option is now available','for subscriptions on news from china daily','for more visit china daily','stand with the standard','the standard group plc')
def publisher_host(url):
 # A replay URL is evidence from its embedded original publisher, not from
 # arbitrary publisher-looking text in its query, fragment, or a foreign host.
 try:
  parsed=urlsplit(url)
  if parsed.scheme not in ('http','https'):return None
  if parsed.hostname=='web.archive.org':
   replay=re.fullmatch(r'/web/\d{1,14}(?:(?:id|if|im|js|cs|oe|mp)_)?/(https?://.+)',parsed.path)
   if not replay:return None
   parsed=urlsplit(replay.group(1))
  return parsed.hostname if parsed.scheme in ('http','https') else None
 except ValueError:return None
def walk(value):
 if isinstance(value,dict):
  yield value
  for v in value.values():yield from walk(v)
 elif isinstance(value,list):
  for v in value:yield from walk(v)
def normalized(text):return ' '.join(text.split())
def continuation_links(tree,url,paywall):
 # Only an explicit continuation in the single article body is evidence of
 # syndication. Navigation, related stories and subscription links are not.
 if paywall:return []
 articles=tree.xpath('//article')
 if len(articles)!=1:return []
 links=[]
 for node in articles[0].xpath('.//a[@href]'):
  label=normalized(node.text_content()).lower()
  if not re.fullmatch(r'(?:read more|continue reading|read (?:the )?full (?:article|story))\s*[.\u2026]*',label):continue
  target=urljoin(url,node.get('href'))
  try:p=urlsplit(target)
  except ValueError:continue
  if p.scheme not in ('http','https') or not p.hostname or p.username or p.password:continue
  choices=[target]
  if p.hostname=='web.archive.org':
   replay=re.fullmatch(r'/web/\d{1,14}(?:(?:id|if|im|js|cs|oe|mp)_)?/(https?://.+)',p.path)
   if replay:
    original=replay.group(1)
    choices.append('https://'+original[len('http://'):] if original.startswith('http://') else original)
  for choice in choices:
   if choice!=url and choice not in links:links.append(choice)
 return links[:2]

def continuation_matches(reference,reference_text,candidate,candidate_text):
 # Keep the ordinary full-body quality gate and independently establish that
 # this is the linked story, not a homepage, redirect or unrelated article.
 def tokens(value):return re.findall(r'\w+',value.lower())
 a=set(tokens(reference.split(' | ')[0].split(' - ')[0]));b=set(tokens(candidate.split(' | ')[0].split(' - ')[0]))
 if min(len(a),len(b))>=5 and 2*len(a&b)/(len(a)+len(b))>=.8:return 'headline'
 full=' '.join(tokens(candidate_text))
 for paragraph in reference_text.split('\n\n'):
  words=tokens(paragraph)
  if len(words)>=12 and ' '.join(words[:12]) in full:return 'opening_text'
 return None
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
 # BusinessDaily uses article elements for wrappers and related-story cards as
 # well as its body. Select the exact body class instead of weakening the
 # listing-page veto for every parser result with og:type=article.
 classes=CLASSES
 if publisher_host(url) in ('businessdailyafrica.com','www.businessdailyafrica.com'):classes+=('article-story',)
 standard=publisher_host(url) in ('standardmedia.co.ke','www.standardmedia.co.ke')
 if standard:classes+=('main-article',)
 for cls in classes:
  nodes=tree.xpath('//*[contains(concat(" ",normalize-space(@class)," ")," '+cls+' ")]')
  for node in nodes[:2]:
   # banner-text is a known single-article body on Kenya Star, not a generic marker.
   if cls=='banner-text' and 'kenyastar.com' not in url:continue
   candidates.append(('article selector: '+cls,clean(node),True))
 articles=tree.xpath('//article')
 if len(articles)==1:candidates.append(('single article element',clean(articles[0]),True))
 for node in tree.xpath('//*[@itemprop="articleBody"]')[:2]:candidates.append(('microdata articleBody',clean(node),True))
 # Older table-based sites can have a single substantial article cell.
 cells=[normalized(e.text_content()) for e in tree.xpath('//td[not(.//td)]')]
 if cells and max(map(len,cells))>1000:
  text=max(cells,key=len)
  if not any(x in text.lower() for x in PROMO):candidates.append(('table article cell',text,True))
 for precise in (True,False):
  text=trafilatura.extract(body,url=url,include_comments=False,include_tables=False,favor_precision=precise) or ''
  candidates.append(('precision parser' if precise else 'fallback parser',text,bool(schemas) or 'article' in tree.xpath('//meta[@property="og:type"]/@content')))
 standard_body=standard and any(m=='article selector: main-article' for m,_,_ in candidates)
 for method,text,anchored in candidates:
  text='\n\n'.join(line for line in text.splitlines() if line.strip() and not any(x in line.lower() for x in PROMO))
  truncated=bool(re.search(r'(?:\.\.\.|…|read more|continue reading)\s*$',text,re.I))
  empty_body_marker=any(m.startswith(('article selector','microdata')) for m,_,_ in candidates) and not any(len(t)>=100 for m,t,_ in candidates if m.startswith(('article selector','microdata')))
  if 'parser' in method and empty_body_marker:anchored=False
  # An explicit legacy Standard body is more reliable than a parser that adds
  # video cards to a short story. Three substantive paragraphs can constitute
  # its entire brief; do not lower the generic article threshold.
  if standard_body and ('parser' in method or method=='table article cell'):anchored=False
  short_brief=(standard and method=='article selector: main-article' and len(text)>=250
               and sum(len(p)>=40 for p in text.split('\n\n'))>=3)
  quality='candidate' if (len(text)>=400 or short_brief) and anchored and not paywall and not truncated else 'partial' if len(text)>=100 and (anchored or schemas or paywall) else 'missing'
  reason='Article body passes structural checks; not human-verified' if quality=='candidate' else 'Subscription preview' if paywall else 'Truncated or insufficient article body'
  result['candidates'].append({'method':method,'characters':len(text),'quality':quality})
  specificity=4 if method.startswith('structured') else 3 if method.startswith(('article selector','microdata')) else 2 if method=='single article element' else 1 if method=='table article cell' else 0
  score=({'candidate':3,'partial':2,'missing':0}[quality],specificity,len(text))
  if score>result.get('_score',(-1,0,0)):result.update(text=text if quality!='missing' else '',quality=quality,method=method,reason=reason,_score=score)
 result.pop('_score',None);result['paywall']=paywall
 result['continuations']=continuation_links(tree,url,paywall)
 # News listings can have many Article cards; no matching headline/body means no success.
 if not schemas and not paywall and len(tree.xpath('//article'))>8 and not any(c['quality']=='candidate' and c['method'].startswith(('article selector','microdata','structured')) for c in result['candidates']):
  result.update(quality='missing',text='',reason='Listing page, not a single article')
 return result
