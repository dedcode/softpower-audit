import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from recover_pilot_text import recover
class RecoveryTests(unittest.TestCase):
 def test_article_only(self):
  row={'outlet':'kenyastar.com','url':'https://example.org/a','http_status':200,'status':'saved'}
  body=('<div class="banner-text"><p>'+('Article sentence. '*40)+'</p></div><aside><p>Other story</p></aside>').encode()
  result=recover(row,body);self.assertNotIn('Other story',result['text']);self.assertEqual(result['assessment'],'full_text_candidate')
 def test_subscription_is_partial(self):
  row={'outlet':'standardmedia.co.ke','url':'https://example.org/a','http_status':200,'status':'saved'}
  body=b'<div class="tbl_art"><p>Actual preview.</p><p>Get Full Access for Ksh299/Week.</p></div>'
  result=recover(row,body);self.assertEqual(result['assessment'],'partial');self.assertEqual(result['text'],'Actual preview.')
 def test_empty(self):
  self.assertEqual(recover({'status':'unavailable'},b'')['assessment'],'missing')
if __name__=='__main__':unittest.main()
