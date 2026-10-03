import sys,unittest
from datetime import date
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backend'))
from fastapi import HTTPException
from articles import selection,overview_sql,stories_sql,base_sql
class ArticleTests(unittest.TestCase):
 def test_invalid_selection(self):
  for country,start,end,mode,origin in [('CH',date(2025,1,1),date(2025,2,1),'broad','gdelt'),('KE',date(2025,2,1),date(2025,1,1),'broad','gdelt'),('KE',date(2025,1,1),date(2025,2,1),'injection','gdelt')]:
   with self.assertRaises(HTTPException):selection(country,start,end,mode,origin)
 def test_distinct_urls_across_days(self):
  self.assertIn('COUNT(DISTINCT url)',overview_sql('broad','gdelt'))
  self.assertIn('GROUP BY outlet,url',stories_sql('only_pair','both'))
 def test_pair_and_specific_same_country(self):
  q=base_sql('only_pair','both',True)
  self.assertIn('ARRAY_LENGTH(countries)=2',q)
  self.assertIn('@country IN UNNEST(specific_location_countries)',q)
  self.assertIn('estimated_country=@country AND domain_country=@country',q)
 def test_page_never_fetches_raw_payload(self):
  self.assertNotIn('raw_locations',stories_sql('broad','gdelt'))
  self.assertIn('OFFSET @offset',stories_sql('broad','gdelt'))
if __name__=='__main__':unittest.main()
