import sys
import unittest
from datetime import date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'backend'))
from fastapi import HTTPException
from main import build_query, validate_selection, shape_result


class AuditTests(unittest.TestCase):
    def test_selection_validation(self):
        for country, start, end, interval in [
            ('CH', date(2020,1,1), date(2020,2,1), 'month'),
            ('ZA', date(2025,1,1), date(2020,2,1), 'month'),
            ('ZA', date(2014,1,1), date(2020,2,1), 'day'),
            ('ZA', date(2020,1,1), date(2020,2,1), 'DROP TABLE'),
        ]:
            with self.assertRaises(HTTPException): validate_selection(country,start,end,interval)
        validate_selection('ZA',date(2015,1,1),date(2025,12,31),'week')

    def test_distinct_outlets_and_parameterization(self):
        q=build_query('week')
        self.assertIn('COUNT(DISTINCT outlet)',q)
        self.assertIn('WEEK(MONDAY)',q)
        self.assertIn('target_country = @country',q)
        self.assertIn('day < @end',q)

    def test_missing_domain_is_not_a_largest_outlet(self):
        rows=[dict(kind='outlet',bucket=None,outlet_group='Local',outlet=None,
                   outlet_country='ZA',estimated_country='ZA',domain_country=None,
                   classification_basis='estimate_only',articles=100,active_outlets=0,active_days=1),
              dict(kind='outlet',bucket=None,outlet_group='Local',outlet='example.zm',
                   outlet_country='ZA',estimated_country='ZA',domain_country='ZA',
                   classification_basis='estimate_domain_agree',articles=20,active_outlets=1,active_days=1),
              dict(kind='summary',outlet_group='Local',articles=120,active_outlets=1,active_days=1)]
        r=shape_result(rows,'ZA',date(2025,1,1),date(2025,1,1),'day')
        self.assertEqual(r['summary']['Local']['largest_outlet'],'example.zm')
        self.assertEqual(r['summary']['Local']['missing_domain_articles'],100)
        self.assertEqual(r['summary']['Chinese']['active_outlets'],0)


if __name__=='__main__':unittest.main()
