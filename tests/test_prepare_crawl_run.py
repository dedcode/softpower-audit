import argparse
import contextlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import prepare_crawl_run


class PrepareRunTests(unittest.TestCase):
    def test_full_run_has_no_runtime_deadline_but_pilot_retains_explicit_limit(self):
        for pilot, expected in ((False, None), (True, 1200)):
            with self.subTest(pilot=pilot):
                args = argparse.Namespace(country='KE', start='2015-01-01', end='2025-12-31',
                                          source_table='citygraph.softpower.china_articles',
                                          pilot=pilot, apply=False)
                output = io.StringIO()
                with patch.object(prepare_crawl_run, 'arguments', return_value=args), contextlib.redirect_stdout(output):
                    prepare_crawl_run.main()
                self.assertEqual(json.loads(output.getvalue())['max_runtime_seconds'], expected)


if __name__ == '__main__':
    unittest.main()
