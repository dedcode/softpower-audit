"""Synthetic controls for the publisher-scoped BusinessDaily body selector."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'crawler'))
from extract import extract, publisher_host, VERSION
from extractor_version import VERSION as LIGHTWEIGHT_VERSION

URL = 'https://www.businessdailyafrica.com/bd/economy/example-article-12345'
TEXT = ('The project report explains the construction timetable, financing, and local employment. '
        'Officials discussed the findings at a public meeting and published supporting figures. ' * 6)
CARDS = '<aside class="related">' + ''.join(
    '<article><h2>Unrelated headline ' + str(index) + '</h2><p>A short unrelated card.</p></article>'
    for index in range(41)) + '</aside>'


def page(text=TEXT, *, body_class='article-story page-box', extra=''):
    return ('<html><head><meta charset="utf-8"><meta property="og:type" content="article">'
            '<link rel="canonical" href="' + URL + '"></head><body><h1>Site masthead</h1>'
            '<article class="wrapper"><article class="' + body_class + '">'
            '<h2 class="article-title">Example reporting headline</h2><div><p>' + text + '</p></div>'
            '</article></article>' + extra + CARDS + '</body></html>').encode()


class BusinessDailyExtractionTests(unittest.TestCase):
    def test_main_article_survives_wrappers_and_related_cards(self):
        result = extract(page(), URL)
        self.assertEqual(result['quality'], 'candidate')
        self.assertEqual(result['method'], 'article selector: article-story')
        self.assertIn(TEXT.strip(), result['text'])
        self.assertNotIn('Unrelated headline', result['text'])
        self.assertFalse(result['paywall'])

    def test_exact_publisher_and_recognized_wayback_replays_are_supported(self):
        for url in (URL, URL.replace('www.', ''),
                    'https://web.archive.org/web/20210810060012/' + URL,
                    'https://web.archive.org/web/20210810060012id_/' + URL,
                    'https://web.archive.org/web/2021/' + URL.replace('https:', 'http:')):
            with self.subTest(url=url):
                self.assertEqual(extract(page(), url)['quality'], 'candidate')

    def test_foreign_host_lookalikes_and_nonreplay_archive_paths_stay_listings(self):
        for url in ('https://example.org/story', URL.replace('.com/', '.com.example.org/'),
                    URL.replace('www.', 'cdn.'),
                    'https://example.org/web/20210810060012/' + URL,
                    'https://web.archive.org/other/' + URL,
                    'https://web.archive.org/web/latest/' + URL,
                    'https://web.archive.org/web/20210810060012/https://example.org/?publisher=' + URL,
                    'ftp://web.archive.org/web/20210810060012/' + URL):
            with self.subTest(url=url), patch('extract.trafilatura.extract', return_value=TEXT):
                result = extract(page(), url)
                self.assertEqual(result['quality'], 'missing')
                self.assertEqual(result['reason'], 'Listing page, not a single article')

    def test_actual_listing_with_article_metadata_remains_missing(self):
        body = ('<meta property="og:type" content="article"><h1>Latest news</h1>' + CARDS).encode()
        with patch('extract.trafilatura.extract', return_value=TEXT):
            result = extract(body, 'https://www.businessdailyafrica.com/')
        self.assertEqual(result['quality'], 'missing')
        self.assertEqual(result['text'], '')

    def test_similar_class_token_is_not_the_main_body_selector(self):
        with patch('extract.trafilatura.extract', return_value=TEXT):
            result = extract(page(body_class='article-story-card'), URL)
        self.assertEqual(result['quality'], 'missing')

    def test_explicit_paywall_preview_remains_partial(self):
        body = page(extra='<p>Subscribe to read this article</p>')
        result = extract(body, URL)
        self.assertEqual(result['quality'], 'partial')
        self.assertTrue(result['paywall'])

    def test_schema_restriction_still_prevents_full_text_success(self):
        schema = {'@type': 'NewsArticle', 'isAccessibleForFree': False, 'articleBody': TEXT * 2}
        body = page(extra='<script type="application/ld+json">' + json.dumps(schema) + '</script>')
        result = extract(body, URL)
        self.assertEqual(result['quality'], 'partial')
        self.assertTrue(result['paywall'])
        self.assertFalse(any(candidate['method'] == 'structured articleBody' for candidate in result['candidates']))

    def test_truncated_body_and_short_preview_never_become_candidates(self):
        for text in (TEXT + '…', 'An introduction describes the proposal and its financing. ' * 3):
            with self.subTest(text=text[-30:]), patch('extract.trafilatura.extract', return_value=text):
                self.assertNotEqual(extract(page(text), URL)['quality'], 'candidate')

    def test_empty_body_cannot_be_replaced_by_parser_navigation(self):
        with patch('extract.trafilatura.extract', return_value=TEXT):
            result = extract(page(''), URL)
        self.assertNotEqual(result['quality'], 'candidate')

    def test_lightweight_version_and_invalid_url_handling(self):
        self.assertEqual(VERSION, LIGHTWEIGHT_VERSION)
        self.assertEqual(VERSION, 'toolbox-4-linked-original')
        self.assertIsNone(publisher_host('https://[invalid'))


if __name__ == '__main__':
    unittest.main()
