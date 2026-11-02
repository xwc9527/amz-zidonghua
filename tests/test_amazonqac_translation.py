"""Fidelity guards and persistent cache checks; real model/browser acceptance is separate."""
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
import psycopg2
from contextlib import closing

from amazonqac_search import app
from amazonqac_translation import ENGINE, validate_translation, translate_terms
from pg_config import get_pg_dsn


class TranslationGuardTests(unittest.TestCase):
    def test_numbers_models_and_brands_cannot_silently_change(self):
        for original, chinese, names in (
            ('24 inch monitor', '27英寸显示器', []),
            ('iphone 15 case', '苹果15手机壳', ['iphone']),
            ('ps5games', 'ps5游戏', ['ps5']),
            ('kitchen organizer', 'kitchen organizer', []),
        ):
            with self.assertRaises(ValueError):
                validate_translation(original, {'chinese': chinese, 'preserved_terms': names})
        self.assertEqual(validate_translation('ikea', {'chinese':'ikea','preserved_terms':['ikea']})['status'], 'name_only')

    def test_invalid_requests_do_not_call_model(self):
        with patch('amazonqac_translation.translate_terms') as translate:
            for terms in ([], [' '], ['a'] * 11, ['x\x00y']):
                self.assertEqual(TestClient(app).post('/api/v2/amazonqac/translate', json={'terms':terms}).status_code, 422)
            translate.assert_not_called()


@unittest.skipUnless(os.getenv('PG_TEST_DSN'), 'Requires isolated PostgreSQL test database')
class TranslationCacheTests(unittest.TestCase):
    def test_exact_original_cache_and_failed_retry(self):
        # Test-only engine leaves the production model cache and original keywords untouched.
        engine = ENGINE + '-test'
        with patch.dict(os.environ, {'TESTING':'1'}), patch('amazonqac_translation.ENGINE', engine):
            try:
                with closing(psycopg2.connect(get_pg_dsn())) as connection:
                    import amazonqac_translation
                    amazonqac_translation.init_cache(connection)
                    with connection, connection.cursor() as cursor:
                        cursor.execute('DELETE FROM amazonqac_ui.translations WHERE engine=%s', (engine,))
                with patch('amazonqac_translation.translate_locally', return_value=[{'chinese':'收纳','preserved_terms':[]}]) as model:
                    first = translate_terms([' storage '])
                    second = translate_terms([' storage '])
                    self.assertEqual(model.call_count, 1)
                self.assertEqual(first['items'][0]['original'], ' storage ')
                self.assertEqual(second['cache_hits'], 1)
                with patch('amazonqac_translation.translate_locally', return_value=[None]) as model:
                    self.assertEqual(translate_terms(['uncached'])['failed'], 1)
                    self.assertEqual(translate_terms(['uncached'])['failed'], 1)
                    self.assertEqual(model.call_count, 2)
            finally:
                with closing(psycopg2.connect(get_pg_dsn())) as connection:
                    with connection, connection.cursor() as cursor:
                        cursor.execute('DELETE FROM amazonqac_ui.translations WHERE engine=%s', (engine,))


if __name__ == '__main__':
    unittest.main()
