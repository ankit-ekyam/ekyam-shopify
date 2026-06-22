import unittest
from mapping_utils import _apply_rule

class TestTransformerLogic(unittest.TestCase):

    def test_divide_transformer(self):
        # A valid rule and data
        rule = {"transformer": "divide", "paths": ["price", "quantity"]}
        data = {"price": 100, "quantity": 4}
        
        result = _apply_rule(rule, data)
        self.assertEqual(result, 25.0)

    def test_divide_by_zero_handling(self):
        # Testing if it handles zero division safely (should return 0.0)
        rule = {"transformer": "divide", "paths": ["price", "quantity"]}
        data = {"price": 100, "quantity": 0}
        
        result = _apply_rule(rule, data)
        self.assertEqual(result, 0.0)

    def test_missing_fields_handling(self):
        # Testing if it handles missing fields without crashing
        rule = {"transformer": "divide", "paths": ["price", "quantity"]}
        data = {"price": 100} # missing quantity
        
        result = _apply_rule(rule, data)
        self.assertEqual(result, 0.0)

    def test_non_existent_transformer(self):
        # Testing if it safely handles a transformer that doesn't exist
        rule = {"transformer": "magical_multiply", "paths": ["price", "quantity"]}
        data = {"price": 100, "quantity": 4}
        
        result = _apply_rule(rule, data)
        self.assertIsNone(result)

    def test_standard_string_path_rule(self):
        # Ensuring it didn't break the normal string path logic
        rule = "customer.id"
        data = {"customer": {"id": 99999}}
        
        result = _apply_rule(rule, data)
        self.assertEqual(result, 99999)

if __name__ == '__main__':
    unittest.main()
