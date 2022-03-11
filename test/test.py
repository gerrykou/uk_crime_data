from src.code import get_forces_list, get_neighbourhoods_list
import unittest


class TestApp(unittest.TestCase):
    # Tests
    # 01
    def test_get_forces_list(self):
        expected_output = 'metropolitan'
        actual_output = get_forces_list()[24]['id']
        self.assertEqual(expected_output, actual_output)


    def test_get_neighbourhoods_list(self):
        # Tests
        # 02
        actual_output = get_neighbourhoods_list('metropolitan')
        self.assertIsInstance(actual_output, list)

if __name__ == '__main__':
    unittest.main()
