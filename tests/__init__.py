from pathlib import Path
from unittest import TestCase
from unittest import main as unittest_main


class BaseTest(TestCase):
    def setUp(self):
        # Create a temporary directory for output files
        self.test_dir = Path('test_analysis')
        self.test_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        # Remove the test directory after tests
        for file in self.test_dir.glob('*'):
            file.unlink()
        self.test_dir.rmdir()


if __name__ == '__main__':
    unittest_main()
