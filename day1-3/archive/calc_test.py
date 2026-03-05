import unittest
from archive.calculator import Calculator

class TestOperations(unittest.TestCase):

    def test_sum(self):
        calculation = Calculator(2, 2)
        self.assertEqual(calculation.add(), 4, "The answer is not 4!!")

    def test_product(self):
        calculation = Calculator(3, 3)
        self.assertEqual(calculation.multiply(), 9, "The product is incorrect!")

    def test_subtract(self):
        calculation = Calculator(5, 2)
        self.assertEqual(calculation.subtract(), 3, "The subtraction result is incorrect!")

    def test_divide(self):
        calculation = Calculator(10, 2)
        self.assertEqual(calculation.divide(), 5, "The division result is incorrect!")


if __name__ == "__main__":
    unittest.main()