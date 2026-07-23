import unittest
from pyflink.datastream.functions import (
    MapFunction, FlatMapFunction, FilterFunction, KeySelector
)
from typing import Iterator

class TypedMapFunction(MapFunction[int, str]):
    def map(self, value: int) -> str:
        return str(value)

class TypedFlatMapFunction(FlatMapFunction[int, str]):
    def flat_map(self, value: int) -> Iterator[str]:
        yield str(value)

class TypedFilterFunction(FilterFunction[int]):
    def filter(self, value: int) -> bool:
        return value > 0

class TypedKeySelector(KeySelector[int, str]):
    def get_key(self, value: int) -> str:
        return str(value)

class TestGenericTyping(unittest.TestCase):
    def test_map_function_generics(self):
        f = TypedMapFunction()
        self.assertEqual(f.map(1), "1")

    def test_flat_map_function_generics(self):
        f = TypedFlatMapFunction()
        self.assertEqual(list(f.flat_map(1)), ["1"])

    def test_filter_function_generics(self):
        f = TypedFilterFunction()
        self.assertTrue(f.filter(1))
        self.assertFalse(f.filter(-1))

    def test_key_selector_generics(self):
        f = TypedKeySelector()
        self.assertEqual(f.get_key(1), "1")

if __name__ == '__main__':
    unittest.main()
