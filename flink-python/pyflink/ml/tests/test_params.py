################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import array
import unittest

from pyflink import keyword
from pyflink.ml.api.param import ParamInfo, TypeConverters, Params
from pyflink.ml.lib.param.colname import HasSelectedCols, HasOutputCol


class ParamsTest(unittest.TestCase):

    def test_default_behavior(self):
        params = Params()

        not_optinal = ParamInfo("a", "", is_optional=False)
        with self.assertRaises(ValueError):
            params.get(not_optinal)

        # get optional without default param
        optional_without_default = ParamInfo("a", "")
        with self.assertRaises(ValueError):
            params.get(optional_without_default)

    def test_get_optional_param(self):
        param_info = ParamInfo(
            "key",
            "",
            has_default_value=True,
            default_value=None,
            type_converter=TypeConverters.to_string)

        params = Params()
        self.assertIsNone(params.get(param_info))

        val = "3"
        params.set(param_info, val)
        self.assertEqual(val, params.get(param_info))

        params.set(param_info, None)
        self.assertIsNone(params.get(param_info))

    def test_remove_contains_size_clear_is_empty(self):
        param_info = ParamInfo(
            "key",
            "",
            has_default_value=True,
            default_value=None,
            type_converter=TypeConverters.to_string)

        params = Params()
        self.assertEqual(params.size(), 0)
        self.assertTrue(params.is_empty())

        val = "3"
        params.set(param_info, val)
        self.assertEqual(params.size(), 1)
        self.assertFalse(params.is_empty())

        params_json = params.to_json()
        params_new = Params.from_json(params_json)
        self.assertEqual(params.get(param_info), val)
        self.assertEqual(params_new.get(param_info), val)

        params.clear()
        self.assertEqual(params.size(), 0)
        self.assertTrue(params.is_empty())

    def test_to_from_json(self):
        import jsonpickle

        param_info = ParamInfo(
            "key",
            "",
            has_default_value=True,
            default_value=None,
            type_converter=TypeConverters.to_string)

        param_info_new = jsonpickle.decode(jsonpickle.encode(param_info))
        self.assertEqual(param_info_new, param_info)

        params = Params()
        val = "3"
        params.set(param_info, val)
        params_new = Params.from_json(params.to_json())
        self.assertEqual(params_new.get(param_info), val)


class ParamTypeConversionTests(unittest.TestCase):
    """
    Test that param type conversion happens.
    """

    def test_list(self):
        l = [0, 1]
        for lst_like in [l, range(2), tuple(l), array.array('l', l)]:
            converted = TypeConverters.to_list(lst_like)
            self.assertEqual(type(converted), list)
            self.assertListEqual(converted, l)

    def test_list_float_or_list_int(self):
        l = [0, 1]
        for lst_like in [l, range(2), tuple(l), array.array('l', l)]:
            converted1 = TypeConverters.to_list_float(lst_like)
            converted2 = TypeConverters.to_list_int(lst_like)
            self.assertEqual(type(converted1), list)
            self.assertEqual(type(converted2), list)
            self.assertListEqual(converted1, l)
            self.assertListEqual(converted2, l)

    def test_list_string(self):
        l = ["aa", "bb"]
        for lst_like in [l, tuple(l)]:
            converted = TypeConverters.to_list_string(lst_like)
            self.assertEqual(type(converted), list)
            self.assertListEqual(converted, l)

    def test_float(self):
        data = 1.45
        converted = TypeConverters.to_float(data)
        self.assertEqual(type(converted), float)
        self.assertEqual(converted, data)

    def test_int(self):
        data = 1234567890
        converted = TypeConverters.to_int(data)
        self.assertEqual(type(converted), int)
        self.assertEqual(converted, data)

    def test_string(self):
        data = "1234567890"
        converted = TypeConverters.to_string(data)
        self.assertEqual(type(converted), str)
        self.assertEqual(converted, data)

    def test_boolean(self):
        data = True
        converted = TypeConverters.to_boolean(data)
        self.assertEqual(type(converted), bool)
        self.assertEqual(converted, data)


class MockVectorAssembler(HasSelectedCols, HasOutputCol):

    @keyword
    def __init__(self, *, selected_cols=None, output_col=None):
        self._params = Params()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def get_params(self):
        return self._params


class TestWithParams(unittest.TestCase):

    def test_set_params_with_keyword_arguments(self):
        assembler = MockVectorAssembler(selected_cols=["a", "b"], output_col="features")
        params = assembler.get_params()
        self.assertEqual(params.size(), 2)
        self.assertEqual(assembler.get(HasSelectedCols.selected_cols), ["a", "b"])
        self.assertEqual(assembler.get(HasOutputCol.output_col), "features")

    def test_set_params_with_builder_mode(self):
        assembler = MockVectorAssembler()\
            .set_selected_cols(["a", "b"])\
            .set_output_col("features")
        params = assembler.get_params()
        self.assertEqual(params.size(), 2)
        self.assertEqual(assembler.get(HasSelectedCols.selected_cols), ["a", "b"])
        self.assertEqual(assembler.get(HasOutputCol.output_col), "features")
