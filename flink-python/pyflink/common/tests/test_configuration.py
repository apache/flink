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
from pyflink.common import Configuration
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ConfigurationTests(PyFlinkTestCase):

    def test_init(self):
        conf = Configuration()

        self.assertEqual(conf.to_dict(), dict())

        conf.set_string("k1", "v1")
        conf2 = Configuration(conf)

        self.assertEqual(conf2.to_dict(), {"k1": "v1"})

    def test_get_set_string(self):
        conf = Configuration()

        conf.set_string("k1", "v1")
        value = conf.get_string("k1", "")

        self.assertEqual(value, "v1")

    def test_get_set_integer(self):
        conf = Configuration()

        conf.set_integer("k1", 2)
        value = conf.get_integer("k1", 0)

        self.assertEqual(value, 2)

    def test_get_set_boolean(self):
        conf = Configuration()

        conf.set_boolean("k1", True)
        value = conf.get_boolean("k1", False)

        self.assertEqual(value, True)

    def test_get_set_float(self):
        conf = Configuration()

        conf.set_float("k1", 0.5)
        value = conf.get_float("k1", 0)

        self.assertEqual(value, 0.5)

    def test_get_set_bytearray(self):
        conf = Configuration()

        conf.set_bytearray("k1", bytearray([1, 2, 3]))
        value = conf.get_bytearray("k1", bytearray())

        self.assertEqual(value, bytearray([1, 2, 3]))

    def test_key_set(self):
        conf = Configuration()

        conf.set_string("k1", "v1")
        conf.set_string("k2", "v2")
        conf.set_string("k3", "v3")
        key_set = conf.key_set()

        self.assertEqual(key_set, {"k1", "k2", "k3"})

    def test_add_all_to_dict(self):
        conf = Configuration()

        conf.set_string("k1", "v1")
        conf.set_integer("k2", 1)
        conf.set_float("k3", 1.2)
        conf.set_boolean("k4", True)
        conf.set_bytearray("k5", bytearray([1, 2, 3]))
        target_dict = dict()
        conf.add_all_to_dict(target_dict)

        self.assertEqual(target_dict, {"k1": "v1",
                                       "k2": 1,
                                       "k3": 1.2,
                                       "k4": True,
                                       "k5": bytearray([1, 2, 3])})

    def test_add_all(self):
        conf = Configuration()
        conf.set_string("k1", "v1")
        conf2 = Configuration()

        conf2.add_all(conf)
        value1 = conf2.get_string("k1", "")

        self.assertEqual(value1, "v1")

        conf2.add_all(conf, "conf_")
        value2 = conf2.get_string("conf_k1", "")

        self.assertEqual(value2, "v1")

    def test_clone(self):
        conf = Configuration()
        conf.set_string("k1", "v1")

        conf2 = conf.clone()

        self.assertEqual(conf2, conf)

        conf2.set_string("k1", "v2")

        self.assertNotEqual(conf2, conf)

    def test_contains_key(self):
        conf = Configuration()
        conf.set_string("k1", "v1")

        contains_k1 = conf.contains_key("k1")
        contains_k2 = conf.contains_key("k2")

        self.assertTrue(contains_k1)
        self.assertFalse(contains_k2)

    def test_to_dict(self):
        conf = Configuration()
        conf.set_string("k1", "v1")
        conf.set_integer("k2", 1)
        conf.set_float("k3", 1.2)
        conf.set_boolean("k4", True)

        target_dict = conf.to_dict()

        self.assertEqual(target_dict, {"k1": "v1", "k2": "1", "k3": "1.2", "k4": "true"})

    def test_remove_config(self):
        conf = Configuration()
        conf.set_string("k1", "v1")
        conf.set_integer("k2", 1)

        self.assertTrue(conf.contains_key("k1"))
        self.assertTrue(conf.contains_key("k2"))

        self.assertTrue(conf.remove_config("k1"))
        self.assertFalse(conf.remove_config("k1"))

        self.assertFalse(conf.contains_key("k1"))

        conf.remove_config("k2")

        self.assertFalse(conf.contains_key("k2"))

    def test_hash_equal_str(self):
        conf = Configuration()
        conf2 = Configuration()

        conf.set_string("k1", "v1")
        conf.set_integer("k2", 1)
        conf2.set_string("k1", "v1")

        self.assertNotEqual(hash(conf), hash(conf2))
        self.assertNotEqual(conf, conf2)

        conf2.set_integer("k2", 1)

        self.assertEqual(hash(conf), hash(conf2))
        self.assertEqual(conf, conf2)

        self.assertEqual(str(conf), "{k1=v1, k2=1}")
