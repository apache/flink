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

from pyflink.ml.api.ml_environment_factory import MLEnvironmentFactory, MLEnvironment
from pyflink.testing.test_case_utils import MLTestCase


class MLEnvironmentFactoryTest(MLTestCase):

    def test_get_default(self):
        ml_env1 = MLEnvironmentFactory.get_default()
        ml_env2 = MLEnvironmentFactory.get_default()
        self.assertEqual(ml_env1, ml_env2)

    def test_register_and_get_ml_environment(self):
        ml_environment = MLEnvironment()
        # test register
        id = MLEnvironmentFactory.register_ml_environment(ml_environment)
        # test get
        ml_environment_2 = MLEnvironmentFactory.get(id)
        self.assertEqual(ml_environment, ml_environment_2)

    def test_get_new_ml_environment_id_and_remove(self):
        # test get_new_ml_environment_id
        id = MLEnvironmentFactory.get_new_ml_environment_id()
        ml_environment = MLEnvironmentFactory.get(id)
        # test remove
        ml_environment_2 = MLEnvironmentFactory.remove(id)
        self.assertEqual(ml_environment, ml_environment_2)
        # test remove default
        self.assertEqual(
            MLEnvironmentFactory.remove(0),
            MLEnvironmentFactory.get_default())
