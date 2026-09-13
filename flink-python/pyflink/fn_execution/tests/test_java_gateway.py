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
import os
import sys
import unittest
from unittest.mock import patch

from pyflink import java_gateway


class JavaGatewayTests(unittest.TestCase):

    def test_embedded_gateway_access_is_rejected_before_locking(self):
        for cached_gateway in [None, object()]:
            for gateway_port in [None, '12345']:
                with self.subTest(cached=cached_gateway is not None, port=gateway_port):
                    with patch.dict(sys.modules, {'_pemja': object()}), \
                            patch.dict(os.environ), \
                            patch.object(java_gateway, '_gateway', cached_gateway), \
                            patch.object(java_gateway, '_lock') as lock:
                        os.environ.pop('PYFLINK_GATEWAY_PORT', None)
                        if gateway_port is not None:
                            os.environ['PYFLINK_GATEWAY_PORT'] = gateway_port
                        lock.__enter__.side_effect = AssertionError('Gateway lock acquired')

                        with self.assertRaisesRegex(RuntimeError, r'pemja\.findClass'):
                            java_gateway.get_gateway()

                        lock.__enter__.assert_not_called()

    def test_embedded_gateway_launch_is_rejected(self):
        with patch.dict(sys.modules, {'_pemja': object()}), \
                patch.object(java_gateway, 'is_launch_gateway_disabled', return_value=False), \
                patch.object(java_gateway, '_find_flink_home'), \
                patch.object(java_gateway, 'launch_gateway_server_process') as launch:
            launch.side_effect = AssertionError('Gateway subprocess launched')

            with self.assertRaisesRegex(RuntimeError, r'pemja\.findClass'):
                java_gateway.launch_gateway()

            launch.assert_not_called()

    def test_client_gateway_is_available_when_pemja_package_is_imported(self):
        cached_gateway = object()
        with patch.dict(sys.modules, {'pemja': object()}), \
                patch.object(java_gateway, '_gateway', cached_gateway):
            sys.modules.pop('_pemja', None)

            self.assertIs(java_gateway.get_gateway(), cached_gateway)

    def test_process_worker_gateway_launch_is_still_disabled(self):
        with patch.dict(sys.modules), \
                patch.dict(os.environ, {'PYFLINK_GATEWAY_DISABLED': 'true'}), \
                patch.object(java_gateway, 'launch_gateway_server_process') as launch:
            sys.modules.pop('_pemja', None)

            with self.assertRaisesRegex(Exception, 'during Python UDF execution'):
                java_gateway.launch_gateway()

            launch.assert_not_called()
