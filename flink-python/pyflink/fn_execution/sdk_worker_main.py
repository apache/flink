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

# force to register the operations to SDK Harness
from apache_beam.options.pipeline_options import PipelineOptions

try:
    import pyflink.fn_execution.fast_operations
except ImportError:
    import pyflink.fn_execution.operations

# force to register the coders to SDK Harness
import pyflink.fn_execution.coders # noqa # pylint: disable=unused-import

import apache_beam.runners.worker.sdk_worker_main

if 'PIPELINE_OPTIONS' in os.environ:
    pipeline_options = apache_beam.runners.worker.sdk_worker_main._parse_pipeline_options(
        os.environ['PIPELINE_OPTIONS'])
else:
    pipeline_options = PipelineOptions.from_dictionary({})

if __name__ == '__main__':
    apache_beam.runners.worker.sdk_worker_main.main(sys.argv)
