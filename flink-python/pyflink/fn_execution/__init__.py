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

if 'PYFLINK_CYTHON_ENABLED' in os.environ:
    PYFLINK_CYTHON_ENABLED = bool(os.environ['PYFLINK_CYTHON_ENABLED'])
else:
    PYFLINK_CYTHON_ENABLED = True


try:
    # check whether beam could be fast
    from apache_beam.coders import stream

    # check whether PyFlink could be fast
    from pyflink.fn_execution import stream_fast
    from pyflink.fn_execution import coder_impl_fast
    from pyflink.fn_execution.beam import beam_operations_fast
    from pyflink.fn_execution.beam import beam_coder_impl_fast
    from pyflink.fn_execution.beam import beam_stream_fast
    from pyflink.fn_execution.table import window_aggregate_fast
    from pyflink.fn_execution.table import aggregate_fast
except:
    PYFLINK_CYTHON_ENABLED = False
