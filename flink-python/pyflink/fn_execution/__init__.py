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

# Supported combinations：
# 1) PyFlink Fast + Beam Fast
# 2) PyFlink Slow + Beam Slow
# 3) PyFlink Slow + Beam Fast

# Check whether beam could be fast and force PyFlink to be slow if beam is slow
try:
    from apache_beam.coders import stream # noqa # pylint: disable=unused-import
except:
    PYFLINK_CYTHON_ENABLED = False


# Check whether PyFlink could be fast
try:
    from pyflink.fn_execution import stream_fast, coder_impl_fast \
        # noqa # pylint: disable=unused-import
    from pyflink.fn_execution.beam import \
        beam_operations_fast, beam_coder_impl_fast, beam_stream_fast \
        # noqa # pylint: disable=unused-import
    from pyflink.fn_execution.table import window_aggregate_fast, aggregate_fast \
        # noqa # pylint: disable=unused-import
except:
    PYFLINK_CYTHON_ENABLED = False
