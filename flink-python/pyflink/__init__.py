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
#################################################################################
import sys
from functools import wraps

if sys.version_info < (3, 5):
    raise RuntimeError(
        'Python versions prior to 3.5 are not supported for PyFlink [' +
        str(sys.version_info) + '].')


def keyword(func):
    """
    A decorator that forces keyword arguments usage and store actual
    input keyword arguments in `_input_kwargs`.
    """
    @wraps(func)
    def wrapper(self, **kwargs):
        self._input_kwargs = kwargs
        return func(self, **kwargs)
    return wrapper
