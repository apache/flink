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

if sys.version_info < (3, 5):
    raise RuntimeError(
        'Python versions prior to 3.5 are not supported for PyFlink [' +
        str(sys.version_info) + '].')


def since(version):
    """
    A decorator that annotates a function to append the version the function was added.
    """
    import re
    indent_p = re.compile(r'\n( +)')

    def deco(f):
        original_doc = f.__doc__ or ""
        indents = indent_p.findall(original_doc)
        indent = ' ' * (min(len(indent) for indent in indents) if indents else 0)
        f.__doc__ = original_doc.rstrip() + "\n\n%s.. versionadded:: %s" % (indent, version)
        return f
    return deco
