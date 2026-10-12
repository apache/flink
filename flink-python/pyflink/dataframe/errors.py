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

from typing import NoReturn

from py4j.protocol import Py4JJavaError

from pyflink.util.java_utils import is_instance_of

_USER_ERROR_CLASSES = (
    "org.apache.flink.table.api.ValidationException",
    "org.apache.flink.table.api.SqlParserException",
)


def _is_user_error(j_exception) -> bool:
    return any(is_instance_of(j_exception, j_class) for j_class in _USER_ERROR_CLASSES)


def _raise_as_value_error(error: Exception) -> NoReturn:
    """
    Re-raise ``error`` from a Flink call, turning user mistakes into :class:`ValueError`.

    When the user gets something wrong, an unknown table, a bad path, invalid options, Flink
    throws ``ValidationException`` or ``SqlParserException``. PyFlink hands those to us as a bare
    ``Py4JJavaError``. This turns them into a :class:`ValueError` with Flink's message, chained to
    the original so the Java trace is still there. Anything else is re-raised as is, including
    exceptions PyFlink already maps to Python classes such as
    :class:`~pyflink.util.exceptions.CatalogException`.
    """
    if isinstance(error, Py4JJavaError) and _is_user_error(error.java_exception):
        raise ValueError(error.java_exception.getMessage()) from error
    raise error
