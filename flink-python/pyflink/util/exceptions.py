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
import py4j
from py4j.protocol import Py4JJavaError


class JavaException(Exception):
    def __init__(self, msg, stack_trace):
        self.msg = msg
        self.stack_trace = stack_trace

    def __str__(self):
        return self.msg + "\n\t at " + self.stack_trace


class TableException(JavaException):
    """
    General Exception for all errors during table handling.
    """


class CatalogException(JavaException):
    """
    A catalog-related exception.
    """


class DatabaseAlreadyExistException(JavaException):
    """
    Exception for trying to create a database that already exists.
    """


class DatabaseNotEmptyException(JavaException):
    """
    Exception for trying to drop on a database that is not empty.
    """


class DatabaseNotExistException(JavaException):
    """
    Exception for trying to operate on a database that doesn't exist.
    """


class FunctionAlreadyExistException(JavaException):
    """
    Exception for trying to create a function that already exists.
    """


class FunctionNotExistException(JavaException):
    """
    Exception for trying to operate on a function that doesn't exist.
    """


class PartitionAlreadyExistsException(JavaException):
    """
    Exception for trying to create a partition that already exists.
    """


class PartitionNotExistException(JavaException):
    """
    Exception for operation on a partition that doesn't exist. The cause includes non-existent
    table, non-partitioned table, invalid partition spec, etc.
    """


class PartitionSpecInvalidException(JavaException):
    """
    Exception for invalid PartitionSpec compared with partition key list of a partitioned Table.
    For example, it is thrown when the size of PartitionSpec exceeds the size of partition key
    list, or when the size of PartitionSpec is 'n' but its keys don't match the first 'n' keys in
    partition key list.
    """


class TableAlreadyExistException(JavaException):
    """
    Exception for trying to create a table (or view) that already exists.
    """


class TableNotExistException(JavaException):
    """
    Exception for trying to operate on a table (or view) that doesn't exist.
    """


class TableNotPartitionedException(JavaException):
    """
    Exception for trying to operate partition on a non-partitioned table.
    """


# Mapping from JavaException to PythonException
exception_mapping = {
    "org.apache.flink.table.api.TableException":
        TableException,
    "org.apache.flink.table.catalog.exceptions.CatalogException":
        CatalogException,
    "org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException":
        DatabaseAlreadyExistException,
    "org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException":
        DatabaseNotEmptyException,
    "org.apache.flink.table.catalog.exceptions.DatabaseNotExistException":
        DatabaseNotExistException,
    "org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException":
        FunctionAlreadyExistException,
    "org.apache.flink.table.catalog.exceptions.FunctionNotExistException":
        FunctionNotExistException,
    "org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException":
        PartitionAlreadyExistsException,
    "org.apache.flink.table.catalog.exceptions.PartitionNotExistException":
        PartitionNotExistException,
    "org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException":
        PartitionSpecInvalidException,
    "org.apache.flink.table.catalog.exceptions.TableAlreadyExistException":
        TableAlreadyExistException,
    "org.apache.flink.table.catalog.exceptions.TableNotExistException":
        TableNotExistException,
    "org.apache.flink.table.catalog.exceptions.TableNotPartitionedException":
        TableNotPartitionedException,
}


def capture_java_exception(f):
    def deco(*a, **kw):
        try:
            return f(*a, **kw)
        except Py4JJavaError as e:
            from pyflink.java_gateway import get_gateway
            get_gateway().jvm.org.apache.flink.client.python.PythonEnvUtils\
                .setPythonException(e.java_exception)
            s = e.java_exception.toString()
            stack_trace = '\n\t at '.join(map(lambda x: x.toString(),
                                              e.java_exception.getStackTrace()))
            for exception in exception_mapping.keys():
                if s.startswith(exception):
                    java_exception = \
                        exception_mapping[exception](s.split(': ', 1)[1], stack_trace)
                    break
            else:
                raise
        raise java_exception
    return deco


def install_exception_handler():
    """
    Hook an exception handler into Py4j, which could capture some exceptions in Java.

    When calling Java API, it will call `get_return_value` to parse the returned object.
    If any exception happened in JVM, the result will be a Java exception object, it raise
    py4j.protocol.Py4JJavaError. We replace the original `get_return_value` with one that
    could capture the Java exception and throw a Python one (with the same error message).

    It's idempotent, could be called multiple times.
    """
    original = py4j.protocol.get_return_value
    # The original `get_return_value` is not patched, it's idempotent.
    patched = capture_java_exception(original)
    # only patch the one used in py4j.java_gateway (call Java API)
    py4j.java_gateway.get_return_value = patched


def install_py4j_hooks():
    """
    Hook the classes such as JavaPackage, etc of Py4j to improve the exception message.
    """
    def wrapped_call(self, *args, **kwargs):
        raise TypeError(
            "Could not found the Java class '%s'. The Java dependencies could be specified via "
            "command line argument '--jarfile' or the config option 'pipeline.jars'" % self._fqn)

    setattr(py4j.java_gateway.JavaPackage, '__call__', wrapped_call)


def convert_py4j_exception(e: Py4JJavaError) -> JavaException:
    """
    Convert Py4J exception to JavaException.
    """
    def extract_java_stack_trace(java_stack_trace):
        return '\n\t at '.join(map(lambda x: x.toString(), java_stack_trace))

    s = e.java_exception.toString()
    cause = e.java_exception.getCause()
    stack_trace = extract_java_stack_trace(e.java_exception.getStackTrace())
    while cause is not None:
        stack_trace += '\nCaused by: %s: %s' % (cause.getClass().getName(), cause.getMessage())
        stack_trace += "\n\t at " + extract_java_stack_trace(cause.getStackTrace())
        cause = cause.getCause()
    return JavaException(s.split(': ', 1)[1], stack_trace)
