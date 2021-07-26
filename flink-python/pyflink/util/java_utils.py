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

from datetime import timedelta

from py4j.java_gateway import JavaClass, get_java_class, JavaObject

from pyflink.java_gateway import get_gateway


def to_jarray(j_type, arr):
    """
    Convert python list to java type array

    :param j_type: java type of element in array
    :param arr: python type list
    """
    gateway = get_gateway()
    j_arr = gateway.new_array(j_type, len(arr))
    for i in range(0, len(arr)):
        j_arr[i] = arr[i]
    return j_arr


def to_j_flink_time(time_delta):
    gateway = get_gateway()
    TimeUnit = gateway.jvm.java.util.concurrent.TimeUnit
    Time = gateway.jvm.org.apache.flink.api.common.time.Time
    if isinstance(time_delta, timedelta):
        total_microseconds = round(time_delta.total_seconds() * 1000 * 1000)
        return Time.of(total_microseconds, TimeUnit.MICROSECONDS)
    else:
        # time delta in milliseconds
        total_milliseconds = time_delta
        return Time.milliseconds(total_milliseconds)


def from_j_flink_time(j_flink_time):
    total_milliseconds = j_flink_time.toMilliseconds()
    return timedelta(milliseconds=total_milliseconds)


def load_java_class(class_name):
    gateway = get_gateway()
    context_classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()
    return context_classloader.loadClass(class_name)


def is_instance_of(java_object, java_class):
    gateway = get_gateway()
    if isinstance(java_class, str):
        param = java_class
    elif isinstance(java_class, JavaClass):
        param = get_java_class(java_class)
    elif isinstance(java_class, JavaObject):
        if not is_instance_of(java_class, gateway.jvm.Class):
            param = java_class.getClass()
        else:
            param = java_class
    else:
        raise TypeError(
            "java_class must be a string, a JavaClass, or a JavaObject")

    return gateway.jvm.org.apache.flink.api.python.shaded.py4j.reflection.TypeUtil.isInstanceOf(
        param, java_object)


def get_j_env_configuration(j_env):
    if is_instance_of(j_env, "org.apache.flink.api.java.ExecutionEnvironment"):
        return j_env.getConfiguration()
    else:
        return invoke_method(
            j_env,
            "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment",
            "getConfiguration"
        )


def invoke_method(obj, object_type, method_name, args=None, arg_types=None):
    env_clazz = load_java_class(object_type)
    method = env_clazz.getDeclaredMethod(
        method_name,
        to_jarray(
            get_gateway().jvm.Class,
            [load_java_class(arg_type) for arg_type in arg_types or []]))
    method.setAccessible(True)
    return method.invoke(obj, to_jarray(get_gateway().jvm.Object, args or []))


def is_local_deployment(j_configuration):
    jvm = get_gateway().jvm
    JDeploymentOptions = jvm.org.apache.flink.configuration.DeploymentOptions
    return j_configuration.containsKey(JDeploymentOptions.TARGET.key()) \
        and j_configuration.getString(JDeploymentOptions.TARGET.key(), None) == "local"


def add_jars_to_context_class_loader(jar_urls):
    """
    Add jars to Python gateway server for local compilation and local execution (i.e. minicluster).
    There are many component in Flink which won't be added to classpath by default. e.g. Kafka
    connector, JDBC connector, CSV format etc. This utility function can be used to hot load the
    jars.

    :param jar_urls: The list of jar urls.
    """
    gateway = get_gateway()
    # validate and normalize
    jar_urls = [gateway.jvm.java.net.URL(url).toString() for url in jar_urls]
    context_classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()
    existing_urls = []
    if context_classloader.getClass().getName() == "java.net.URLClassLoader":
        existing_urls = set([url.toString() for url in context_classloader.getURLs()])
    if all([url in existing_urls for url in jar_urls]):
        # if urls all existed, no need to create new class loader.
        return
    jar_urls.extend(existing_urls)
    # remove duplicates and create Java objects.
    j_urls = [gateway.jvm.java.net.URL(url) for url in set(jar_urls)]
    new_classloader = gateway.jvm.java.net.URLClassLoader(
        to_jarray(gateway.jvm.java.net.URL, j_urls), context_classloader)
    gateway.jvm.Thread.currentThread().setContextClassLoader(new_classloader)


def to_j_explain_detail_arr(p_extra_details):
    # sphinx will check "import loop" when generating doc,
    # use local import to avoid above error
    from pyflink.table.explain_detail import ExplainDetail
    gateway = get_gateway()

    def to_j_explain_detail(p_extra_detail):
        if p_extra_detail == ExplainDetail.JSON_EXECUTION_PLAN:
            return gateway.jvm.org.apache.flink.table.api.ExplainDetail.JSON_EXECUTION_PLAN
        elif p_extra_detail == ExplainDetail.CHANGELOG_MODE:
            return gateway.jvm.org.apache.flink.table.api.ExplainDetail.CHANGELOG_MODE
        else:
            return gateway.jvm.org.apache.flink.table.api.ExplainDetail.ESTIMATED_COST

    _len = len(p_extra_details) if p_extra_details else 0
    j_arr = gateway.new_array(gateway.jvm.org.apache.flink.table.api.ExplainDetail, _len)
    for i in range(0, _len):
        j_arr[i] = to_j_explain_detail(p_extra_details[i])

    return j_arr
