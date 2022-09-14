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
from py4j.protocol import Py4JJavaError

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
    env_clazz = load_java_class(
        "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
    field = env_clazz.getDeclaredField("configuration")
    field.setAccessible(True)
    return field.get(j_env)


def get_field_value(java_obj, field_name):
    field = get_field(java_obj.getClass(), field_name)
    return field.get(java_obj)


def get_field(cls, field_name):
    try:
        field = cls.getDeclaredField(field_name)
        field.setAccessible(True)
        return field
    except Py4JJavaError:
        while cls.getSuperclass() is not None:
            cls = cls.getSuperclass()
            try:
                field = cls.getDeclaredField(field_name)
                field.setAccessible(True)
                return field
            except Py4JJavaError:
                pass


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
        and j_configuration.getString(JDeploymentOptions.TARGET.key(), None) in \
        ("local", "minicluster")


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
    jar_urls = [gateway.jvm.java.net.URL(url) for url in jar_urls]
    context_classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()
    existing_urls = []
    class_loader_name = context_classloader.getClass().getName()
    if class_loader_name == "java.net.URLClassLoader":
        existing_urls = set([url.toString() for url in context_classloader.getURLs()])
    if all([url.toString() in existing_urls for url in jar_urls]):
        # if urls all existed, no need to create new class loader.
        return

    URLClassLoaderClass = load_java_class("java.net.URLClassLoader")
    if is_instance_of(context_classloader, URLClassLoaderClass):
        if class_loader_name == "org.apache.flink.runtime.execution.librarycache." \
                                "FlinkUserCodeClassLoaders$SafetyNetWrapperClassLoader":
            ensureInner = context_classloader.getClass().getDeclaredMethod("ensureInner", None)
            ensureInner.setAccessible(True)
            context_classloader = ensureInner.invoke(context_classloader, None)

        addURL = URLClassLoaderClass.getDeclaredMethod(
            "addURL",
            to_jarray(
                gateway.jvm.Class,
                [load_java_class("java.net.URL")]))
        addURL.setAccessible(True)

        for url in jar_urls:
            addURL.invoke(context_classloader, to_jarray(get_gateway().jvm.Object, [url]))

    else:
        context_classloader = create_url_class_loader(jar_urls, context_classloader)
        gateway.jvm.Thread.currentThread().setContextClassLoader(context_classloader)


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


def create_url_class_loader(urls, parent_class_loader):
    gateway = get_gateway()
    url_class_loader = gateway.jvm.java.net.URLClassLoader(
        to_jarray(gateway.jvm.java.net.URL, urls), parent_class_loader)
    return url_class_loader
