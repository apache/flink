/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedThrowable;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link SerializedThrowable}. */
class SerializedThrowableTest {

    @Test
    void testIdenticalMessageAndStack() {
        try {
            IllegalArgumentException original = new IllegalArgumentException("test message");
            SerializedThrowable serialized = new SerializedThrowable(original);

            assertThat(ExceptionUtils.stringifyException(original))
                    .isEqualTo(ExceptionUtils.stringifyException(serialized));

            assertThat(original.getStackTrace()).isEqualTo(serialized.getStackTrace());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testSerialization() {
        try {
            // We need an exception whose class is not in the core class loader
            final ClassLoaderUtils.ObjectAndClassLoader<Exception> outsideClassLoading =
                    ClassLoaderUtils.createExceptionObjectFromNewClassLoader();
            ClassLoader loader = outsideClassLoading.getClassLoader();
            Exception userException = outsideClassLoading.getObject();
            Class<?> clazz = userException.getClass();

            // check that we cannot simply copy the exception
            byte[] serializedBytes = InstantiationUtil.serializeObject(userException);
            assertThatThrownBy(
                            () ->
                                    InstantiationUtil.deserializeObject(
                                            serializedBytes, getClass().getClassLoader()))
                    .withFailMessage("should fail with a class not found exception")
                    .isInstanceOf(ClassNotFoundException.class);

            // validate that the SerializedThrowable mimics the original exception
            SerializedThrowable serialized = new SerializedThrowable(userException);
            assertThat(ExceptionUtils.stringifyException(userException))
                    .isEqualTo(ExceptionUtils.stringifyException(serialized));
            assertThat(userException.getStackTrace()).isEqualTo(serialized.getStackTrace());

            // validate the detailMessage of SerializedThrowable contains the class name of original
            // exception
            Exception userException2 = new Exception("error");
            SerializedThrowable serialized2 = new SerializedThrowable(userException2);
            String result =
                    String.format(
                            "%s: %s",
                            userException2.getClass().getName(), userException2.getMessage());
            assertThat(serialized2).hasMessage(result);

            // copy the serialized throwable and make sure everything still works
            SerializedThrowable copy = CommonTestUtils.createCopySerializable(serialized);
            assertThat(ExceptionUtils.stringifyException(userException))
                    .isEqualTo(ExceptionUtils.stringifyException(copy));
            assertThat(userException.getStackTrace()).isEqualTo(copy.getStackTrace());

            // deserialize the proper exception
            Throwable deserialized = copy.deserializeError(loader);
            assertThat(deserialized).isInstanceOf(clazz);

            // deserialization with the wrong classloader does not lead to a failure
            Throwable wronglyDeserialized = copy.deserializeError(getClass().getClassLoader());
            assertThat(ExceptionUtils.stringifyException(userException))
                    .isEqualTo(ExceptionUtils.stringifyException(wronglyDeserialized));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCauseChaining() {
        Exception cause2 = new Exception("level2");
        Exception cause1 = new Exception("level1", cause2);
        Exception root = new Exception("level0", cause1);

        SerializedThrowable st = new SerializedThrowable(root);

        assertThat(st).hasMessage("java.lang.Exception: level0");
        assertThat(st.getCause()).isNotNull().hasMessage("java.lang.Exception: level1");
        assertThat(st.getCause().getCause()).isNotNull().hasMessage("java.lang.Exception: level2");
    }

    @Test
    void testCyclicCauseChaining() {
        Exception cause3 = new Exception("level3");
        Exception cause2 = new Exception("level2", cause3);
        Exception cause1 = new Exception("level1", cause2);
        Exception root = new Exception("level0", cause1);

        // introduce a cyclic reference
        cause3.initCause(cause1);

        SerializedThrowable st = new SerializedThrowable(root);

        assertThat(root.getStackTrace()).isEqualTo(st.getStackTrace());
        assertThat(ExceptionUtils.stringifyException(root))
                .isEqualTo(ExceptionUtils.stringifyException(st));
    }

    @Test
    void testCopyPreservesCause() {
        Exception original = new Exception("original message");
        Exception parent = new Exception("parent message", original);

        SerializedThrowable serialized = new SerializedThrowable(parent);
        assertThat(serialized.getCause()).isNotNull();

        SerializedThrowable copy = new SerializedThrowable(serialized);
        assertThat(copy)
                .hasMessage(
                        "org.apache.flink.util.SerializedThrowable: java.lang.Exception: parent message");
        assertThat(copy.getCause()).isNotNull().hasMessage("java.lang.Exception: original message");
    }

    @Test
    void testSuppressedTransferring() {
        Exception root = new Exception("root");
        Exception suppressed = new Exception("suppressed");
        root.addSuppressed(suppressed);

        SerializedThrowable serializedThrowable = new SerializedThrowable(root);

        assertThat(serializedThrowable.getSuppressed()).hasSize(1);
        Throwable actualSuppressed = serializedThrowable.getSuppressed()[0];
        assertThat(actualSuppressed)
                .isInstanceOf(SerializedThrowable.class)
                .hasMessage("java.lang.Exception: suppressed");
    }

    @Test
    void testCopySuppressed() {
        Exception root = new Exception("root");
        Exception suppressed = new Exception("suppressed");
        root.addSuppressed(suppressed);

        SerializedThrowable serializedThrowable = new SerializedThrowable(root);
        SerializedThrowable copy = new SerializedThrowable(serializedThrowable);

        assertThat(copy.getSuppressed()).hasSize(1);
        Throwable actualSuppressed = copy.getSuppressed()[0];
        assertThat(actualSuppressed)
                .isInstanceOf(SerializedThrowable.class)
                .hasMessage("java.lang.Exception: suppressed");
    }
}
