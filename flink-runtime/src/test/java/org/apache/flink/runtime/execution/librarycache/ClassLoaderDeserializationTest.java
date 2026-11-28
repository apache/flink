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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for classloader deserialize utilities. */
class ClassLoaderDeserializationTest {

    @Test
    void testMessageDecodingWithUnavailableClass(@TempDir Path temporaryFolder) throws Exception {
        final ClassLoader systemClassLoader = getClass().getClassLoader();

        final String className = "UserClass";
        final URLClassLoader userClassLoader =
                ClassLoaderUtils.compileAndLoadJava(
                        temporaryFolder.toFile(),
                        className + ".java",
                        "import java.io.Serializable;\n"
                                + "public class "
                                + className
                                + " implements Serializable {}");

        RemoteRpcInvocation method =
                new RemoteRpcInvocation(
                        className,
                        "test",
                        new Class<?>[] {
                            int.class, Class.forName(className, false, userClassLoader)
                        },
                        new Object[] {
                            1,
                            Class.forName(className, false, userClassLoader)
                                    .getDeclaredConstructor()
                                    .newInstance()
                        });

        SerializedValue<RemoteRpcInvocation> serializedMethod = new SerializedValue<>(method);

        final Throwable thrown =
                catchThrowable(
                        () -> serializedMethod.deserializeValue(systemClassLoader).getMethodName());

        assertThat(thrown).isInstanceOf(ClassNotFoundException.class);
        assertThat(thrown.getSuppressed())
                .anySatisfy(
                        suppressed ->
                                assertThat(suppressed)
                                        .isInstanceOf(ClassNotFoundException.class)
                                        .hasMessageContaining(
                                                "Could not deserialize 1th parameter type of method test(int, ...)."));

        userClassLoader.close();
    }
}
