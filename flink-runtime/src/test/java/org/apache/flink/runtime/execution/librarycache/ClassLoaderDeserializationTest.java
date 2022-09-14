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
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.net.URLClassLoader;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasProperty;

/** Tests for classloader deserialize utilities. */
public class ClassLoaderDeserializationTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMessageDecodingWithUnavailableClass() throws Exception {
        final ClassLoader systemClassLoader = getClass().getClassLoader();

        final String className = "UserClass";
        final URLClassLoader userClassLoader =
                ClassLoaderUtils.compileAndLoadJava(
                        temporaryFolder.newFolder(),
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
                            1, Class.forName(className, false, userClassLoader).newInstance()
                        });

        SerializedValue<RemoteRpcInvocation> serializedMethod = new SerializedValue<>(method);

        expectedException.expect(ClassNotFoundException.class);
        expectedException.expect(
                allOf(
                        isA(ClassNotFoundException.class),
                        hasProperty(
                                "suppressed",
                                hasItemInArray(
                                        allOf(
                                                isA(ClassNotFoundException.class),
                                                hasProperty(
                                                        "message",
                                                        containsString(
                                                                "Could not deserialize 1th parameter type of method test(int, ...).")))))));

        RemoteRpcInvocation deserializedMethod =
                serializedMethod.deserializeValue(systemClassLoader);
        deserializedMethod.getMethodName();

        userClassLoader.close();
    }
}
