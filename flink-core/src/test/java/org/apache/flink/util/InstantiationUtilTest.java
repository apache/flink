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

package org.apache.flink.util;

import org.apache.flink.api.common.typeutils.base.DoubleValueSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URLClassLoader;
import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link InstantiationUtil}. */
class InstantiationUtilTest {

    @TempDir private static java.nio.file.Path tempFolder;

    private static final String PROXY_DEFINITION_FORMAT =
            "import java.lang.reflect.InvocationHandler;"
                    + "import java.lang.reflect.Method;"
                    + "import java.io.Serializable;"
                    + "public class %s implements InvocationHandler, Serializable {\n"
                    + "\n"
                    + "  @Override\n"
                    + "  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {\n"
                    + "    return null;\n"
                    + "  }\n"
                    + "}";

    @Test
    void testResolveProxyClass() throws Exception {
        final String interfaceName = "UserDefinedInterface";
        final String proxyName = "UserProxy";

        try (URLClassLoader userClassLoader = createClassLoader(interfaceName, proxyName)) {
            Class<?> userInterface = Class.forName(interfaceName, false, userClassLoader);
            InvocationHandler userProxy =
                    (InvocationHandler)
                            Class.forName(proxyName, false, userClassLoader).newInstance();

            Object proxy =
                    Proxy.newProxyInstance(userClassLoader, new Class[] {userInterface}, userProxy);

            byte[] serializeObject = InstantiationUtil.serializeObject(proxy);
            Object deserializedProxy =
                    InstantiationUtil.deserializeObject(serializeObject, userClassLoader);
            assertThat(deserializedProxy).isNotNull();
        }
    }

    private URLClassLoader createClassLoader(String interfaceName, String proxyName)
            throws IOException {
        return ClassLoaderUtils.withRoot(TempDirUtils.newFolder(tempFolder))
                .addClass(
                        interfaceName, String.format("interface %s { void test();}", interfaceName))
                .addClass(proxyName, createProxyDefinition(proxyName))
                .build();
    }

    private String createProxyDefinition(String proxyName) {
        return String.format(PROXY_DEFINITION_FORMAT, proxyName);
    }

    @Test
    void testInstantiationOfStringValue() {
        Object stringValue = InstantiationUtil.instantiate(StringValue.class, null);
        assertThat(stringValue).isNotNull();
    }

    @Test
    void testInstantiationOfStringValueAndCastToValue() {
        Object stringValue = InstantiationUtil.instantiate(StringValue.class, Value.class);
        assertThat(stringValue).isNotNull();
    }

    @Test
    void testHasNullaryConstructor() {
        assertThat(InstantiationUtil.hasPublicNullaryConstructor(StringValue.class)).isTrue();
    }

    /**
     * Test that {@link InstantiationUtil} class per se does not have a nullary public constructor.
     */
    @Test
    void testHasNullaryConstructorFalse() {
        assertThat(InstantiationUtil.hasPublicNullaryConstructor(InstantiationUtil.class))
                .isFalse();
    }

    @Test
    void testClassIsProper() {
        assertThat(InstantiationUtil.isProperClass(StringValue.class)).isTrue();
    }

    @Test
    void testClassIsNotProper() {
        assertThat(InstantiationUtil.isProperClass(Value.class)).isFalse();
    }

    @Test
    void testCheckForInstantiationOfPrivateClass() {
        assertThatThrownBy(() -> InstantiationUtil.checkForInstantiation(TestClass.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testSerializationToByteArray() throws IOException {
        final DoubleValue toSerialize = new DoubleValue(Math.random());
        final DoubleValueSerializer serializer = new DoubleValueSerializer();

        byte[] serialized = InstantiationUtil.serializeToByteArray(serializer, toSerialize);

        DoubleValue deserialized =
                InstantiationUtil.deserializeFromByteArray(serializer, serialized);

        assertThat(deserialized).isEqualTo(toSerialize);
    }

    @Test
    void testCompressionAndSerializationAlongWithDecompressionAndDeserialization()
            throws IOException, ClassNotFoundException {
        final String value = "teststring";

        assertThat(
                        (String)
                                InstantiationUtil.decompressAndDeserializeObject(
                                        InstantiationUtil.serializeObjectAndCompress(value),
                                        getClass().getClassLoader()))
                .isEqualTo(value);
    }

    @Test
    void testWriteToConfigFailingSerialization() throws IOException {
        final String key1 = "testkey1";
        final String key2 = "testkey2";
        final Configuration config = new Configuration();

        assertThatThrownBy(
                        () ->
                                InstantiationUtil.writeObjectToConfig(
                                        new TestClassWriteFails(), config, "irgnored"))
                .isInstanceOf(TestException.class);

        InstantiationUtil.writeObjectToConfig(new TestClassReadFails(), config, key1);
        InstantiationUtil.writeObjectToConfig(new TestClassReadFailsCNF(), config, key2);

        assertThatThrownBy(
                        () ->
                                InstantiationUtil.readObjectFromConfig(
                                        config, key1, getClass().getClassLoader()))
                .isInstanceOf(TestException.class);

        assertThatThrownBy(
                        () ->
                                InstantiationUtil.readObjectFromConfig(
                                        config, key2, getClass().getClassLoader()))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void testCopyWritable() throws Exception {
        WritableType original = new WritableType();
        WritableType copy = InstantiationUtil.createCopyWritable(original);

        assertThat(copy).isNotSameAs(original);
        assertThat(copy).isEqualTo(original);
    }

    // --------------------------------------------------------------------------------------------

    private class TestClass {}

    private static class TestException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    private static class TestClassWriteFails implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private void writeObject(ObjectOutputStream out) throws IOException {
            throw new TestException();
        }
    }

    private static class TestClassReadFails implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            throw new TestException();
        }
    }

    private static class TestClassReadFailsCNF implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            throw new ClassNotFoundException("test exception");
        }
    }

    /** A simple test type. */
    public static final class WritableType implements IOReadableWritable {

        private int aInt;
        private long aLong;

        public WritableType() {
            Random rnd = new Random();
            this.aInt = rnd.nextInt();
            this.aLong = rnd.nextLong();
        }

        @Override
        public int hashCode() {
            return Objects.hash(aInt, aLong);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj != null && obj.getClass() == WritableType.class) {
                WritableType that = (WritableType) obj;
                return this.aLong == that.aLong && this.aInt == that.aInt;
            } else {
                return false;
            }
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeInt(aInt);
            out.writeLong(aLong);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            this.aInt = in.readInt();
            this.aLong = in.readLong();
        }
    }
}
