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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

/** Base class for tests for {@link ParameterTool}. */
public abstract class AbstractParameterToolTest {

    @TempDir Path tempPath;

    // Test parser

    @Test
    void testThrowExceptionIfParameterIsNotPrefixed() {

        assertThatThrownBy(() -> createParameterToolFromArgs(new String[] {"a"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Error parsing arguments '[a]' on 'a'. Please prefix keys with -- or -.");
    }

    @Test
    void testNoVal() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-berlin"});
        assertThat(parameter.getNumberOfParameters()).isOne();
        assertThat(parameter.has("berlin")).isTrue();
    }

    @Test
    void testNoValDouble() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"--berlin"});
        assertThat(parameter.getNumberOfParameters()).isOne();
        assertThat(parameter.has("berlin")).isTrue();
    }

    @Test
    void testMultipleNoVal() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(
                        new String[] {"--a", "--b", "--c", "--d", "--e", "--f"});
        assertThat(parameter.getNumberOfParameters()).isEqualTo(6);
        assertThat(parameter.has("a")).isTrue();
        assertThat(parameter.has("b")).isTrue();
        assertThat(parameter.has("c")).isTrue();
        assertThat(parameter.has("d")).isTrue();
        assertThat(parameter.has("e")).isTrue();
        assertThat(parameter.has("f")).isTrue();
    }

    @Test
    void testMultipleNoValMixed() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"--a", "-b", "-c", "-d", "--e", "--f"});
        assertThat(parameter.getNumberOfParameters()).isEqualTo(6);
        assertThat(parameter.has("a")).isTrue();
        assertThat(parameter.has("b")).isTrue();
        assertThat(parameter.has("c")).isTrue();
        assertThat(parameter.has("d")).isTrue();
        assertThat(parameter.has("e")).isTrue();
        assertThat(parameter.has("f")).isTrue();
    }

    @Test
    void testEmptyVal() {

        assertThatThrownBy(() -> createParameterToolFromArgs(new String[] {"--a", "-b", "--"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The input [--a, -b, --] contains an empty argument");
    }

    @Test
    void testEmptyValShort() {

        assertThatThrownBy(() -> createParameterToolFromArgs(new String[] {"--a", "-b", "-"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The input [--a, -b, -] contains an empty argument");
    }

    // Test unrequested
    // Boolean

    @Test
    void testUnrequestedBoolean() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-boolean", "true"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        // test parameter access
        assertThat(parameter.getBoolean("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getBoolean("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedBooleanWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-boolean", "true"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        // test parameter access
        assertThat(parameter.getBoolean("boolean", false)).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getBoolean("boolean", false)).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedBooleanWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-boolean"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        parameter.getBoolean("boolean");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    // Byte

    @Test
    void testUnrequestedByte() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte", "1"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("byte");

        // test parameter access
        assertThat(parameter.getByte("byte")).isOne();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getByte("byte")).isOne();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedByteWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte", "1"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("byte");

        // test parameter access
        assertThat(parameter.getByte("byte", (byte) 0)).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getByte("byte", (byte) 0)).isEqualTo((byte) 1);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedByteWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("byte");

        assertThatThrownBy(() -> parameter.getByte("byte"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("For input string: \"__NO_VALUE_KEY\"");
    }

    // Short

    @Test
    void testUnrequestedShort() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short", "2"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("short");

        // test parameter access
        assertThat(parameter.getShort("short")).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getShort("short")).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedShortWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short", "2"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("short");

        // test parameter access
        assertThat(parameter.getShort("short", (short) 0)).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getShort("short", (short) 0)).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedShortWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("short");

        assertThatThrownBy(() -> parameter.getShort("short"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("For input string: \"__NO_VALUE_KEY\"");
    }

    // Int

    @Test
    void testUnrequestedInt() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("int");

        // test parameter access
        assertThat(parameter.getInt("int")).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getInt("int")).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedIntWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("int");

        // test parameter access
        assertThat(parameter.getInt("int", 0)).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getInt("int", 0)).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedIntWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("int");

        assertThatThrownBy(() -> parameter.getInt("int"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("For input string: \"__NO_VALUE_KEY\"");
    }

    // Long

    @Test
    void testUnrequestedLong() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("long");

        // test parameter access
        assertThat(parameter.getLong("long")).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getLong("long")).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedLongWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("long");

        // test parameter access
        assertThat(parameter.getLong("long", 0)).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getLong("long", 0)).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedLongWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("long");

        assertThatThrownBy(() -> parameter.getLong("long"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("For input string: \"__NO_VALUE_KEY\"");
    }

    // Float

    @Test
    void testUnrequestedFloat() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("float");

        // test parameter access
        assertThat(parameter.getFloat("float")).isCloseTo(4.0f, offset(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getFloat("float")).isCloseTo(4.0f, offset(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedFloatWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float", "4"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("float");

        // test parameter access
        assertThat(parameter.getFloat("float", 0.0f)).isCloseTo(4.0f, offset(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getFloat("float", 0.0f)).isCloseTo(4.0f, offset(0.00001f));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedFloatWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("float");

        assertThatThrownBy(() -> parameter.getFloat("float"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("For input string: \"__NO_VALUE_KEY\"");
    }

    // Double

    @Test
    void testUnrequestedDouble() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-double", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("double");

        // test parameter access
        assertThat(parameter.getDouble("double")).isCloseTo(8.0, offset(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getDouble("double")).isCloseTo(8.0, offset(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedDoubleWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-double", "8"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("double");

        // test parameter access
        assertThat(parameter.getDouble("double", 0.0)).isCloseTo(8.0, offset(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getDouble("double", 0.0)).isCloseTo(8.0, offset(0.00001));
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedDoubleWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-double"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("double");

        assertThatThrownBy(() -> parameter.getDouble("double"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("For input string: \"__NO_VALUE_KEY\"");
    }

    // String

    @Test
    void testUnrequestedString() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-string", "∞"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        // test parameter access
        assertThat(parameter.get("string")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.get("string")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedStringWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-string", "∞"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        // test parameter access
        assertThat(parameter.get("string", "0.0")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.get("string", "0.0")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedStringWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-string"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        parameter.get("string");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    // Additional methods

    @Test
    void testUnrequestedHas() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-boolean"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("boolean");

        // test parameter access
        assertThat(parameter.has("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.has("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedRequired() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-required", "∞"});
        assertThat(parameter.getUnrequestedParameters()).containsExactly("required");

        // test parameter access
        assertThat(parameter.getRequired("required")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        // test repeated access
        assertThat(parameter.getRequired("required")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedMultiple() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(
                        new String[] {
                            "-boolean",
                            "true",
                            "-byte",
                            "1",
                            "-short",
                            "2",
                            "-int",
                            "4",
                            "-long",
                            "8",
                            "-float",
                            "4.0",
                            "-double",
                            "8.0",
                            "-string",
                            "∞"
                        });
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder(
                        "boolean", "byte", "short", "int", "long", "float", "double", "string");

        assertThat(parameter.getBoolean("boolean")).isTrue();
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder(
                        "byte", "short", "int", "long", "float", "double", "string");

        assertThat(parameter.getByte("byte")).isOne();
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("short", "int", "long", "float", "double", "string");

        assertThat(parameter.getShort("short")).isEqualTo((short) 2);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("int", "long", "float", "double", "string");

        assertThat(parameter.getInt("int")).isEqualTo(4);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("long", "float", "double", "string");

        assertThat(parameter.getLong("long")).isEqualTo(8);
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("float", "double", "string");

        assertThat(parameter.getFloat("float")).isCloseTo(4.0f, offset(0.00001f));
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("double", "string");

        assertThat(parameter.getDouble("double")).isCloseTo(8.0, offset(0.00001));
        assertThat(parameter.getUnrequestedParameters()).containsExactly("string");

        assertThat(parameter.get("string")).isEqualTo("∞");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testUnrequestedUnknown() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {});
        assertThat(parameter.getUnrequestedParameters()).isEmpty();

        assertThat(parameter.getBoolean("boolean", true)).isTrue();
        assertThat(parameter.getByte("byte", (byte) 0)).isZero();
        assertThat(parameter.getShort("short", (short) 0)).isZero();
        assertThat(parameter.getInt("int", 0)).isZero();
        assertThat(parameter.getLong("long", 0)).isZero();
        assertThat(parameter.getFloat("float", 0)).isCloseTo(0f, offset(0.00001f));
        assertThat(parameter.getDouble("double", 0)).isCloseTo(0, offset(0.00001));
        assertThat(parameter.get("string", "0")).isEqualTo("0");

        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    protected AbstractParameterTool createParameterToolFromArgs(String[] args) {
        return ParameterTool.fromArgs(args);
    }

    protected static <T> Set<T> createHashSet(T... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }

    protected void validate(AbstractParameterTool parameter) {
        ClosureCleaner.ensureSerializable(parameter);
        internalValidate(parameter);

        // -------- test behaviour after serialization ------------
        try {
            byte[] b = InstantiationUtil.serializeObject(parameter);
            final AbstractParameterTool copy =
                    InstantiationUtil.deserializeObject(b, getClass().getClassLoader());
            internalValidate(copy);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void internalValidate(AbstractParameterTool parameter) {
        assertThat(parameter.getRequired("input")).isEqualTo("myInput");
        assertThat(parameter.get("output", "myDefaultValue")).isEqualTo("myDefaultValue");
        assertThat(parameter.get("whatever")).isNull();
        assertThat(parameter.getLong("expectedCount", -1L)).isEqualTo(15L);
        assertThat(parameter.getBoolean("thisIsUseful", true)).isTrue();
        assertThat(parameter.getByte("myDefaultByte", (byte) 42)).isEqualTo((byte) 42);
        assertThat(parameter.getShort("myDefaultShort", (short) 42)).isEqualTo((short) 42);

        if (parameter instanceof ParameterTool) {
            ParameterTool parameterTool = (ParameterTool) parameter;
            final Configuration config = parameterTool.getConfiguration();
            assertThat(
                            config.getLong(
                                    ConfigOptions.key("expectedCount")
                                            .longType()
                                            .defaultValue(-1L)))
                    .isEqualTo(15L);

            final Properties props = parameterTool.getProperties();
            assertThat(props.getProperty("input")).isEqualTo("myInput");

            // -------- test the default file creation ------------
            try {
                final String pathToFile = tempPath.toFile().getAbsolutePath();
                parameterTool.createPropertiesFile(pathToFile);
                final Properties defaultProps = new Properties();
                try (FileInputStream fis = new FileInputStream(pathToFile)) {
                    defaultProps.load(fis);
                }

                assertThat(defaultProps)
                        .containsEntry("output", "myDefaultValue")
                        .containsEntry("expectedCount", "-1")
                        .containsKey("input");

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (parameter instanceof MultipleParameterTool) {
            MultipleParameterTool multipleParameterTool = (MultipleParameterTool) parameter;
            List<String> multiValues = Arrays.asList("multiValue1", "multiValue2");
            assertThat(multipleParameterTool.getMultiParameter("multi")).isEqualTo(multiValues);

            assertThat(multipleParameterTool.toMultiMap()).containsEntry("multi", multiValues);

            // The last value is used.
            assertThat(multipleParameterTool.toMap()).containsEntry("multi", "multiValue2");
        }
    }
}
