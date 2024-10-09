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

package org.apache.flink.configuration;

import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getDoubleConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getFloatConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getIntConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getLongConfigOption;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link Configuration} conversion between types. Extracted from {@link
 * ConfigurationTest}.
 */
@SuppressWarnings("deprecation")
@ExtendWith(ParameterizedTestExtension.class)
class ConfigurationConversionsTest {

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final long TOO_LONG = Integer.MAX_VALUE + 10L;
    private static final double TOO_LONG_DOUBLE = Double.MAX_VALUE;

    private Configuration pc;

    @Parameter private TestSpec testSpec;

    @BeforeEach
    void init() {
        pc = new Configuration();

        pc.set(getIntConfigOption("int"), 5);
        pc.set(getLongConfigOption("long"), 15L);
        pc.set(getLongConfigOption("too_long"), TOO_LONG);
        pc.set(getFloatConfigOption("float"), 2.1456775f);
        pc.set(getDoubleConfigOption("double"), Math.PI);
        pc.set(getDoubleConfigOption("negative_double"), -1.0);
        pc.set(getDoubleConfigOption("zero"), 0.0);
        pc.set(getDoubleConfigOption("too_long_double"), TOO_LONG_DOUBLE);
        pc.setString("string", "42");
        pc.setString("non_convertible_string", "bcdefg&&");
        pc.set(getBooleanConfigOption("boolean"), true);
    }

    @Parameters(name = "testSpec={0}")
    private static Collection<TestSpec> getSpecs() {
        return Arrays.asList(
                // from integer
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("int"), 0)).expect(5),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("int"), 0L)).expect(5L),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("int"), 0f)).expect(5f),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("int"), 0.0))
                        .expect(5.0),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("int"), true))
                        .expectException("Could not parse value '5' for key 'int'."),
                TestSpec.whenAccessed(conf -> conf.getString("int", "0")).expect("5"),
                TestSpec.whenAccessed(conf -> conf.getBytes("int", EMPTY_BYTES))
                        .expectException("Configuration cannot evaluate value 5 as a byte[] value"),

                // from long
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("long"), 0)).expect(15),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("long"), 0L))
                        .expect(15L),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("long"), 0f))
                        .expect(15f),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("long"), 0.0))
                        .expect(15.0),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("long"), true))
                        .expectException("Could not parse value '15' for key 'long'."),
                TestSpec.whenAccessed(conf -> conf.getString("long", "0")).expect("15"),
                TestSpec.whenAccessed(conf -> conf.getBytes("long", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 15 as a byte[] value"),

                // from too long
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("too_long"), 0))
                        .expectException("Could not parse value '2147483657' for key 'too_long'."),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("too_long"), 0L))
                        .expect(TOO_LONG),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("too_long"), 0f))
                        .expect((float) TOO_LONG),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("too_long"), 0.0))
                        .expect((double) TOO_LONG),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("too_long"), true))
                        .expectException("Could not parse value '2147483657' for key 'too_long'."),
                TestSpec.whenAccessed(conf -> conf.getString("too_long", "0"))
                        .expect(String.valueOf(TOO_LONG)),
                TestSpec.whenAccessed(conf -> conf.getBytes("too_long", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 2147483657 as a byte[] value"),

                // from float
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("float"), 0))
                        .expectException(
                                "Could not parse value '2.1456776' for key 'float'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("float"), 0L))
                        .expectException(
                                "Could not parse value '2.1456776' for key 'float'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("float"), 0f))
                        .expect(2.1456775f),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("float"), 0.0))
                        .expect(
                                new Condition<>(
                                        d -> Math.abs(d - 2.1456775) < 0.0000001,
                                        "Expected value")),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("float"), true))
                        .expectException("Could not parse value '2.1456776' for key 'float'."),
                TestSpec.whenAccessed(conf -> conf.getString("float", "0"))
                        .expect(new Condition<>(s -> s.startsWith("2.145677"), "Expected value")),
                TestSpec.whenAccessed(conf -> conf.getBytes("float", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 2.1456776 as a byte[] value"),

                // from double
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("double"), 0))
                        .expectException(
                                "Could not parse value '3.141592653589793' for key 'double'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("double"), 0L))
                        .expectException(
                                "Could not parse value '3.141592653589793' for key 'double'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("double"), 0f))
                        .expect(new IsCloseTo(3.141592f, 0.000001f)),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("double"), 0.0))
                        .expect(Math.PI),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("double"), true))
                        .expectException(
                                "Could not parse value '3.141592653589793' for key 'double'."),
                TestSpec.whenAccessed(conf -> conf.getString("double", "0"))
                        .expect(new Condition<>(s -> s.startsWith("3.141592"), "Expected value")),
                TestSpec.whenAccessed(conf -> conf.getBytes("double", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 3.141592653589793 as a byte[] value"),

                // from negative double
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("negative_double"), 0))
                        .expectException(
                                "Could not parse value '-1.0' for key 'negative_double'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("negative_double"), 0L))
                        .expectException(
                                "Could not parse value '-1.0' for key 'negative_double'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("negative_double"), 0f))
                        .expect(new IsCloseTo(-1f, 0.000001f)),
                TestSpec.whenAccessed(
                                conf -> conf.get(getDoubleConfigOption("negative_double"), 0.0))
                        .expect(-1D),
                TestSpec.whenAccessed(
                                conf -> conf.get(getBooleanConfigOption("negative_double"), true))
                        .expectException("Could not parse value '-1.0' for key 'negative_double'."),
                TestSpec.whenAccessed(conf -> conf.getString("negative_double", "0"))
                        .expect(new Condition<>(s -> s.startsWith("-1.0"), "Expected value")),
                TestSpec.whenAccessed(conf -> conf.getBytes("negative_double", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value -1.0 as a byte[] value"),

                // from zero
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("zero"), 0))
                        .expectException(
                                "Could not parse value '0.0' for key 'zero'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("zero"), 0L))
                        .expectException(
                                "Could not parse value '0.0' for key 'zero'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("zero"), 0f))
                        .expect(new IsCloseTo(0f, 0.000001f)),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("zero"), 0.0))
                        .expect(0D),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("zero"), true))
                        .expectException("Could not parse value '0.0' for key 'zero'."),
                TestSpec.whenAccessed(conf -> conf.getString("zero", "0"))
                        .expect(new Condition<>(s -> s.startsWith("0"), "Expected value")),
                TestSpec.whenAccessed(conf -> conf.getBytes("zero", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 0.0 as a byte[] value"),

                // from too long double
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("too_long_double"), 0))
                        .expectException(
                                "Could not parse value '1.7976931348623157E308' for key 'too_long_double'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("too_long_double"), 0L))
                        .expectException(
                                "Could not parse value '1.7976931348623157E308' for key 'too_long_double'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("too_long_double"), 0f))
                        .expectException(
                                "Could not parse value '1.7976931348623157E308' for key 'too_long_double'."),
                TestSpec.whenAccessed(
                                conf -> conf.get(getDoubleConfigOption("too_long_double"), 0.0))
                        .expect(TOO_LONG_DOUBLE),
                TestSpec.whenAccessed(
                                conf -> conf.get(getBooleanConfigOption("too_long_double"), true))
                        .expectException(
                                "Could not parse value '1.7976931348623157E308' for key 'too_long_double'."),
                TestSpec.whenAccessed(conf -> conf.getString("too_long_double", "0"))
                        .expect(String.valueOf(TOO_LONG_DOUBLE)),
                TestSpec.whenAccessed(conf -> conf.getBytes("too_long_double", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 1.7976931348623157E308 as a byte[] value"),

                // from string
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("string"), 0)).expect(42),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("string"), 0L))
                        .expect(42L),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("string"), 0f))
                        .expect(42f),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("string"), 0.0))
                        .expect(42.0),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("string"), true))
                        .expectException("Could not parse value '42' for key 'string'."),
                TestSpec.whenAccessed(conf -> conf.getString("string", "0")).expect("42"),
                TestSpec.whenAccessed(conf -> conf.getBytes("string", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value 42 as a byte[] value"),

                // from non convertible string
                TestSpec.whenAccessed(
                                conf -> conf.get(getIntConfigOption("non_convertible_string"), 0))
                        .expectException(
                                "Could not parse value 'bcdefg&&' for key 'non_convertible_string'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(
                                conf -> conf.get(getLongConfigOption("non_convertible_string"), 0L))
                        .expectException(
                                "Could not parse value 'bcdefg&&' for key 'non_convertible_string'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(
                                conf ->
                                        conf.get(
                                                getFloatConfigOption("non_convertible_string"), 0f))
                        .expectException(
                                "Could not parse value 'bcdefg&&' for key 'non_convertible_string'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(
                                conf ->
                                        conf.get(
                                                getDoubleConfigOption("non_convertible_string"),
                                                0.0))
                        .expectException(
                                "Could not parse value 'bcdefg&&' for key 'non_convertible_string'.",
                                IllegalArgumentException.class),
                TestSpec.whenAccessed(
                                conf ->
                                        conf.get(
                                                getBooleanConfigOption("non_convertible_string"),
                                                true))
                        .expectException(
                                "Could not parse value 'bcdefg&&' for key 'non_convertible_string'."),
                TestSpec.whenAccessed(conf -> conf.getString("non_convertible_string", "0"))
                        .expect("bcdefg&&"),
                TestSpec.whenAccessed(conf -> conf.getBytes("non_convertible_string", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value bcdefg&& as a byte[] value"),

                // from boolean
                TestSpec.whenAccessed(conf -> conf.get(getIntConfigOption("boolean"), 0))
                        .expectException("Could not parse value 'true' for key 'boolean'."),
                TestSpec.whenAccessed(conf -> conf.get(getLongConfigOption("boolean"), 0L))
                        .expectException("Could not parse value 'true' for key 'boolean'."),
                TestSpec.whenAccessed(conf -> conf.get(getFloatConfigOption("boolean"), 0f))
                        .expectException("Could not parse value 'true' for key 'boolean'."),
                TestSpec.whenAccessed(conf -> conf.get(getDoubleConfigOption("boolean"), 0.0))
                        .expectException("Could not parse value 'true' for key 'boolean'."),
                TestSpec.whenAccessed(conf -> conf.get(getBooleanConfigOption("boolean"), false))
                        .expect(true),
                TestSpec.whenAccessed(conf -> conf.getString("boolean", "0")).expect("true"),
                TestSpec.whenAccessed(conf -> conf.getBytes("boolean", EMPTY_BYTES))
                        .expectException(
                                "Configuration cannot evaluate value true as a byte[] value"));
    }

    @TestTemplate
    void testConversions() throws Exception {

        Optional<String> expectedException = testSpec.getExpectedException();

        if (expectedException.isPresent()) {
            assertThatThrownBy(() -> testSpec.assertConfiguration(pc))
                    .isInstanceOf(testSpec.getExceptionClass())
                    .hasMessageContaining(expectedException.get());
            return;
        }

        // workaround for type erasure
        testSpec.assertConfiguration(pc);
    }

    private static class IsCloseTo extends Condition<Float> {
        private final float delta;
        private final float value;

        public IsCloseTo(float value, float error) {
            this.delta = error;
            this.value = value;
        }

        @Override
        public boolean matches(Float item) {
            return this.actualDelta(item) <= 0.0D;
        }

        private double actualDelta(Float item) {
            return Math.abs(item - this.value) - this.delta;
        }
    }

    private static class TestSpec<T> {
        private final ConfigurationAccessor<T> configurationAccessor;
        private Condition<T> condition;
        @Nullable private String expectedException = null;
        @Nullable private Class<? extends Exception> exceptionClass;

        @FunctionalInterface
        private interface ConfigurationAccessor<T> {
            T access(Configuration configuration) throws Exception;
        }

        private TestSpec(ConfigurationAccessor<T> configurationAccessor) {
            this.configurationAccessor = configurationAccessor;
        }

        public static <T> TestSpec<T> whenAccessed(ConfigurationAccessor<T> configurationAccessor) {
            return new TestSpec<>(configurationAccessor);
        }

        public TestSpec<T> expect(Condition<T> expected) {
            this.condition = expected;
            return this;
        }

        public TestSpec<T> expect(T expected) {
            this.condition =
                    new Condition<>(value -> Objects.equals(value, expected), "Expected value");
            return this;
        }

        public TestSpec<T> expectException(String message) {
            this.expectedException = message;
            this.exceptionClass = IllegalArgumentException.class;
            return this;
        }

        public TestSpec<T> expectException(
                String message, Class<? extends Exception> exceptionClass) {
            this.expectedException = message;
            this.exceptionClass = exceptionClass;
            return this;
        }

        public Optional<String> getExpectedException() {
            return Optional.ofNullable(expectedException);
        }

        @Nullable
        public Class<? extends Exception> getExceptionClass() {
            return exceptionClass;
        }

        void assertConfiguration(Configuration conf) throws Exception {
            assertThat(configurationAccessor.access(conf)).is(condition);
        }

        @Override
        public String toString() {
            return String.format("accessor = %s, expected = %s", configurationAccessor, condition);
        }
    }
}
