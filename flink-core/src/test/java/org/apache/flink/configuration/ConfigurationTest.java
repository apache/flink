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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class contains test for the configuration package. In particular, the serialization of
 * {@link Configuration} objects is tested.
 */
@ExtendWith(ParameterizedTestExtension.class)
@SuppressWarnings("deprecation")
class ConfigurationTest {

    @Parameter private boolean standardYaml;

    @Parameters(name = "standardYaml: {0}")
    private static Collection<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    private static final ConfigOption<String> STRING_OPTION =
            ConfigOptions.key("test-string-key").stringType().noDefaultValue();

    private static final ConfigOption<List<String>> LIST_STRING_OPTION =
            ConfigOptions.key("test-list-key").stringType().asList().noDefaultValue();

    private static final ConfigOption<Map<String, String>> MAP_OPTION =
            ConfigOptions.key("test-map-key").mapType().noDefaultValue();

    private static final ConfigOption<Duration> DURATION_OPTION =
            ConfigOptions.key("test-duration-key").durationType().noDefaultValue();

    private static final Map<String, String> PROPERTIES_MAP = new HashMap<>();

    static {
        PROPERTIES_MAP.put("prop1", "value1");
        PROPERTIES_MAP.put("prop2", "12");
    }

    private static final String MAP_PROPERTY_1 = MAP_OPTION.key() + ".prop1";

    private static final String MAP_PROPERTY_2 = MAP_OPTION.key() + ".prop2";

    /** This test checks the serialization/deserialization of configuration objects. */
    @TestTemplate
    void testConfigurationSerializationAndGetters() throws ClassNotFoundException, IOException {
        final Configuration orig = new Configuration(standardYaml);
        orig.setString("mykey", "myvalue");
        orig.setInteger("mynumber", 100);
        orig.setLong("longvalue", 478236947162389746L);
        orig.setFloat("PI", 3.1415926f);
        orig.setDouble("E", Math.E);
        orig.setBoolean("shouldbetrue", true);
        orig.setBytes("bytes sequence", new byte[] {1, 2, 3, 4, 5});
        orig.setClass("myclass", this.getClass());

        final Configuration copy = InstantiationUtil.createCopyWritable(orig);
        assertThat("myvalue").isEqualTo(copy.getString("mykey", "null"));
        assertThat(copy.getInteger("mynumber", 0)).isEqualTo(100);
        assertThat(copy.getLong("longvalue", 0L)).isEqualTo(478236947162389746L);
        assertThat(copy.getFloat("PI", 3.1415926f)).isCloseTo(3.1415926f, Offset.offset(0.0f));
        assertThat(copy.getDouble("E", 0.0)).isCloseTo(Math.E, Offset.offset(0.0));
        assertThat(copy.getBoolean("shouldbetrue", false)).isTrue();
        assertThat(copy.getBytes("bytes sequence", null)).containsExactly(1, 2, 3, 4, 5);
        assertThat(getClass())
                .isEqualTo(copy.getClass("myclass", null, getClass().getClassLoader()));

        assertThat(copy).isEqualTo(orig);
        assertThat(copy.keySet()).isEqualTo(orig.keySet());
        assertThat(copy).hasSameHashCodeAs(orig);
    }

    @TestTemplate
    void testCopyConstructor() {
        final String key = "theKey";

        Configuration cfg1 = new Configuration(standardYaml);
        cfg1.setString(key, "value");

        Configuration cfg2 = new Configuration(cfg1);
        cfg2.setString(key, "another value");

        assertThat(cfg1.getString(key, "")).isEqualTo("value");
    }

    @TestTemplate
    void testOptionWithDefault() {
        Configuration cfg = new Configuration(standardYaml);
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigOptions.key("string-key").stringType().defaultValue("my-beautiful-default");
        ConfigOption<Integer> presentIntOption =
                ConfigOptions.key("int-key").intType().defaultValue(87);

        assertThat(cfg.getString(presentStringOption)).isEqualTo("abc");
        assertThat(cfg.getValue(presentStringOption)).isEqualTo("abc");

        assertThat(cfg.getInteger(presentIntOption)).isEqualTo(11);
        assertThat(cfg.getValue(presentIntOption)).isEqualTo("11");

        // test getting default when no value is present

        ConfigOption<String> stringOption =
                ConfigOptions.key("test").stringType().defaultValue("my-beautiful-default");
        ConfigOption<Integer> intOption = ConfigOptions.key("test2").intType().defaultValue(87);

        // getting strings with default value should work
        assertThat(cfg.getValue(stringOption)).isEqualTo("my-beautiful-default");
        assertThat(cfg.getString(stringOption)).isEqualTo("my-beautiful-default");

        // overriding the default should work
        assertThat(cfg.getString(stringOption, "override")).isEqualTo("override");

        // getting a primitive with a default value should work
        assertThat(cfg.getInteger(intOption)).isEqualTo(87);
        assertThat(cfg.getValue(intOption)).isEqualTo("87");
    }

    @TestTemplate
    void testOptionWithNoDefault() {
        Configuration cfg = new Configuration(standardYaml);
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigOptions.key("string-key").stringType().noDefaultValue();

        assertThat(cfg.getString(presentStringOption)).isEqualTo("abc");
        assertThat(cfg.getValue(presentStringOption)).isEqualTo("abc");

        // test getting default when no value is present

        ConfigOption<String> stringOption = ConfigOptions.key("test").stringType().noDefaultValue();

        // getting strings for null should work
        assertThat(cfg.getValue(stringOption)).isNull();
        assertThat(cfg.getString(stringOption)).isNull();

        // overriding the null default should work
        assertThat(cfg.getString(stringOption, "override")).isEqualTo("override");
    }

    @TestTemplate
    void testDeprecatedKeys() {
        Configuration cfg = new Configuration(standardYaml);
        cfg.setInteger("the-key", 11);
        cfg.setInteger("old-key", 12);
        cfg.setInteger("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigOptions.key("the-key")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigOptions.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigOptions.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigOptions.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys("not-there", "also-not-there");

        assertThat(cfg.getInteger(matchesFirst)).isEqualTo(11);
        assertThat(cfg.getInteger(matchesSecond)).isEqualTo(12);
        assertThat(cfg.getInteger(matchesThird)).isEqualTo(13);
        assertThat(cfg.getInteger(notContained)).isEqualTo(-1);
    }

    @TestTemplate
    void testFallbackKeys() {
        Configuration cfg = new Configuration(standardYaml);
        cfg.setInteger("the-key", 11);
        cfg.setInteger("old-key", 12);
        cfg.setInteger("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigOptions.key("the-key")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigOptions.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigOptions.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigOptions.key("does-not-exist")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys("not-there", "also-not-there");

        assertThat(cfg.getInteger(matchesFirst)).isEqualTo(11);
        assertThat(cfg.getInteger(matchesSecond)).isEqualTo(12);
        assertThat(cfg.getInteger(matchesThird)).isEqualTo(13);
        assertThat(cfg.getInteger(notContained)).isEqualTo(-1);
    }

    @TestTemplate
    void testFallbackAndDeprecatedKeys() {
        final ConfigOption<Integer> fallback =
                ConfigOptions.key("fallback").intType().defaultValue(-1);

        final ConfigOption<Integer> deprecated =
                ConfigOptions.key("deprecated").intType().defaultValue(-1);

        final ConfigOption<Integer> mainOption =
                ConfigOptions.key("main")
                        .intType()
                        .defaultValue(-1)
                        .withFallbackKeys(fallback.key())
                        .withDeprecatedKeys(deprecated.key());

        final Configuration fallbackCfg = new Configuration(standardYaml);
        fallbackCfg.setInteger(fallback, 1);
        assertThat(fallbackCfg.getInteger(mainOption)).isOne();

        final Configuration deprecatedCfg = new Configuration(standardYaml);
        deprecatedCfg.setInteger(deprecated, 2);
        assertThat(deprecatedCfg.getInteger(mainOption)).isEqualTo(2);

        // reverse declaration of fallback and deprecated keys, fallback keys should always be used
        // first
        final ConfigOption<Integer> reversedMainOption =
                ConfigOptions.key("main")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys(deprecated.key())
                        .withFallbackKeys(fallback.key());

        final Configuration deprecatedAndFallBackConfig = new Configuration(standardYaml);
        deprecatedAndFallBackConfig.setInteger(fallback, 1);
        deprecatedAndFallBackConfig.setInteger(deprecated, 2);
        assertThat(deprecatedAndFallBackConfig.getInteger(mainOption)).isOne();
        assertThat(deprecatedAndFallBackConfig.getInteger(reversedMainOption)).isOne();
    }

    @TestTemplate
    void testRemove() {
        Configuration cfg = new Configuration(standardYaml);
        cfg.setInteger("a", 1);
        cfg.setInteger("b", 2);

        ConfigOption<Integer> validOption = ConfigOptions.key("a").intType().defaultValue(-1);

        ConfigOption<Integer> deprecatedOption =
                ConfigOptions.key("c").intType().defaultValue(-1).withDeprecatedKeys("d", "b");

        ConfigOption<Integer> unexistedOption =
                ConfigOptions.key("e").intType().defaultValue(-1).withDeprecatedKeys("f", "g", "j");

        assertThat(cfg.keySet()).hasSize(2).as("Wrong expectation about size");
        assertThat(cfg.removeConfig(validOption)).isTrue().as("Expected 'validOption' is removed");
        assertThat(cfg.keySet()).hasSize(1).as("Wrong expectation about size");
        assertThat(cfg.removeConfig(deprecatedOption))
                .isTrue()
                .as("Expected 'existedOption' is removed");
        assertThat(cfg.keySet()).hasSize(0).as("Wrong expectation about size");
        assertThat(cfg.removeConfig(unexistedOption))
                .isFalse()
                .as("Expected 'unexistedOption' is not removed");
    }

    @TestTemplate
    void testRemoveKey() {
        Configuration cfg = new Configuration(standardYaml);
        String key1 = "a.b";
        String key2 = "c.d";
        cfg.setInteger(key1, 42);
        cfg.setInteger(key2, 44);
        cfg.setInteger(key2 + ".f1", 44);
        cfg.setInteger(key2 + ".f2", 44);
        cfg.setInteger("e.f", 1337);

        assertThat(cfg.removeKey("not-existing-key")).isFalse();
        assertThat(cfg.removeKey(key1)).isTrue();
        assertThat(cfg.containsKey(key1)).isFalse();

        assertThat(cfg.removeKey(key2)).isTrue();
        assertThat(cfg.keySet()).containsExactlyInAnyOrder("e.f");
    }

    @TestTemplate
    void testShouldParseValidStringToEnum() {
        final Configuration configuration = new Configuration(standardYaml);
        configuration.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(TestEnum.VALUE1).isEqualTo(parsedEnumValue);
    }

    @TestTemplate
    void testShouldParseValidStringToEnumIgnoringCase() {
        final Configuration configuration = new Configuration(standardYaml);
        configuration.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString().toLowerCase());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(TestEnum.VALUE1).isEqualTo(parsedEnumValue);
    }

    @TestTemplate
    void testThrowsExceptionIfTryingToParseInvalidStringForEnum() {
        final Configuration configuration = new Configuration(standardYaml);
        final String invalidValueForTestEnum = "InvalidValueForTestEnum";
        configuration.setString(STRING_OPTION.key(), invalidValueForTestEnum);

        assertThatThrownBy(() -> configuration.getEnum(TestEnum.class, STRING_OPTION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Value for config option "
                                + STRING_OPTION.key()
                                + " must be one of [VALUE1, VALUE2] (was "
                                + invalidValueForTestEnum
                                + ")");
    }

    @TestTemplate
    void testToMap() {
        final Configuration configuration = new Configuration(standardYaml);
        final String listValues = "value1;value2;value3";
        final String yamlListValues = "[value1, value2, value3]";
        configuration.set(LIST_STRING_OPTION, Arrays.asList(listValues.split(";")));

        final String mapValues = "key1:value1,key2:value2";
        final String yamlMapValues = "{key1: value1, key2: value2}";
        configuration.set(
                MAP_OPTION,
                Arrays.stream(mapValues.split(","))
                        .collect(Collectors.toMap(e -> e.split(":")[0], e -> e.split(":")[1])));

        final Duration duration = Duration.ofMillis(3000);
        configuration.set(DURATION_OPTION, duration);

        if (standardYaml) {
            assertThat(yamlListValues)
                    .isEqualTo(configuration.toMap().get(LIST_STRING_OPTION.key()));
            assertThat(yamlMapValues).isEqualTo(configuration.toMap().get(MAP_OPTION.key()));
        } else {
            assertThat(listValues).isEqualTo(configuration.toMap().get(LIST_STRING_OPTION.key()));
            assertThat(mapValues).isEqualTo(configuration.toMap().get(MAP_OPTION.key()));
        }
        assertThat("3 s").isEqualTo(configuration.toMap().get(DURATION_OPTION.key()));
    }

    @TestTemplate
    void testToFileWritableMap() {
        final Configuration configuration = new Configuration(standardYaml);
        final String listValues = "value1;value2;value3";
        final String yamlListValues = "[value1, value2, value3]";
        configuration.set(LIST_STRING_OPTION, Arrays.asList(listValues.split(";")));

        final String mapValues = "key1:value1,key2:value2";
        final String yamlMapValues = "{key1: value1, key2: value2}";
        configuration.set(
                MAP_OPTION,
                Arrays.stream(mapValues.split(","))
                        .collect(Collectors.toMap(e -> e.split(":")[0], e -> e.split(":")[1])));

        final Duration duration = Duration.ofMillis(3000);
        configuration.set(DURATION_OPTION, duration);

        final String strValues = "*";
        final String yamlStrValues = "'*'";
        configuration.set(STRING_OPTION, strValues);

        if (standardYaml) {
            assertThat(configuration.toFileWritableMap().get(LIST_STRING_OPTION.key()))
                    .isEqualTo(yamlListValues);
            assertThat(configuration.toFileWritableMap().get(MAP_OPTION.key()))
                    .isEqualTo(yamlMapValues);
            assertThat(configuration.toFileWritableMap().get(STRING_OPTION.key()))
                    .isEqualTo(yamlStrValues);
        } else {
            assertThat(configuration.toFileWritableMap().get(LIST_STRING_OPTION.key()))
                    .isEqualTo(listValues);
            assertThat(configuration.toFileWritableMap().get(MAP_OPTION.key()))
                    .isEqualTo(mapValues);
            assertThat(configuration.toFileWritableMap().get(STRING_OPTION.key()))
                    .isEqualTo(strValues);
        }
        assertThat(configuration.toMap().get(DURATION_OPTION.key())).isEqualTo("3 s");
    }

    @TestTemplate
    void testMapNotContained() {
        final Configuration cfg = new Configuration(standardYaml);

        assertThat(cfg.getOptional(MAP_OPTION)).isNotPresent();
        assertThat(cfg.contains(MAP_OPTION)).isFalse();
    }

    @TestTemplate
    void testMapWithPrefix() {
        final Configuration cfg = new Configuration(standardYaml);
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 12);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @TestTemplate
    void testMapWithoutPrefix() {
        final Configuration cfg = new Configuration(standardYaml);
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @TestTemplate
    void testMapNonPrefixHasPrecedence() {
        final Configuration cfg = new Configuration(standardYaml);
        cfg.set(MAP_OPTION, PROPERTIES_MAP);
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 99999);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isTrue();
    }

    @TestTemplate
    void testMapThatOverwritesPrefix() {
        final Configuration cfg = new Configuration(standardYaml);
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 99999);
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isFalse();
    }

    @TestTemplate
    void testMapRemovePrefix() {
        final Configuration cfg = new Configuration(standardYaml);
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.setInteger(MAP_PROPERTY_2, 99999);
        cfg.removeConfig(MAP_OPTION);

        assertThat(cfg.contains(MAP_OPTION)).isFalse();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isFalse();
        assertThat(cfg.containsKey(MAP_PROPERTY_2)).isFalse();
    }

    @TestTemplate
    void testListParserErrorDoesNotLeakSensitiveData() {
        ConfigOption<List<String>> secret =
                ConfigOptions.key("secret").stringType().asList().noDefaultValue();

        assertThat(GlobalConfiguration.isSensitive(secret.key())).isTrue();

        final Configuration cfg = new Configuration(standardYaml);
        // missing closing quote
        cfg.setString(secret.key(), "'secret_value");

        assertThatThrownBy(() -> cfg.get(secret))
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(
                        e ->
                                assertThat(ExceptionUtils.stringifyException(e))
                                        .doesNotContain("secret_value"));
    }

    @TestTemplate
    void testMapParserErrorDoesNotLeakSensitiveData() {
        ConfigOption<Map<String, String>> secret =
                ConfigOptions.key("secret").mapType().noDefaultValue();

        assertThat(GlobalConfiguration.isSensitive(secret.key())).isTrue();

        final Configuration cfg = new Configuration(standardYaml);
        // malformed map representation
        cfg.setString(secret.key(), "secret_value");

        assertThatThrownBy(() -> cfg.get(secret))
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(
                        e ->
                                assertThat(ExceptionUtils.stringifyException(e))
                                        .doesNotContain("secret_value"));
    }

    @TestTemplate
    void testToStringDoesNotLeakSensitiveData() {
        ConfigOption<Map<String, String>> secret =
                ConfigOptions.key("secret").mapType().noDefaultValue();

        assertThat(GlobalConfiguration.isSensitive(secret.key())).isTrue();

        final Configuration cfg = new Configuration(standardYaml);
        cfg.setString(secret.key(), "secret_value");

        assertThat(cfg.toString()).doesNotContain("secret_value");
    }

    @TestTemplate
    void testGetWithOverrideDefault() {
        final Configuration conf = new Configuration(standardYaml);

        // Test for integer without default value.
        ConfigOption<Integer> integerOption0 =
                ConfigOptions.key("integer.key0").intType().noDefaultValue();
        // integerOption0 doesn't exist in conf, and it should be overrideDefault.
        assertThat(conf.get(integerOption0, 2)).isEqualTo(2);
        // integerOption0 exists in conf, and it should be value that set before.
        conf.set(integerOption0, 3);
        assertThat(conf.get(integerOption0, 2)).isEqualTo(3);

        // Test for integer with default value, the default value should be ignored.
        ConfigOption<Integer> integerOption1 =
                ConfigOptions.key("integer.key1").intType().defaultValue(4);
        assertThat(conf.get(integerOption1, 5)).isEqualTo(5);
        // integerOption1 is changed.
        conf.set(integerOption1, 6);
        assertThat(conf.get(integerOption1, 5)).isEqualTo(6);

        // Test for string without default value.
        ConfigOption<String> stringOption0 =
                ConfigOptions.key("string.key0").stringType().noDefaultValue();
        // stringOption0 doesn't exist in conf, and it should be overrideDefault.
        assertThat(conf.get(stringOption0, "a")).isEqualTo("a");
        // stringOption0 exists in conf, and it should be value that set before.
        conf.set(stringOption0, "b");
        assertThat(conf.get(stringOption0, "a")).isEqualTo("b");

        // Test for string with default value, the default value should be ignored.
        ConfigOption<String> stringOption1 =
                ConfigOptions.key("string.key1").stringType().defaultValue("c");
        assertThat(conf.get(stringOption1, "d")).isEqualTo("d");
        // stringOption1 is changed.
        conf.set(stringOption1, "e");
        assertThat(conf.get(stringOption1, "d")).isEqualTo("e");
    }

    // --------------------------------------------------------------------------------------------
    // Test classes
    // --------------------------------------------------------------------------------------------

    enum TestEnum {
        VALUE1,
        VALUE2
    }
}
