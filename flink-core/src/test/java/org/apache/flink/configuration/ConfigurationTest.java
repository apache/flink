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

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigurationUtils.getBooleanConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getDoubleConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getFloatConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getIntConfigOption;
import static org.apache.flink.configuration.ConfigurationUtils.getLongConfigOption;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class contains test for the configuration package. In particular, the serialization of
 * {@link Configuration} objects is tested.
 */
@SuppressWarnings("deprecation")
class ConfigurationTest {

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
    @Test
    void testConfigurationSerializationAndGetters() throws ClassNotFoundException, IOException {
        final Configuration orig = new Configuration();
        orig.setString("mykey", "myvalue");
        orig.set(getIntConfigOption("mynumber"), 100);
        orig.set(getLongConfigOption("longvalue"), 478236947162389746L);
        orig.set(getFloatConfigOption("PI"), 3.1415926f);
        orig.set(getDoubleConfigOption("E"), Math.E);
        orig.set(getBooleanConfigOption("shouldbetrue"), true);
        orig.setBytes("bytes sequence", new byte[] {1, 2, 3, 4, 5});

        final Configuration copy = InstantiationUtil.createCopyWritable(orig);
        assertThat("myvalue").isEqualTo(copy.getString("mykey", "null"));
        assertThat(copy.get(getIntConfigOption("mynumber"), 0)).isEqualTo(100);
        assertThat(copy.get(getLongConfigOption("longvalue"), 0L)).isEqualTo(478236947162389746L);
        assertThat(copy.get(getFloatConfigOption("PI"), 3.1415926f))
                .isCloseTo(3.1415926f, Offset.offset(0.0f));
        assertThat(copy.get(getDoubleConfigOption("E"), 0.0)).isCloseTo(Math.E, Offset.offset(0.0));
        assertThat(copy.get(getBooleanConfigOption("shouldbetrue"), false)).isTrue();
        assertThat(copy.getBytes("bytes sequence", null)).containsExactly(1, 2, 3, 4, 5);

        assertThat(copy).isEqualTo(orig);
        assertThat(copy.keySet()).isEqualTo(orig.keySet());
        assertThat(copy).hasSameHashCodeAs(orig);
    }

    @Test
    void testCopyConstructor() {
        final String key = "theKey";

        Configuration cfg1 = new Configuration();
        cfg1.setString(key, "value");

        Configuration cfg2 = new Configuration(cfg1);
        cfg2.setString(key, "another value");

        assertThat(cfg1.getString(key, "")).isEqualTo("value");
    }

    @Test
    void testOptionWithDefault() {
        Configuration cfg = new Configuration();
        cfg.set(getIntConfigOption("int-key"), 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigOptions.key("string-key").stringType().defaultValue("my-beautiful-default");
        ConfigOption<Integer> presentIntOption =
                ConfigOptions.key("int-key").intType().defaultValue(87);

        assertThat(cfg.get(presentStringOption)).isEqualTo("abc");
        assertThat(cfg.getValue(presentStringOption)).isEqualTo("abc");

        assertThat(cfg.get(presentIntOption)).isEqualTo(11);
        assertThat(cfg.getValue(presentIntOption)).isEqualTo("11");

        // test getting default when no value is present

        ConfigOption<String> stringOption =
                ConfigOptions.key("test").stringType().defaultValue("my-beautiful-default");
        ConfigOption<Integer> intOption = ConfigOptions.key("test2").intType().defaultValue(87);

        // getting strings with default value should work
        assertThat(cfg.getValue(stringOption)).isEqualTo("my-beautiful-default");
        assertThat(cfg.get(stringOption)).isEqualTo("my-beautiful-default");

        // overriding the default should work
        assertThat(cfg.get(stringOption, "override")).isEqualTo("override");

        // getting a primitive with a default value should work
        assertThat(cfg.get(intOption)).isEqualTo(87);
        assertThat(cfg.getValue(intOption)).isEqualTo("87");
    }

    @Test
    void testOptionWithNoDefault() {
        Configuration cfg = new Configuration();
        cfg.get(getIntConfigOption("int-key"), 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigOptions.key("string-key").stringType().noDefaultValue();

        assertThat(cfg.get(presentStringOption)).isEqualTo("abc");
        assertThat(cfg.getValue(presentStringOption)).isEqualTo("abc");

        // test getting default when no value is present

        ConfigOption<String> stringOption = ConfigOptions.key("test").stringType().noDefaultValue();

        // getting strings for null should work
        assertThat(cfg.getValue(stringOption)).isNull();
        assertThat(cfg.get(stringOption)).isNull();

        // overriding the null default should work
        assertThat(cfg.get(stringOption, "override")).isEqualTo("override");
    }

    @Test
    void testDeprecatedKeys() {
        Configuration cfg = new Configuration();
        cfg.set(getIntConfigOption("the-key"), 11);
        cfg.set(getIntConfigOption("old-key"), 12);
        cfg.set(getIntConfigOption("older-key"), 13);

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

        assertThat(cfg.get(matchesFirst)).isEqualTo(11);
        assertThat(cfg.get(matchesSecond)).isEqualTo(12);
        assertThat(cfg.get(matchesThird)).isEqualTo(13);
        assertThat(cfg.get(notContained)).isEqualTo(-1);
    }

    @Test
    void testFallbackKeys() {
        Configuration cfg = new Configuration();
        cfg.set(getIntConfigOption("the-key"), 11);
        cfg.set(getIntConfigOption("old-key"), 12);
        cfg.set(getIntConfigOption("older-key"), 13);

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

        assertThat(cfg.get(matchesFirst)).isEqualTo(11);
        assertThat(cfg.get(matchesSecond)).isEqualTo(12);
        assertThat(cfg.get(matchesThird)).isEqualTo(13);
        assertThat(cfg.get(notContained)).isEqualTo(-1);
    }

    @Test
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

        final Configuration fallbackCfg = new Configuration();
        fallbackCfg.set(fallback, 1);
        assertThat(fallbackCfg.get(mainOption)).isOne();

        final Configuration deprecatedCfg = new Configuration();
        deprecatedCfg.set(deprecated, 2);
        assertThat(deprecatedCfg.get(mainOption)).isEqualTo(2);

        // reverse declaration of fallback and deprecated keys, fallback keys should always be used
        // first
        final ConfigOption<Integer> reversedMainOption =
                ConfigOptions.key("main")
                        .intType()
                        .defaultValue(-1)
                        .withDeprecatedKeys(deprecated.key())
                        .withFallbackKeys(fallback.key());

        final Configuration deprecatedAndFallBackConfig = new Configuration();
        deprecatedAndFallBackConfig.set(fallback, 1);
        deprecatedAndFallBackConfig.set(deprecated, 2);
        assertThat(deprecatedAndFallBackConfig.get(mainOption)).isOne();
        assertThat(deprecatedAndFallBackConfig.get(reversedMainOption)).isOne();
    }

    @Test
    void testRemove() {
        Configuration cfg = new Configuration();
        cfg.set(getIntConfigOption("a"), 1);
        cfg.set(getIntConfigOption("b"), 2);

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

    @Test
    void testRemoveKey() {
        Configuration cfg = new Configuration();
        String key1 = "a.b";
        String key2 = "c.d";
        cfg.set(getIntConfigOption(key1), 42);
        cfg.set(getIntConfigOption(key2), 44);
        cfg.set(getIntConfigOption(key2 + ".f1"), 44);
        cfg.set(getIntConfigOption(key2 + ".f2"), 44);
        cfg.set(getIntConfigOption("e.f"), 1337);

        assertThat(cfg.removeKey("not-existing-key")).isFalse();
        assertThat(cfg.removeKey(key1)).isTrue();
        assertThat(cfg.containsKey(key1)).isFalse();

        assertThat(cfg.removeKey(key2)).isTrue();
        assertThat(cfg.keySet()).containsExactlyInAnyOrder("e.f");
    }

    @Test
    void testShouldParseValidStringToEnum() {
        final Configuration configuration = new Configuration();
        configuration.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(TestEnum.VALUE1).isEqualTo(parsedEnumValue);
    }

    @Test
    void testShouldParseValidStringToEnumIgnoringCase() {
        final Configuration configuration = new Configuration();
        configuration.setString(STRING_OPTION.key(), TestEnum.VALUE1.toString().toLowerCase());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, STRING_OPTION);
        assertThat(TestEnum.VALUE1).isEqualTo(parsedEnumValue);
    }

    @Test
    void testThrowsExceptionIfTryingToParseInvalidStringForEnum() {
        final Configuration configuration = new Configuration();
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

    @Test
    void testToMap() {
        final Configuration configuration = new Configuration();
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

        assertThat(yamlListValues).isEqualTo(configuration.toMap().get(LIST_STRING_OPTION.key()));
        assertThat(yamlMapValues).isEqualTo(configuration.toMap().get(MAP_OPTION.key()));
        assertThat("3 s").isEqualTo(configuration.toMap().get(DURATION_OPTION.key()));
    }

    @Test
    void testToFileWritableMap() {
        final Configuration configuration = new Configuration();
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

        assertThat(configuration.toFileWritableMap().get(LIST_STRING_OPTION.key()))
                .isEqualTo(yamlListValues);
        assertThat(configuration.toFileWritableMap().get(MAP_OPTION.key()))
                .isEqualTo(yamlMapValues);
        assertThat(configuration.toFileWritableMap().get(STRING_OPTION.key()))
                .isEqualTo(yamlStrValues);
        assertThat(configuration.toMap().get(DURATION_OPTION.key())).isEqualTo("3 s");
    }

    @Test
    void testMapNotContained() {
        final Configuration cfg = new Configuration();

        assertThat(cfg.getOptional(MAP_OPTION)).isNotPresent();
        assertThat(cfg.contains(MAP_OPTION)).isFalse();
    }

    @Test
    void testMapWithPrefix() {
        final Configuration cfg = new Configuration();
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.set(getIntConfigOption(MAP_PROPERTY_2), 12);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @Test
    void testMapWithoutPrefix() {
        final Configuration cfg = new Configuration();
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
    }

    @Test
    void testMapNonPrefixHasPrecedence() {
        final Configuration cfg = new Configuration();
        cfg.set(MAP_OPTION, PROPERTIES_MAP);
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.get(getIntConfigOption(MAP_PROPERTY_2), 99999);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isTrue();
    }

    @Test
    void testMapThatOverwritesPrefix() {
        final Configuration cfg = new Configuration();
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.get(getIntConfigOption(MAP_PROPERTY_2), 99999);
        cfg.set(MAP_OPTION, PROPERTIES_MAP);

        assertThat(cfg.get(MAP_OPTION)).isEqualTo(PROPERTIES_MAP);
        assertThat(cfg.contains(MAP_OPTION)).isTrue();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isFalse();
    }

    @Test
    void testMapRemovePrefix() {
        final Configuration cfg = new Configuration();
        cfg.setString(MAP_PROPERTY_1, "value1");
        cfg.get(getIntConfigOption(MAP_PROPERTY_2), 99999);
        cfg.removeConfig(MAP_OPTION);

        assertThat(cfg.contains(MAP_OPTION)).isFalse();
        assertThat(cfg.containsKey(MAP_PROPERTY_1)).isFalse();
        assertThat(cfg.containsKey(MAP_PROPERTY_2)).isFalse();
    }

    @Test
    void testListParserErrorDoesNotLeakSensitiveData() {
        ConfigOption<List<String>> secret =
                ConfigOptions.key("secret").stringType().asList().noDefaultValue();

        assertThat(GlobalConfiguration.isSensitive(secret.key())).isTrue();

        final Configuration cfg = new Configuration();
        // missing closing quote
        cfg.setString(secret.key(), "'secret_value");

        assertThatThrownBy(() -> cfg.get(secret))
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(
                        e ->
                                assertThat(ExceptionUtils.stringifyException(e))
                                        .doesNotContain("secret_value"));
    }

    @Test
    void testMapParserErrorDoesNotLeakSensitiveData() {
        ConfigOption<Map<String, String>> secret =
                ConfigOptions.key("secret").mapType().noDefaultValue();

        assertThat(GlobalConfiguration.isSensitive(secret.key())).isTrue();

        final Configuration cfg = new Configuration();
        // malformed map representation
        cfg.setString(secret.key(), "secret_value");

        assertThatThrownBy(() -> cfg.get(secret))
                .isInstanceOf(IllegalArgumentException.class)
                .satisfies(
                        e ->
                                assertThat(ExceptionUtils.stringifyException(e))
                                        .doesNotContain("secret_value"));
    }

    @Test
    void testToStringDoesNotLeakSensitiveData() {
        ConfigOption<Map<String, String>> secret =
                ConfigOptions.key("secret").mapType().noDefaultValue();

        assertThat(GlobalConfiguration.isSensitive(secret.key())).isTrue();

        final Configuration cfg = new Configuration();
        cfg.setString(secret.key(), "secret_value");

        assertThat(cfg.toString()).doesNotContain("secret_value");
    }

    @Test
    void testGetWithOverrideDefault() {
        final Configuration conf = new Configuration();

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
