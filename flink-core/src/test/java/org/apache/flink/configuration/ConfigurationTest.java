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

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains test for the configuration package. In particular, the serialization of
 * {@link Configuration} objects is tested.
 */
public class ConfigurationTest extends TestLogger {

    /** This test checks the serialization/deserialization of configuration objects. */
    @Test
    public void testConfigurationSerializationAndGetters() {
        try {
            final Configuration orig = new Configuration();
            orig.setString("mykey", "myvalue");
            orig.setInteger("mynumber", 100);
            orig.setLong("longvalue", 478236947162389746L);
            orig.setFloat("PI", 3.1415926f);
            orig.setDouble("E", Math.E);
            orig.setBoolean("shouldbetrue", true);
            orig.setBytes("bytes sequence", new byte[] {1, 2, 3, 4, 5});
            orig.setClass("myclass", this.getClass());

            final Configuration copy = InstantiationUtil.createCopyWritable(orig);
            assertEquals("myvalue", copy.getString("mykey", "null"));
            assertEquals(100, copy.getInteger("mynumber", 0));
            assertEquals(478236947162389746L, copy.getLong("longvalue", 0L));
            assertEquals(3.1415926f, copy.getFloat("PI", 3.1415926f), 0.0);
            assertEquals(Math.E, copy.getDouble("E", 0.0), 0.0);
            assertEquals(true, copy.getBoolean("shouldbetrue", false));
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, copy.getBytes("bytes sequence", null));
            assertEquals(getClass(), copy.getClass("myclass", null, getClass().getClassLoader()));

            assertEquals(orig, copy);
            assertEquals(orig.keySet(), copy.keySet());
            assertEquals(orig.hashCode(), copy.hashCode());

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCopyConstructor() {
        try {
            final String key = "theKey";

            Configuration cfg1 = new Configuration();
            cfg1.setString(key, "value");

            Configuration cfg2 = new Configuration(cfg1);
            cfg2.setString(key, "another value");

            assertEquals("value", cfg1.getString(key, ""));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testOptionWithDefault() {
        Configuration cfg = new Configuration();
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption =
                ConfigOptions.key("string-key").defaultValue("my-beautiful-default");
        ConfigOption<Integer> presentIntOption = ConfigOptions.key("int-key").defaultValue(87);

        assertEquals("abc", cfg.getString(presentStringOption));
        assertEquals("abc", cfg.getValue(presentStringOption));

        assertEquals(11, cfg.getInteger(presentIntOption));
        assertEquals("11", cfg.getValue(presentIntOption));

        // test getting default when no value is present

        ConfigOption<String> stringOption =
                ConfigOptions.key("test").defaultValue("my-beautiful-default");
        ConfigOption<Integer> intOption = ConfigOptions.key("test2").defaultValue(87);

        // getting strings with default value should work
        assertEquals("my-beautiful-default", cfg.getValue(stringOption));
        assertEquals("my-beautiful-default", cfg.getString(stringOption));

        // overriding the default should work
        assertEquals("override", cfg.getString(stringOption, "override"));

        // getting a primitive with a default value should work
        assertEquals(87, cfg.getInteger(intOption));
        assertEquals("87", cfg.getValue(intOption));
    }

    @Test
    public void testOptionWithNoDefault() {
        Configuration cfg = new Configuration();
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption = ConfigOptions.key("string-key").noDefaultValue();

        assertEquals("abc", cfg.getString(presentStringOption));
        assertEquals("abc", cfg.getValue(presentStringOption));

        // test getting default when no value is present

        ConfigOption<String> stringOption = ConfigOptions.key("test").noDefaultValue();

        // getting strings for null should work
        assertNull(cfg.getValue(stringOption));
        assertNull(cfg.getString(stringOption));

        // overriding the null default should work
        assertEquals("override", cfg.getString(stringOption, "override"));
    }

    @Test
    public void testDeprecatedKeys() {
        Configuration cfg = new Configuration();
        cfg.setInteger("the-key", 11);
        cfg.setInteger("old-key", 12);
        cfg.setInteger("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigOptions.key("the-key")
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withDeprecatedKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withDeprecatedKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withDeprecatedKeys("not-there", "also-not-there");

        assertEquals(11, cfg.getInteger(matchesFirst));
        assertEquals(12, cfg.getInteger(matchesSecond));
        assertEquals(13, cfg.getInteger(matchesThird));
        assertEquals(-1, cfg.getInteger(notContained));
    }

    @Test
    public void testFallbackKeys() {
        Configuration cfg = new Configuration();
        cfg.setInteger("the-key", 11);
        cfg.setInteger("old-key", 12);
        cfg.setInteger("older-key", 13);

        ConfigOption<Integer> matchesFirst =
                ConfigOptions.key("the-key")
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesSecond =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withFallbackKeys("old-key", "older-key");

        ConfigOption<Integer> matchesThird =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withFallbackKeys("foo", "older-key");

        ConfigOption<Integer> notContained =
                ConfigOptions.key("does-not-exist")
                        .defaultValue(-1)
                        .withFallbackKeys("not-there", "also-not-there");

        assertEquals(11, cfg.getInteger(matchesFirst));
        assertEquals(12, cfg.getInteger(matchesSecond));
        assertEquals(13, cfg.getInteger(matchesThird));
        assertEquals(-1, cfg.getInteger(notContained));
    }

    @Test
    public void testFallbackAndDeprecatedKeys() {
        final ConfigOption<Integer> fallback = ConfigOptions.key("fallback").defaultValue(-1);

        final ConfigOption<Integer> deprecated = ConfigOptions.key("deprecated").defaultValue(-1);

        final ConfigOption<Integer> mainOption =
                ConfigOptions.key("main")
                        .defaultValue(-1)
                        .withFallbackKeys(fallback.key())
                        .withDeprecatedKeys(deprecated.key());

        final Configuration fallbackCfg = new Configuration();
        fallbackCfg.setInteger(fallback, 1);
        assertEquals(1, fallbackCfg.getInteger(mainOption));

        final Configuration deprecatedCfg = new Configuration();
        deprecatedCfg.setInteger(deprecated, 2);
        assertEquals(2, deprecatedCfg.getInteger(mainOption));

        // reverse declaration of fallback and deprecated keys, fallback keys should always be used
        // first
        final ConfigOption<Integer> reversedMainOption =
                ConfigOptions.key("main")
                        .defaultValue(-1)
                        .withDeprecatedKeys(deprecated.key())
                        .withFallbackKeys(fallback.key());

        final Configuration deprecatedAndFallBackConfig = new Configuration();
        deprecatedAndFallBackConfig.setInteger(fallback, 1);
        deprecatedAndFallBackConfig.setInteger(deprecated, 2);
        assertEquals(1, deprecatedAndFallBackConfig.getInteger(mainOption));
        assertEquals(1, deprecatedAndFallBackConfig.getInteger(reversedMainOption));
    }

    @Test
    public void testRemove() {
        Configuration cfg = new Configuration();
        cfg.setInteger("a", 1);
        cfg.setInteger("b", 2);

        ConfigOption<Integer> validOption = ConfigOptions.key("a").defaultValue(-1);

        ConfigOption<Integer> deprecatedOption =
                ConfigOptions.key("c").defaultValue(-1).withDeprecatedKeys("d", "b");

        ConfigOption<Integer> unexistedOption =
                ConfigOptions.key("e").defaultValue(-1).withDeprecatedKeys("f", "g", "j");

        assertEquals("Wrong expectation about size", cfg.keySet().size(), 2);
        assertTrue("Expected 'validOption' is removed", cfg.removeConfig(validOption));
        assertEquals("Wrong expectation about size", cfg.keySet().size(), 1);
        assertTrue("Expected 'existedOption' is removed", cfg.removeConfig(deprecatedOption));
        assertEquals("Wrong expectation about size", cfg.keySet().size(), 0);
        assertFalse("Expected 'unexistedOption' is not removed", cfg.removeConfig(unexistedOption));
    }

    @Test
    public void testShouldParseValidStringToEnum() {
        final ConfigOption<String> configOption = createStringConfigOption();

        final Configuration configuration = new Configuration();
        configuration.setString(configOption.key(), TestEnum.VALUE1.toString());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, configOption);
        assertEquals(TestEnum.VALUE1, parsedEnumValue);
    }

    @Test
    public void testShouldParseValidStringToEnumIgnoringCase() {
        final ConfigOption<String> configOption = createStringConfigOption();

        final Configuration configuration = new Configuration();
        configuration.setString(configOption.key(), TestEnum.VALUE1.toString().toLowerCase());

        final TestEnum parsedEnumValue = configuration.getEnum(TestEnum.class, configOption);
        assertEquals(TestEnum.VALUE1, parsedEnumValue);
    }

    @Test
    public void testThrowsExceptionIfTryingToParseInvalidStringForEnum() {
        final ConfigOption<String> configOption = createStringConfigOption();

        final Configuration configuration = new Configuration();
        final String invalidValueForTestEnum = "InvalidValueForTestEnum";
        configuration.setString(configOption.key(), invalidValueForTestEnum);

        try {
            configuration.getEnum(TestEnum.class, configOption);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException e) {
            final String expectedMessage =
                    "Value for config option "
                            + configOption.key()
                            + " must be one of [VALUE1, VALUE2] (was "
                            + invalidValueForTestEnum
                            + ")";
            assertThat(e.getMessage(), containsString(expectedMessage));
        }
    }

    @Test
    public void testToMap() {
        final ConfigOption<List<String>> listConfigOption = createListStringConfigOption();
        final Configuration configuration = new Configuration();
        final String listValues = "value1;value2;value3";
        configuration.set(listConfigOption, Arrays.asList(listValues.split(";")));

        final ConfigOption<Map<String, String>> mapConfigOption = createMapConfigOption();
        final String mapValues = "key1:value1,key2:value2";
        configuration.set(
                mapConfigOption,
                Arrays.stream(mapValues.split(","))
                        .collect(Collectors.toMap(e -> e.split(":")[0], e -> e.split(":")[1])));

        final ConfigOption<Duration> durationConfigOption = createDurationConfigOption();
        final Duration duration = Duration.ofMillis(3000);
        configuration.set(durationConfigOption, duration);

        assertEquals(listValues, configuration.toMap().get(listConfigOption.key()));
        assertEquals(mapValues, configuration.toMap().get(mapConfigOption.key()));
        assertEquals(
                duration.toNanos() + " ns", configuration.toMap().get(durationConfigOption.key()));
    }

    enum TestEnum {
        VALUE1,
        VALUE2
    }

    private static ConfigOption<String> createStringConfigOption() {
        return ConfigOptions.key("test-string-key").noDefaultValue();
    }

    private static ConfigOption<List<String>> createListStringConfigOption() {
        return ConfigOptions.key("test-list-key").stringType().asList().noDefaultValue();
    }

    private static ConfigOption<Map<String, String>> createMapConfigOption() {
        return ConfigOptions.key("test-map-key").mapType().noDefaultValue();
    }

    private static ConfigOption<Duration> createDurationConfigOption() {
        return ConfigOptions.key("test-duration-key").durationType().noDefaultValue();
    }
}
