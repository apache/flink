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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/** Tests the {@link ConfigUtils} methods. */
public class ConfigUtilsTest {

    private static final ConfigOption<List<String>> TEST_OPTION =
            key("test.option.key").stringType().asList().noDefaultValue();

    private static final Integer[] intArray = {1, 3, 2, 4};
    private static final List<Integer> intList = Arrays.asList(intArray);

    @Test
    public void collectionIsCorrectlyPutAndFetched() {
        final Configuration configurationUnderTest = new Configuration();
        ConfigUtils.encodeCollectionToConfig(
                configurationUnderTest, TEST_OPTION, intList, Object::toString);

        final List<Integer> recovered =
                ConfigUtils.decodeListFromConfig(
                        configurationUnderTest, TEST_OPTION, Integer::valueOf);
        assertThat(recovered, equalTo(intList));
    }

    @Test
    public void arrayIsCorrectlyPutAndFetched() {
        final Configuration configurationUnderTest = new Configuration();
        ConfigUtils.encodeArrayToConfig(
                configurationUnderTest, TEST_OPTION, intArray, Object::toString);

        final List<Integer> recovered =
                ConfigUtils.decodeListFromConfig(
                        configurationUnderTest, TEST_OPTION, Integer::valueOf);
        assertThat(recovered, equalTo(intList));
    }

    @Test
    public void nullCollectionPutsNothingInConfig() {
        final Configuration configurationUnderTest = new Configuration();
        ConfigUtils.encodeCollectionToConfig(
                configurationUnderTest, TEST_OPTION, null, Object::toString);

        assertThat(configurationUnderTest.keySet(), is(empty()));

        final Object recovered = configurationUnderTest.get(TEST_OPTION);
        assertThat(recovered, is(nullValue()));

        final List<Integer> recoveredList =
                ConfigUtils.decodeListFromConfig(
                        configurationUnderTest, TEST_OPTION, Integer::valueOf);
        assertThat(recoveredList, is(empty()));
    }

    @Test
    public void nullArrayPutsNothingInConfig() {
        final Configuration configurationUnderTest = new Configuration();
        ConfigUtils.encodeArrayToConfig(
                configurationUnderTest, TEST_OPTION, null, Object::toString);

        assertThat(configurationUnderTest.keySet(), is(empty()));

        final Object recovered = configurationUnderTest.get(TEST_OPTION);
        assertThat(recovered, is(nullValue()));

        final List<Integer> recoveredList =
                ConfigUtils.decodeListFromConfig(
                        configurationUnderTest, TEST_OPTION, Integer::valueOf);
        assertThat(recoveredList, is(empty()));
    }

    @Test
    public void emptyCollectionPutsEmptyValueInConfig() {
        final Configuration configurationUnderTest = new Configuration();
        ConfigUtils.encodeCollectionToConfig(
                configurationUnderTest, TEST_OPTION, Collections.emptyList(), Object::toString);

        final List<String> recovered = configurationUnderTest.get(TEST_OPTION);
        assertThat(recovered, is(empty()));

        final List<Integer> recoveredList =
                ConfigUtils.decodeListFromConfig(
                        configurationUnderTest, TEST_OPTION, Integer::valueOf);
        assertThat(recoveredList, is(empty()));
    }

    @Test
    public void emptyArrayPutsEmptyValueInConfig() {
        final Configuration configurationUnderTest = new Configuration();
        ConfigUtils.encodeArrayToConfig(
                configurationUnderTest, TEST_OPTION, new Integer[5], Object::toString);

        final List<String> recovered = configurationUnderTest.get(TEST_OPTION);
        assertThat(recovered, is(empty()));

        final List<Integer> recoveredList =
                ConfigUtils.decodeListFromConfig(
                        configurationUnderTest, TEST_OPTION, Integer::valueOf);
        assertThat(recoveredList, is(empty()));
    }
}
