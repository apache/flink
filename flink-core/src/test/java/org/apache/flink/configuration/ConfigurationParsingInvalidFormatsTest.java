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

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for reading configuration parameters with invalid formats. */
@ExtendWith(ParameterizedTestExtension.class)
class ConfigurationParsingInvalidFormatsTest {

    @Parameters(name = "option = {0}, invalidString = {1}")
    private static Object[][] getSpecs() {
        return new Object[][] {
            new Object[] {ConfigOptions.key("int").intType().defaultValue(1), "ABC"},
            new Object[] {ConfigOptions.key("long").longType().defaultValue(1L), "ABC"},
            new Object[] {ConfigOptions.key("float").floatType().defaultValue(1F), "ABC"},
            new Object[] {ConfigOptions.key("double").doubleType().defaultValue(1D), "ABC"},
            new Object[] {ConfigOptions.key("boolean").booleanType().defaultValue(true), "ABC"},
            new Object[] {
                ConfigOptions.key("memory").memoryType().defaultValue(MemorySize.parse("1kB")),
                "ABC"
            },
            new Object[] {
                ConfigOptions.key("duration").durationType().defaultValue(Duration.ofSeconds(1)),
                "ABC"
            },
            new Object[] {
                ConfigOptions.key("enum").enumType(TestEnum.class).defaultValue(TestEnum.ENUM1),
                "ABC"
            },
            new Object[] {
                ConfigOptions.key("map").mapType().defaultValue(Collections.emptyMap()), "ABC"
            },
            new Object[] {
                ConfigOptions.key("list<int>").intType().asList().defaultValues(1, 2), "A;B;C"
            },
            new Object[] {
                ConfigOptions.key("list<string>").stringType().asList().defaultValues("A"), "'A;B;C"
            }
        };
    }

    @Parameter private ConfigOption<?> option;

    @Parameter(value = 1)
    private String invalidString;

    @TestTemplate
    void testInvalidStringParsingWithGetOptional() {
        assertThatThrownBy(
                        () -> {
                            Configuration configuration = new Configuration();
                            configuration.setString(option.key(), invalidString);
                            configuration.getOptional(option);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        String.format(
                                "Could not parse value '%s' for key '%s'",
                                invalidString, option.key()));
    }

    @TestTemplate
    void testInvalidStringParsingWithGet() {
        assertThatThrownBy(
                        () -> {
                            Configuration configuration = new Configuration();
                            configuration.setString(option.key(), invalidString);
                            configuration.get(option);
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        String.format(
                                "Could not parse value '%s' for key '%s'",
                                invalidString, option.key()));
    }

    private enum TestEnum {
        ENUM1,
        ENUM2
    }
}
