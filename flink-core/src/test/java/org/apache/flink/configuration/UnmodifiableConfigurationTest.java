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

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class verifies that the Unmodifiable Configuration class overrides all setter methods in
 * Configuration.
 */
class UnmodifiableConfigurationTest {

    @Test
    void testOverrideAddMethods() {
        Class<UnmodifiableConfiguration> clazz = UnmodifiableConfiguration.class;
        for (Method m : clazz.getMethods()) {
            if (m.getName().startsWith("add")) {
                assertThat(m.getDeclaringClass()).isEqualTo(clazz);
            }
        }
    }

    @Test
    void testExceptionOnSet() {
        @SuppressWarnings("rawtypes")
        final ConfigOption rawOption = ConfigOptions.key("testkey").defaultValue("value");

        Map<Class<?>, Object> parameters = new HashMap<>();
        parameters.put(byte[].class, new byte[0]);
        parameters.put(Class.class, Object.class);
        parameters.put(int.class, 0);
        parameters.put(long.class, 0L);
        parameters.put(float.class, 0.0f);
        parameters.put(double.class, 0.0);
        parameters.put(String.class, "");
        parameters.put(boolean.class, false);

        Class<UnmodifiableConfiguration> clazz = UnmodifiableConfiguration.class;
        UnmodifiableConfiguration config = new UnmodifiableConfiguration(new Configuration());

        for (Method m : clazz.getMethods()) {
            // ignore WritableConfig#set as it is covered in ReadableWritableConfigurationTest
            if (m.getName().startsWith("set") && !m.getName().equals("set")) {

                Class<?> keyClass = m.getParameterTypes()[0];
                Class<?> parameterClass = m.getParameterTypes()[1];
                Object key = keyClass == String.class ? "key" : rawOption;

                Object parameter = parameters.get(parameterClass);
                assertThat(parameter).as("method " + m + " not covered by test").isNotNull();

                assertThatThrownBy(() -> m.invoke(config, key, parameter))
                        .isInstanceOf(InvocationTargetException.class)
                        .hasCauseInstanceOf(UnsupportedOperationException.class);
            }
        }
    }
}
