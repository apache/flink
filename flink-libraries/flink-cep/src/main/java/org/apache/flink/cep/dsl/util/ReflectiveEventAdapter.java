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

package org.apache.flink.cep.dsl.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.cep.dsl.api.EventAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Event adapter that uses Java reflection to access POJO fields and getters.
 *
 * <p>This adapter automatically discovers and caches field accessors for performance. It supports:
 * <ul>
 *   <li>Public getter methods (e.g., {@code getTemperature()} for attribute "temperature")</li>
 *   <li>Direct field access (if no getter is found)</li>
 *   <li>Nested attributes using dot notation (e.g., "sensor.location.city")</li>
 * </ul>
 *
 * <p>Thread-safe and optimized for repeated access to the same attributes.
 *
 * @param <T> The type of events to adapt
 */
@PublicEvolving
public class ReflectiveEventAdapter<T> implements EventAdapter<T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ReflectiveEventAdapter.class);

    // Cache for field accessors (not serializable, will be rebuilt after deserialization)
    private transient ConcurrentHashMap<String, FieldAccessor> cache;

    @Override
    public Optional<Object> getAttribute(T event, String attributeName) {
        if (event == null) {
            return Optional.empty();
        }

        initCache();

        // Handle nested attributes (e.g., "user.name")
        if (attributeName.contains(".")) {
            return getNestedAttribute(event, attributeName);
        }

        // Get or create accessor for this attribute
        String cacheKey = event.getClass().getName() + "." + attributeName;
        FieldAccessor accessor =
                cache.computeIfAbsent(
                        cacheKey, key -> createAccessor(event.getClass(), attributeName));

        return accessor.get(event);
    }

    @Override
    public String getEventType(T event) {
        if (event == null) {
            return "null";
        }
        return event.getClass().getSimpleName();
    }

    /** Handle nested attribute access (e.g., "sensor.location.city"). */
    private Optional<Object> getNestedAttribute(Object event, String attributeName) {
        String[] parts = attributeName.split("\\.", 2);
        String first = parts[0];
        String rest = parts[1];

        Optional<Object> intermediate = getAttribute((T) event, first);
        if (!intermediate.isPresent()) {
            return Optional.empty();
        }

        // Recursively access nested attribute
        ReflectiveEventAdapter<Object> nestedAdapter = new ReflectiveEventAdapter<>();
        return nestedAdapter.getAttribute(intermediate.get(), rest);
    }

    /** Initialize the cache if needed (handles deserialization). */
    private void initCache() {
        if (cache == null) {
            cache = new ConcurrentHashMap<>();
        }
    }

    /** Create an accessor for the given class and field name. */
    private FieldAccessor createAccessor(Class<?> clazz, String fieldName) {
        // Try getter method first (e.g., getTemperature())
        String getterName = "get" + capitalize(fieldName);
        try {
            Method method = clazz.getMethod(getterName);
            LOG.debug(
                    "Created method accessor for {}.{} using getter {}",
                    clazz.getName(),
                    fieldName,
                    getterName);
            return new MethodAccessor(method);
        } catch (NoSuchMethodException e) {
            // Try boolean getter (e.g., isActive())
            String booleanGetterName = "is" + capitalize(fieldName);
            try {
                Method method = clazz.getMethod(booleanGetterName);
                LOG.debug(
                        "Created method accessor for {}.{} using boolean getter {}",
                        clazz.getName(),
                        fieldName,
                        booleanGetterName);
                return new MethodAccessor(method);
            } catch (NoSuchMethodException e2) {
                // Fall back to direct field access
                try {
                    Field field = findField(clazz, fieldName);
                    field.setAccessible(true);
                    LOG.debug(
                            "Created field accessor for {}.{} using direct field access",
                            clazz.getName(),
                            fieldName);
                    return new DirectFieldAccessor(field);
                } catch (NoSuchFieldException e3) {
                    LOG.warn(
                            "No accessor found for attribute '{}' on class {}",
                            fieldName,
                            clazz.getName());
                    return new NullAccessor();
                }
            }
        }
    }

    /** Find a field in the class hierarchy. */
    private Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(
                "Field '" + fieldName + "' not found in " + clazz.getName());
    }

    /** Capitalize the first letter of a string. */
    private String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    // Internal accessor interfaces

    /** Interface for accessing a field/method. */
    private interface FieldAccessor extends Serializable {
        Optional<Object> get(Object obj);
    }

    /** Accessor using a getter method. */
    private static class MethodAccessor implements FieldAccessor {
        private static final long serialVersionUID = 1L;
        private final Method method;

        MethodAccessor(Method method) {
            this.method = method;
        }

        @Override
        public Optional<Object> get(Object obj) {
            try {
                return Optional.ofNullable(method.invoke(obj));
            } catch (Exception e) {
                LOG.warn("Failed to invoke method {} on object", method.getName(), e);
                return Optional.empty();
            }
        }
    }

    /** Accessor using direct field access. */
    private static class DirectFieldAccessor implements FieldAccessor {
        private static final long serialVersionUID = 1L;
        private final Field field;

        DirectFieldAccessor(Field field) {
            this.field = field;
        }

        @Override
        public Optional<Object> get(Object obj) {
            try {
                return Optional.ofNullable(field.get(obj));
            } catch (Exception e) {
                LOG.warn("Failed to access field {} on object", field.getName(), e);
                return Optional.empty();
            }
        }
    }

    /** Null accessor when no field/method is found. */
    private static class NullAccessor implements FieldAccessor {
        private static final long serialVersionUID = 1L;

        @Override
        public Optional<Object> get(Object obj) {
            return Optional.empty();
        }
    }
}
