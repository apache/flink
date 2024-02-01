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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.base.Defaults;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Utility class for constructing Java records in the {@link PojoSerializer}. */
@Internal
final class JavaRecordBuilderFactory<T> {

    private final Constructor<T> canonicalConstructor;

    /**
     * Record constructor parameter index mapping in case the new constructor has a different
     * parameter order than the serialized data. Used for schema evolution or `null` if no schema
     * evolution is applied for that record class.
     */
    @Nullable private final int[] paramIndexMapping;

    /**
     * Default record args used for newly introduced primitive fields during schema evolution.
     * `null` if no schema evolution is applied for that record class.
     */
    @Nullable private final Object[] defaultConstructorArgs;

    private JavaRecordBuilderFactory(Constructor<T> canonicalConstructor) {
        this(canonicalConstructor, null, null);
    }

    private JavaRecordBuilderFactory(
            Constructor<T> canonicalConstructor,
            @Nullable int[] argIndexMapping,
            @Nullable Object[] defaultConstructorArgs) {
        Preconditions.checkArgument((argIndexMapping == null) == (defaultConstructorArgs == null));
        this.canonicalConstructor = canonicalConstructor;
        this.paramIndexMapping = argIndexMapping;
        this.defaultConstructorArgs = defaultConstructorArgs;
    }

    JavaRecordBuilder newBuilder() {
        return new JavaRecordBuilder();
    }

    /** Builder class for incremental record construction. */
    @Internal
    final class JavaRecordBuilder {
        private final Object[] args;

        JavaRecordBuilder() {
            if (defaultConstructorArgs == null) {
                args = new Object[canonicalConstructor.getParameterCount()];
            } else {
                args = Arrays.copyOf(defaultConstructorArgs, defaultConstructorArgs.length);
            }
        }

        T build() {
            try {
                return canonicalConstructor.newInstance(args);
            } catch (Exception e) {
                throw new RuntimeException("Could not instantiate record", e);
            }
        }

        /**
         * Set record field by index. If parameter index mapping is provided, the index is mapped,
         * otherwise it is used as is.
         *
         * @param i index of field to be set
         * @param value field value
         */
        void setField(int i, Object value) {
            if (paramIndexMapping != null) {
                args[paramIndexMapping[i]] = value;
            } else {
                args[i] = value;
            }
        }
    }

    static <T> JavaRecordBuilderFactory<T> create(Class<T> clazz, Field[] fields) {
        try {
            Object[] recordComponents =
                    (Object[]) Class.class.getMethod("getRecordComponents").invoke(clazz);

            Class<?>[] componentTypes = new Class[recordComponents.length];
            List<String> componentNames = new ArrayList<>(recordComponents.length);

            // We need to use reflection to access record components as they are not available in
            // before Java 14
            Method getType =
                    Class.forName("java.lang.reflect.RecordComponent").getMethod("getType");
            Method getName =
                    Class.forName("java.lang.reflect.RecordComponent").getMethod("getName");
            for (int i = 0; i < recordComponents.length; i++) {
                componentNames.add((String) getName.invoke(recordComponents[i]));
                componentTypes[i] = (Class<?>) getType.invoke(recordComponents[i]);
            }
            Constructor<T> recordConstructor = clazz.getDeclaredConstructor(componentTypes);
            recordConstructor.setAccessible(true);

            List<String> previousFields =
                    Arrays.stream(fields)
                            // There may be (removed) null fields due to schema evolution
                            .filter(Objects::nonNull)
                            .map(Field::getName)
                            .collect(Collectors.toList());

            // If the field names / order changed we know that we are migrating the records and arg
            // index remapping may be necessary
            boolean migrating = !previousFields.equals(componentNames);
            if (migrating) {
                // If the order / index of arguments changed in the new record class we have to map
                // it, otherwise we pass the wrong arguments to the constructor
                int[] argIndexMapping = new int[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[i];
                    // There may be (removed) null fields due to schema evolution
                    argIndexMapping[i] =
                            field == null ? -1 : componentNames.indexOf(fields[i].getName());
                }

                // We have to initialize newly added primitive fields to their correct default value
                Object[] defaultValues = new Object[componentNames.size()];
                for (int i = 0; i < componentNames.size(); i++) {
                    Class<?> fieldType = componentTypes[i];
                    boolean newPrimitive =
                            fieldType.isPrimitive()
                                    && !previousFields.contains(componentNames.get(i));
                    defaultValues[i] = newPrimitive ? Defaults.defaultValue(fieldType) : null;
                }
                return new JavaRecordBuilderFactory<>(
                        recordConstructor, argIndexMapping, defaultValues);
            } else {
                return new JavaRecordBuilderFactory<>(recordConstructor);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not find record canonical constructor", e);
        }
    }
}
