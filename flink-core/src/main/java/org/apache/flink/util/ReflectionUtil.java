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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

/** Utility for reflection operations on classes and generic type parametrization. */
@Internal
@SuppressWarnings("unused")
public final class ReflectionUtil {

    public static <T> T newInstance(Class<T> clazz) {
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTemplateType(Class<?> clazz, int num) {
        return (Class<T>) getSuperTemplateTypes(clazz)[num];
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTemplateType(
            Class<?> clazz, Class<?> classWithParameter, int num) {
        return (Class<T>) getSuperTemplateTypes(clazz)[num];
    }

    public static <T> Class<T> getTemplateType1(Class<?> clazz) {
        return getTemplateType(clazz, 0);
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTemplateType1(Type type) {
        if (type instanceof ParameterizedType) {
            return (Class<T>) getTemplateTypes((ParameterizedType) type)[0];
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static <T> Class<T> getTemplateType2(Class<?> clazz) {
        return getTemplateType(clazz, 1);
    }

    public static <T> Class<T> getTemplateType3(Class<?> clazz) {
        return getTemplateType(clazz, 2);
    }

    public static <T> Class<T> getTemplateType4(Class<?> clazz) {
        return getTemplateType(clazz, 3);
    }

    public static <T> Class<T> getTemplateType5(Class<?> clazz) {
        return getTemplateType(clazz, 4);
    }

    public static <T> Class<T> getTemplateType6(Class<?> clazz) {
        return getTemplateType(clazz, 5);
    }

    public static <T> Class<T> getTemplateType7(Class<?> clazz) {
        return getTemplateType(clazz, 6);
    }

    public static <T> Class<T> getTemplateType8(Class<?> clazz) {
        return getTemplateType(clazz, 7);
    }

    public static Class<?>[] getSuperTemplateTypes(Class<?> clazz) {
        Type type = clazz.getGenericSuperclass();
        while (true) {
            if (type instanceof ParameterizedType) {
                return getTemplateTypes((ParameterizedType) type);
            }

            if (clazz.getGenericSuperclass() == null) {
                throw new IllegalArgumentException();
            }

            type = clazz.getGenericSuperclass();
            clazz = clazz.getSuperclass();
        }
    }

    public static Class<?>[] getSuperTemplateTypes(Class<?> clazz, Class<?> searchedSuperClass) {
        if (clazz == null || searchedSuperClass == null) {
            throw new NullPointerException();
        }

        Class<?> superClass;
        do {
            superClass = clazz.getSuperclass();
            if (superClass == searchedSuperClass) {
                break;
            }
        } while ((clazz = superClass) != null);

        if (clazz == null) {
            throw new IllegalArgumentException(
                    "The searched for superclass is not a superclass of the given class.");
        }

        final Type type = clazz.getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            return getTemplateTypes((ParameterizedType) type);
        } else {
            throw new IllegalArgumentException(
                    "The searched for superclass is not a generic class.");
        }
    }

    public static Class<?>[] getTemplateTypes(ParameterizedType paramterizedType) {
        Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
        int i = 0;
        for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
            assert templateArgument instanceof Class<?>;
            types[i++] = (Class<?>) templateArgument;
        }
        return types;
    }

    public static Class<?>[] getTemplateTypes(Class<?> clazz) {
        Type type = clazz.getGenericSuperclass();
        assert (type instanceof ParameterizedType);
        ParameterizedType paramterizedType = (ParameterizedType) type;
        Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
        int i = 0;
        for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
            assert (templateArgument instanceof Class<?>);
            types[i++] = (Class<?>) templateArgument;
        }
        return types;
    }

    /**
     * Extract the full template type information from the given type's template parameter at the
     * given position.
     *
     * @param type type to extract the full template parameter information from
     * @param templatePosition describing at which position the template type parameter is
     * @return Full type information describing the template parameter's type
     */
    public static FullTypeInfo getFullTemplateType(Type type, int templatePosition) {
        if (type instanceof ParameterizedType) {
            return getFullTemplateType(
                    ((ParameterizedType) type).getActualTypeArguments()[templatePosition]);
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Extract the full type information from the given type.
     *
     * @param type to be analyzed
     * @return Full type information describing the given type
     */
    public static FullTypeInfo getFullTemplateType(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;

            FullTypeInfo[] templateTypeInfos =
                    new FullTypeInfo[parameterizedType.getActualTypeArguments().length];

            for (int i = 0; i < parameterizedType.getActualTypeArguments().length; i++) {
                templateTypeInfos[i] =
                        getFullTemplateType(parameterizedType.getActualTypeArguments()[i]);
            }

            return new FullTypeInfo((Class<?>) parameterizedType.getRawType(), templateTypeInfos);
        } else {
            return new FullTypeInfo((Class<?>) type, null);
        }
    }

    /**
     * Container for the full type information of a type. This means that it contains the {@link
     * Class} object and for each template parameter it contains a full type information describing
     * the type.
     */
    public static class FullTypeInfo {

        private final Class<?> clazz;
        private final FullTypeInfo[] templateTypeInfos;

        public FullTypeInfo(Class<?> clazz, FullTypeInfo[] templateTypeInfos) {
            this.clazz = Preconditions.checkNotNull(clazz);
            this.templateTypeInfos = templateTypeInfos;
        }

        public Class<?> getClazz() {
            return clazz;
        }

        public FullTypeInfo[] getTemplateTypeInfos() {
            return templateTypeInfos;
        }

        public Iterator<Class<?>> getClazzIterator() {
            UnionIterator<Class<?>> unionIterator = new UnionIterator<>();

            unionIterator.add(Collections.<Class<?>>singleton(clazz).iterator());

            if (templateTypeInfos != null) {
                for (int i = 0; i < templateTypeInfos.length; i++) {
                    unionIterator.add(templateTypeInfos[i].getClazzIterator());
                }
            }

            return unionIterator;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();

            builder.append(clazz.getSimpleName());

            if (templateTypeInfos != null) {
                builder.append("<");

                for (int i = 0; i < templateTypeInfos.length - 1; i++) {
                    builder.append(templateTypeInfos[i]).append(", ");
                }

                builder.append(templateTypeInfos[templateTypeInfos.length - 1]);
                builder.append(">");
            }

            return builder.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FullTypeInfo) {
                FullTypeInfo other = (FullTypeInfo) obj;

                return clazz == other.getClazz()
                        && Arrays.equals(templateTypeInfos, other.getTemplateTypeInfos());
            } else {
                return false;
            }
        }
    }

    /** Private constructor to prevent instantiation. */
    private ReflectionUtil() {
        throw new RuntimeException();
    }
}
