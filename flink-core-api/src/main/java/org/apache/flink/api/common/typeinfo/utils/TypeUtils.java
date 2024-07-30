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

package org.apache.flink.api.common.typeinfo.utils;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.lang.reflect.Constructor;

/** Utility class to create objects via reflection. */
@Experimental
public class TypeUtils implements Serializable {

    public static Object getInstance(String classPath, Object... args)
            throws ReflectiveOperationException {
        Class<?> descriptorClass = Class.forName(classPath);
        Constructor<?>[] constructors = descriptorClass.getConstructors();

        for (Constructor<?> constructor : constructors) {
            Class<?>[] parameterTypes = constructor.getParameterTypes();

            if (areParameterTypesCompatible(parameterTypes, args)) {
                return constructor.newInstance(convertArgs(parameterTypes, args));
            }
        }

        throw new NoSuchMethodException("No suitable constructor found for the given arguments.");
    }

    private static boolean areParameterTypesCompatible(Class<?>[] parameterTypes, Object[] args) {
        if (parameterTypes.length != args.length) {
            return false;
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            if (args[i] == null) {
                if (parameterTypes[i].isPrimitive()) {
                    return false; // primitive types cannot be null
                }
            } else if (!isCompatible(parameterTypes[i], args[i])) {
                return false;
            }
        }
        return true;
    }

    private static boolean isCompatible(Class<?> parameterType, Object arg) {
        if (parameterType.isPrimitive()) {
            Class<?> wrapperType = getWrapperClass(parameterType);
            return wrapperType.isInstance(arg);
        } else {
            return parameterType.isInstance(arg);
        }
    }

    private static Class<?> getWrapperClass(Class<?> primitiveType) {
        if (primitiveType == boolean.class) {
            return Boolean.class;
        } else if (primitiveType == byte.class) {
            return Byte.class;
        } else if (primitiveType == char.class) {
            return Character.class;
        } else if (primitiveType == short.class) {
            return Short.class;
        } else if (primitiveType == int.class) {
            return Integer.class;
        } else if (primitiveType == long.class) {
            return Long.class;
        } else if (primitiveType == float.class) {
            return Float.class;
        } else if (primitiveType == double.class) {
            return Double.class;
        }
        throw new IllegalArgumentException("Unknown primitive type: " + primitiveType.getName());
    }

    private static Object[] convertArgs(Class<?>[] parameterTypes, Object[] args) {
        Object[] convertedArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            if (parameterTypes[i].isPrimitive()) {
                convertedArgs[i] = convertToPrimitiveWrapper(parameterTypes[i], args[i]);
            } else {
                convertedArgs[i] = args[i];
            }
        }
        return convertedArgs;
    }

    private static Object convertToPrimitiveWrapper(Class<?> parameterType, Object arg) {
        if (parameterType == boolean.class) {
            return (Boolean) arg;
        } else if (parameterType == byte.class) {
            return (Byte) arg;
        } else if (parameterType == char.class) {
            return (Character) arg;
        } else if (parameterType == short.class) {
            return (Short) arg;
        } else if (parameterType == int.class) {
            return (Integer) arg;
        } else if (parameterType == long.class) {
            return (Long) arg;
        } else if (parameterType == float.class) {
            return (Float) arg;
        } else if (parameterType == double.class) {
            return (Double) arg;
        }
        throw new IllegalArgumentException("Unknown primitive type: " + parameterType.getName());
    }
}
