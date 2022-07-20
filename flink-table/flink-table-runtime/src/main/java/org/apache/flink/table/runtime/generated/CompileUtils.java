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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities to compile a generated code to a Class. */
public final class CompileUtils {

    // used for logging the generated codes to a same place
    private static final Logger CODE_LOG = LoggerFactory.getLogger(CompileUtils.class);

    /**
     * Cache of compile, Janino generates a new Class Loader and a new Class file every compile
     * (guaranteeing that the class name will not be repeated). This leads to multiple tasks of the
     * same process that generate a large number of duplicate class, resulting in a large number of
     * Meta zone GC (class unloading), resulting in performance bottlenecks. So we add a cache to
     * avoid this problem.
     */
    static final Cache<ClassKey, Class<?>> COMPILED_CLASS_CACHE =
            CacheBuilder.newBuilder()
                    // estimated maximum planning/startup time
                    .expireAfterAccess(Duration.ofMinutes(5))
                    // estimated cache size
                    .maximumSize(300)
                    .softValues()
                    .build();

    static final Cache<ExpressionKey, ExpressionEvaluator> COMPILED_EXPRESSION_CACHE =
            CacheBuilder.newBuilder()
                    // estimated maximum planning/startup time
                    .expireAfterAccess(Duration.ofMinutes(5))
                    // estimated cache size
                    .maximumSize(100)
                    .softValues()
                    .build();

    /** Triggers internal garbage collection of expired cache entries. */
    public static void cleanUp() {
        COMPILED_CLASS_CACHE.cleanUp();
        COMPILED_EXPRESSION_CACHE.cleanUp();
    }

    /**
     * Compiles a generated code to a Class.
     *
     * @param cl the ClassLoader used to load the class
     * @param name the class name
     * @param code the generated code
     * @param <T> the class type
     * @return the compiled class
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> compile(ClassLoader cl, String name, String code) {
        try {
            // The class name is part of the "code" and makes the string unique,
            // to prevent class leaks we don't cache the class loader directly
            // but only its hash code
            final ClassKey classKey = new ClassKey(cl.hashCode(), code);
            return (Class<T>) COMPILED_CLASS_CACHE.get(classKey, () -> doCompile(cl, name, code));
        } catch (Exception e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    private static <T> Class<T> doCompile(ClassLoader cl, String name, String code) {
        checkNotNull(cl, "Classloader must not be null.");
        CODE_LOG.debug("Compiling: {} \n\n Code:\n{}", name, code);
        SimpleCompiler compiler = new SimpleCompiler();
        compiler.setParentClassLoader(cl);
        try {
            compiler.cook(code);
        } catch (Throwable t) {
            System.out.println(addLineNumber(code));
            throw new InvalidProgramException(
                    "Table program cannot be compiled. This is a bug. Please file an issue.", t);
        }
        try {
            //noinspection unchecked
            return (Class<T>) compiler.getClassLoader().loadClass(name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Can not load class " + name, e);
        }
    }

    /**
     * To output more information when an error occurs. Generally, when cook fails, it shows which
     * line is wrong. This line number starts at 1.
     */
    private static String addLineNumber(String code) {
        String[] lines = code.split("\n");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            builder.append("/* ").append(i + 1).append(" */").append(lines[i]).append("\n");
        }
        return builder.toString();
    }

    /**
     * Compiles an expression code to a janino {@link ExpressionEvaluator}.
     *
     * @param code the expression code
     * @param argumentNames the expression argument names
     * @param argumentClasses the expression argument classes
     * @param returnClass the return type of the expression
     * @return the compiled class
     */
    public static ExpressionEvaluator compileExpression(
            String code,
            List<String> argumentNames,
            List<Class<?>> argumentClasses,
            Class<?> returnClass) {
        try {
            ExpressionKey key =
                    new ExpressionKey(code, argumentNames, argumentClasses, returnClass);
            return COMPILED_EXPRESSION_CACHE.get(
                    key,
                    () -> {
                        ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator();
                        // Input args
                        expressionEvaluator.setParameters(
                                argumentNames.toArray(new String[0]),
                                argumentClasses.toArray(new Class[0]));
                        // Result type
                        expressionEvaluator.setExpressionType(returnClass);
                        try {
                            // Compile
                            expressionEvaluator.cook(code);
                        } catch (CompileException e) {
                            throw new InvalidProgramException(
                                    "Table program cannot be compiled. This is a bug. Please file an issue.\nExpression: "
                                            + code,
                                    e);
                        }
                        return expressionEvaluator;
                    });
        } catch (Exception e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    /** Class to use as key for the {@link #COMPILED_CLASS_CACHE}. */
    private static class ClassKey {
        private final int classLoaderId;
        private final String code;

        private ClassKey(int classLoaderId, String code) {
            this.classLoaderId = classLoaderId;
            this.code = code;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClassKey classKey = (ClassKey) o;
            return classLoaderId == classKey.classLoaderId && code.equals(classKey.code);
        }

        @Override
        public int hashCode() {
            return Objects.hash(classLoaderId, code);
        }
    }

    /** Class to use as key for the {@link #COMPILED_EXPRESSION_CACHE}. */
    private static class ExpressionKey {
        private final String code;
        private final List<String> argumentNames;
        private final List<Class<?>> argumentClasses;
        private final Class<?> returnClass;

        private ExpressionKey(
                String code,
                List<String> argumentNames,
                List<Class<?>> argumentClasses,
                Class<?> returnClass) {
            this.code = code;
            this.argumentNames = argumentNames;
            this.argumentClasses = argumentClasses;
            this.returnClass = returnClass;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExpressionKey that = (ExpressionKey) o;
            return code.equals(that.code)
                    && argumentNames.equals(that.argumentNames)
                    && argumentClasses.equals(that.argumentClasses)
                    && returnClass.equals(that.returnClass);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, argumentNames, argumentClasses, returnClass);
        }
    }
}
