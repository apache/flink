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
    protected static final Cache<String, Cache<ClassLoader, Class>> COMPILED_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(100) // estimated cache size
                    .build();

    protected static final Cache<ExpressionEntry, ExpressionEvaluator> COMPILED_EXPRESSION_CACHE =
            CacheBuilder.newBuilder()
                    .maximumSize(100) // estimated cache size
                    .build();

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
            Cache<ClassLoader, Class> compiledClasses =
                    COMPILED_CACHE.get(
                            // "code" as a key should be sufficient as the class name
                            // is part of the Java code
                            code,
                            () ->
                                    CacheBuilder.newBuilder()
                                            .maximumSize(5)
                                            .weakKeys()
                                            .softValues()
                                            .build());
            return compiledClasses.get(cl, () -> doCompile(cl, name, code));
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
            ExpressionEntry key =
                    new ExpressionEntry(code, argumentNames, argumentClasses, returnClass);
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

    /** Class to use as key for the {@link #COMPILED_EXPRESSION_CACHE}. */
    private static class ExpressionEntry {
        private final String code;
        private final List<String> argumentNames;
        private final List<Class<?>> argumentClasses;
        private final Class<?> returnClass;

        private ExpressionEntry(
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
            ExpressionEntry that = (ExpressionEntry) o;
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
