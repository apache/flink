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

package org.apache.flink.test.migration;

import org.apache.flink.FlinkVersion;
import org.apache.flink.test.util.MigrationTest;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.rules.RunRules;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** A utility class to execute generateSnapshots method for migration tests. */
class SnapshotGeneratorUtils {

    static void executeGenerate(Class<?> migrationTestClass, FlinkVersion flinkVersion)
            throws Throwable {
        for (Method method : migrationTestClass.getMethods()) {
            if (method.isAnnotationPresent(MigrationTest.SnapshotsGenerator.class)) {
                executeGenerateMethods(migrationTestClass, method, flinkVersion);
            } else if (method.isAnnotationPresent(
                    MigrationTest.ParameterizedSnapshotsGenerator.class)) {
                String parametersMethodName =
                        method.getAnnotation(MigrationTest.ParameterizedSnapshotsGenerator.class)
                                .value();
                Method parametersMethod =
                        migrationTestClass.getMethod(parametersMethodName, FlinkVersion.class);
                executeParameterizedGenerateMethods(
                        migrationTestClass, method, parametersMethod, flinkVersion);
            }
        }
    }

    private static void executeGenerateMethods(
            Class<?> migrationTestClass, Method method, FlinkVersion version) throws Throwable {
        method.setAccessible(true);
        List<TestRule> classRules = getRuleFields(migrationTestClass, ClassRule.class, null);

        executeWithRules(
                classRules,
                new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        Object migrationTest = createMigrationTest(migrationTestClass);
                        List<TestRule> rules =
                                getRuleFields(migrationTestClass, Rule.class, migrationTest);
                        executeWithRules(
                                rules,
                                new Statement() {
                                    @Override
                                    public void evaluate() throws Throwable {
                                        method.invoke(migrationTest, version);
                                    }
                                });
                    }
                });
    }

    private static void executeParameterizedGenerateMethods(
            Class<?> migrationTestClass,
            Method method,
            Method parametersMethod,
            FlinkVersion version)
            throws Throwable {
        method.setAccessible(true);
        parametersMethod.setAccessible(true);
        List<TestRule> classRules = getRuleFields(migrationTestClass, ClassRule.class, null);

        executeWithRules(
                classRules,
                new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        Object migrationTest = createMigrationTest(migrationTestClass);
                        List<TestRule> rules =
                                getRuleFields(migrationTestClass, Rule.class, migrationTest);
                        Collection<?> arguments =
                                (Collection<?>) parametersMethod.invoke(migrationTest, version);
                        for (Object argument : arguments) {
                            executeWithRules(
                                    rules,
                                    new Statement() {
                                        @Override
                                        public void evaluate() throws Throwable {
                                            method.invoke(migrationTest, argument);
                                        }
                                    });
                        }
                    }
                });
    }

    private static void executeWithRules(List<TestRule> rules, Statement statement)
            throws Throwable {
        new RunRules(statement, rules, Description.EMPTY).evaluate();
    }

    private static List<TestRule> getRuleFields(
            Class<?> migrationTestClass, Class<? extends Annotation> tagClass, Object migrationTest)
            throws IllegalAccessException {
        List<TestRule> rules = new ArrayList<>();

        Field[] fields = migrationTestClass.getFields();
        for (Field field : fields) {
            if (ExternalResource.class.isAssignableFrom(field.getType())
                    && field.isAnnotationPresent(tagClass)) {
                field.setAccessible(true);
                rules.add((ExternalResource) field.get(migrationTest));
            }
        }

        return rules;
    }

    private static Object createMigrationTest(Class<?> migrationTestClass) throws Exception {
        Constructor<?>[] constructors = migrationTestClass.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getParameterCount() == 0) {
                constructor.setAccessible(true);
                return constructor.newInstance();
            }
        }

        // This class does not have a constructor without argument.
        Constructor<?> constructor = constructors[0];
        constructor.setAccessible(true);

        // Check if we could find method labeled with @Parameterized.Parameters
        for (Method method : migrationTestClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())
                    && method.isAnnotationPresent(Parameterized.Parameters.class)) {
                Object argumentLists = method.invoke(null);
                if (argumentLists instanceof Collection) {
                    return constructor.newInstance(
                            ((Collection<?>) argumentLists).iterator().next());
                } else if (argumentLists.getClass().isArray()) {
                    return constructor.newInstance(Array.get(argumentLists, 0));
                } else {
                    throw new RuntimeException(
                            "Failed to create parameterized class object due to argument lists type not supported: "
                                    + argumentLists.getClass());
                }
            }
        }

        throw new RuntimeException(
                "Could not create the object for "
                        + migrationTestClass
                        + ": No default constructor or @Parameterized.Parameters method found.");
    }
}
