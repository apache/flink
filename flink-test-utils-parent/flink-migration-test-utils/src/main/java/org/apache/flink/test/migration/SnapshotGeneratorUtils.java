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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A utility class to execute generateSnapshots method for migration tests. */
class SnapshotGeneratorUtils {

    static void executeGenerate(Class<?> migrationTestClass, FlinkVersion flinkVersion)
            throws Throwable {
        for (Method method : findGeneratorMethods(migrationTestClass)) {
            if (method.isAnnotationPresent(MigrationTest.SnapshotsGenerator.class)) {
                executeGenerateMethods(migrationTestClass, method, flinkVersion);
            } else if (method.isAnnotationPresent(
                    MigrationTest.ParameterizedSnapshotsGenerator.class)) {
                String parametersMethodName =
                        method.getAnnotation(MigrationTest.ParameterizedSnapshotsGenerator.class)
                                .value();
                Method parametersMethod =
                        findDeclaredMethod(
                                migrationTestClass, parametersMethodName, FlinkVersion.class);
                executeParameterizedGenerateMethods(
                        migrationTestClass, method, parametersMethod, flinkVersion);
            }
        }
    }

    /**
     * Finds all methods annotated with {@link MigrationTest.SnapshotsGenerator} or {@link
     * MigrationTest.ParameterizedSnapshotsGenerator} declared by the test class or any of its super
     * classes. Unlike {@link Class#getMethods()}, this also discovers non-public generator methods.
     */
    private static List<Method> findGeneratorMethods(Class<?> migrationTestClass) {
        List<Method> methods = new ArrayList<>();
        Set<String> visitedSignatures = new HashSet<>();
        for (Class<?> clazz = migrationTestClass;
                clazz != null && clazz != Object.class;
                clazz = clazz.getSuperclass()) {
            for (Method method : clazz.getDeclaredMethods()) {
                if (method.isAnnotationPresent(MigrationTest.SnapshotsGenerator.class)
                        || method.isAnnotationPresent(
                                MigrationTest.ParameterizedSnapshotsGenerator.class)) {
                    String signature =
                            method.getName() + Arrays.toString(method.getParameterTypes());
                    if (visitedSignatures.add(signature)) {
                        methods.add(method);
                    }
                }
            }
        }
        return methods;
    }

    /**
     * Finds a method by name and parameter types declared by the test class or any of its super
     * classes. Unlike {@link Class#getMethod(String, Class[])}, this also discovers non-public
     * methods, such as the {@code parameters} method referenced by {@link
     * MigrationTest.ParameterizedSnapshotsGenerator}.
     */
    private static Method findDeclaredMethod(
            Class<?> migrationTestClass, String name, Class<?>... parameterTypes)
            throws NoSuchMethodException {
        for (Class<?> clazz = migrationTestClass;
                clazz != null && clazz != Object.class;
                clazz = clazz.getSuperclass()) {
            try {
                return clazz.getDeclaredMethod(name, parameterTypes);
            } catch (NoSuchMethodException ignored) {
                // Continue searching up the class hierarchy.
            }
        }
        throw new NoSuchMethodException(
                "Could not find method "
                        + name
                        + Arrays.toString(parameterTypes)
                        + " in "
                        + migrationTestClass
                        + " or any of its super classes.");
    }

    private static void executeGenerateMethods(
            Class<?> migrationTestClass, Method method, FlinkVersion version) throws Throwable {
        method.setAccessible(true);
        executeWithLifecycle(
                migrationTestClass,
                (migrationTest, invocation) ->
                        invocation.run(() -> invoke(method, migrationTest, version)));
    }

    private static void executeParameterizedGenerateMethods(
            Class<?> migrationTestClass,
            Method method,
            Method parametersMethod,
            FlinkVersion version)
            throws Throwable {
        method.setAccessible(true);
        parametersMethod.setAccessible(true);
        executeWithLifecycle(
                migrationTestClass,
                (migrationTest, invocation) -> {
                    Collection<?> arguments =
                            (Collection<?>) invoke(parametersMethod, migrationTest, version);
                    for (Object argument : arguments) {
                        invocation.run(() -> invoke(method, migrationTest, argument));
                    }
                });
    }

    /**
     * Drives the migration test's lifecycle around the given {@code action}. Both the JUnit 4
     * {@code @ClassRule}/{@code @Rule} {@link ExternalResource}s and the JUnit 5 lifecycle (see
     * {@link JUnit5TestEnvironment}) are set up, so that migration tests using either framework
     * have their resources initialized. The JUnit 5 steps are no-ops for tests that do not use any
     * of the supported JUnit 5 constructs, leaving pure JUnit 4 tests unaffected.
     *
     * <p>Class-level setup ({@code @ClassRule}s, {@code beforeAll}) and the test instance are
     * created once. The {@code action} then triggers one or more generator method invocations
     * through the provided {@link Invocation}, and each invocation is wrapped in its own instance
     * lifecycle ({@code @Rule}s, {@code beforeEach}/{@code afterEach}), so that a fresh set of
     * per-test resources (such as a {@code MiniCluster}) is provided for every generated snapshot.
     */
    private static void executeWithLifecycle(Class<?> migrationTestClass, GenerateAction action)
            throws Throwable {
        List<TestRule> classRules = getRuleFields(migrationTestClass, ClassRule.class, null);
        executeWithRules(
                classRules,
                new Statement() {
                    @Override
                    public void evaluate() throws Throwable {
                        JUnit5TestEnvironment environment =
                                new JUnit5TestEnvironment(migrationTestClass);
                        // beforeAll must run before the instance is created, as instance field
                        // initializers may reference resources set up by statically registered
                        // extensions.
                        environment.beforeAll();
                        try {
                            Object migrationTest = createMigrationTest(migrationTestClass);
                            environment.initializeTestInstance(migrationTest);
                            List<TestRule> rules =
                                    getRuleFields(migrationTestClass, Rule.class, migrationTest);
                            Invocation invocation =
                                    generatorCall ->
                                            executeWithRules(
                                                    rules,
                                                    new Statement() {
                                                        @Override
                                                        public void evaluate() throws Throwable {
                                                            environment.beforeEach(migrationTest);
                                                            try {
                                                                generatorCall.run();
                                                            } finally {
                                                                environment.afterEach(
                                                                        migrationTest);
                                                            }
                                                        }
                                                    });
                            action.execute(migrationTest, invocation);
                        } finally {
                            environment.afterAll();
                        }
                    }
                });
    }

    private static void executeWithRules(List<TestRule> rules, Statement statement)
            throws Throwable {
        new RunRules(statement, rules, Description.EMPTY).evaluate();
    }

    private static Object invoke(Method method, Object target, Object... args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    /**
     * Generates snapshots using a migration test instance. Every generator method invocation must
     * be triggered through the given {@link Invocation} so that it runs within its own instance
     * lifecycle.
     */
    @FunctionalInterface
    private interface GenerateAction {
        void execute(Object migrationTest, Invocation invocation) throws Throwable;
    }

    /** Runs a single generator method invocation within a fresh instance lifecycle. */
    @FunctionalInterface
    private interface Invocation {
        void run(GeneratorCall generatorCall) throws Throwable;
    }

    /** A single generator method invocation. */
    @FunctionalInterface
    private interface GeneratorCall {
        void run() throws Throwable;
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

        // Check if we could find method labeled with @Parameterized.Parameters or @Parameters
        for (Method method : migrationTestClass.getMethods()) {
            if (Modifier.isStatic(method.getModifiers())
                    && (method.isAnnotationPresent(Parameterized.Parameters.class)
                            || method.isAnnotationPresent(Parameters.class))) {
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
                        + ": No default constructor or @Parameterized.Parameters or @Parameters method found.");
    }
}
