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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExecutableInvoker;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestInstances;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Drives the subset of the JUnit 5 lifecycle that the migration test data generation relies on.
 *
 * <p>The {@link MigrationTestsSnapshotGenerator} instantiates the migration test classes directly
 * via reflection and invokes their
 * {@code @SnapshotsGenerator}/{@code @ParameterizedSnapshotsGenerator} methods outside of any JUnit
 * engine. Migration tests that have been migrated to JUnit 5 therefore need their extensions and
 * lifecycle methods to be driven manually, otherwise resources such as the {@link
 * org.apache.flink.runtime.minicluster.MiniCluster} or the executor service backing the test would
 * never be set up.
 *
 * <p>This environment supports:
 *
 * <ul>
 *   <li>{@code @RegisterExtension} fields (static and instance) implementing the {@link
 *       BeforeAllCallback}, {@link BeforeEachCallback}, {@link AfterEachCallback}, {@link
 *       AfterAllCallback} and {@link ParameterResolver} extension points,
 *   <li>{@code @BeforeEach}/{@code @AfterEach} lifecycle methods, including parameter injection
 *       (for example {@link org.apache.flink.test.junit5.InjectClusterClient}) through the
 *       registered {@link ParameterResolver}s, and
 *   <li>{@code @TempDir} fields.
 * </ul>
 *
 * <p>Class-level extensions registered via {@code @ExtendWith} are intentionally not driven: they
 * are concerned with the test runner itself (parameterization of {@code @TestTemplate} methods,
 * logging) and are not required to set up the resources used during snapshot generation.
 *
 * <p>The lifecycle is driven so that {@link #beforeAll()} runs before the test instance is created,
 * matching JUnit 5 semantics and, crucially, allowing instance field initializers to reference
 * resources set up by statically registered extensions.
 */
class JUnit5TestEnvironment {

    private final Class<?> testClass;
    private final GeneratorExtensionContext extensionContext;
    private final List<Object> staticExtensions = new ArrayList<>();
    private final List<Object> instanceExtensions = new ArrayList<>();
    private final List<Path> temporaryDirectories = new ArrayList<>();

    JUnit5TestEnvironment(Class<?> testClass) {
        this.testClass = testClass;
        this.extensionContext = new GeneratorExtensionContext(testClass);
    }

    /**
     * Runs the {@code beforeAll} callbacks of statically registered extensions. Must be called
     * before the test instance is created, because instance field initializers may already
     * reference resources set up by these callbacks (for example the executor service used to build
     * a {@link org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter}).
     */
    void beforeAll() throws Exception {
        collectRegisteredExtensions(null, staticExtensions);
        for (Object extension : staticExtensions) {
            if (extension instanceof BeforeAllCallback) {
                ((BeforeAllCallback) extension).beforeAll(extensionContext);
            }
        }
    }

    /** Collects instance-level extensions and injects {@code @TempDir} fields. */
    void initializeTestInstance(Object testInstance) throws Exception {
        extensionContext.setTestInstance(testInstance);
        collectRegisteredExtensions(testInstance, instanceExtensions);
        injectTemporaryDirectories(testInstance);
    }

    /** Runs {@code beforeEach} callbacks followed by the {@code @BeforeEach} methods. */
    void beforeEach(Object testInstance) throws Throwable {
        for (Object extension : allExtensions()) {
            if (extension instanceof BeforeEachCallback) {
                ((BeforeEachCallback) extension).beforeEach(extensionContext);
            }
        }
        for (Method method : lifecycleMethods(BeforeEach.class, true)) {
            invokeLifecycleMethod(testInstance, method);
        }
    }

    /** Runs the {@code @AfterEach} methods followed by the {@code afterEach} callbacks. */
    void afterEach(Object testInstance) throws Throwable {
        for (Method method : lifecycleMethods(AfterEach.class, false)) {
            invokeLifecycleMethod(testInstance, method);
        }
        for (Object extension : reversed(allExtensions())) {
            if (extension instanceof AfterEachCallback) {
                ((AfterEachCallback) extension).afterEach(extensionContext);
            }
        }
    }

    /**
     * Runs the {@code afterAll} callbacks of statically registered extensions, closes the stored
     * {@link ExtensionContext.Store.CloseableResource}s and removes the temporary directories.
     */
    void afterAll() throws Exception {
        for (Object extension : reversed(staticExtensions)) {
            if (extension instanceof AfterAllCallback) {
                ((AfterAllCallback) extension).afterAll(extensionContext);
            }
        }
        extensionContext.close();
        deleteTemporaryDirectories();
    }

    private List<Object> allExtensions() {
        List<Object> all = new ArrayList<>(staticExtensions);
        all.addAll(instanceExtensions);
        return all;
    }

    private static List<Object> reversed(List<Object> list) {
        List<Object> reversed = new ArrayList<>(list);
        Collections.reverse(reversed);
        return reversed;
    }

    private void collectRegisteredExtensions(Object instance, List<Object> target)
            throws IllegalAccessException {
        // Iterate the hierarchy from the topmost ancestor down to the test class, so that
        // extensions declared in super classes are set up before those of sub classes.
        for (Class<?> clazz : classHierarchy(testClass)) {
            for (Field field : clazz.getDeclaredFields()) {
                if (!field.isAnnotationPresent(RegisterExtension.class)) {
                    continue;
                }
                boolean isStatic = Modifier.isStatic(field.getModifiers());
                // When looking up static extensions no instance is available yet; when looking up
                // instance extensions we skip the static ones as they were already collected.
                if (isStatic != (instance == null)) {
                    continue;
                }
                field.setAccessible(true);
                Object value = field.get(instance);
                if (value != null) {
                    target.add(value);
                }
            }
        }
    }

    private void injectTemporaryDirectories(Object instance)
            throws IOException, IllegalAccessException {
        for (Class<?> clazz : classHierarchy(testClass)) {
            for (Field field : clazz.getDeclaredFields()) {
                if (!field.isAnnotationPresent(TempDir.class)) {
                    continue;
                }
                Path directory = Files.createTempDirectory("migration-test-generator");
                temporaryDirectories.add(directory);
                field.setAccessible(true);
                Class<?> type = field.getType();
                if (Path.class.equals(type)) {
                    field.set(instance, directory);
                } else if (File.class.equals(type)) {
                    field.set(instance, directory.toFile());
                } else {
                    throw new IllegalStateException(
                            "Unsupported @TempDir field type "
                                    + type.getName()
                                    + " on field "
                                    + field
                                    + ". Only java.nio.file.Path and java.io.File are supported.");
                }
            }
        }
    }

    private void deleteTemporaryDirectories() {
        for (Path directory : temporaryDirectories) {
            try (Stream<Path> paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(JUnit5TestEnvironment::deleteQuietly);
            } catch (IOException ignored) {
                // Best effort clean up of temporary directories.
            }
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // Best effort clean up of temporary directories.
        }
    }

    private List<Method> lifecycleMethods(
            Class<? extends Annotation> annotation, boolean superClassFirst) {
        List<Class<?>> hierarchy = classHierarchy(testClass);
        if (!superClassFirst) {
            Collections.reverse(hierarchy);
        }
        List<Method> methods = new ArrayList<>();
        Set<String> visitedSignatures = new HashSet<>();
        for (Class<?> clazz : hierarchy) {
            for (Method method : clazz.getDeclaredMethods()) {
                if (!method.isAnnotationPresent(annotation)) {
                    continue;
                }
                String signature = method.getName() + Arrays.toString(method.getParameterTypes());
                if (visitedSignatures.add(signature)) {
                    methods.add(method);
                }
            }
        }
        return methods;
    }

    private void invokeLifecycleMethod(Object testInstance, Method method) throws Throwable {
        method.setAccessible(true);
        Object[] arguments = resolveParameters(testInstance, method);
        try {
            method.invoke(testInstance, arguments);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private Object[] resolveParameters(Object testInstance, Method method) {
        Parameter[] parameters = method.getParameters();
        Object[] arguments = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            GeneratorParameterContext parameterContext =
                    new GeneratorParameterContext(parameters[i], i, Optional.of(testInstance));
            arguments[i] = resolveParameter(parameterContext);
        }
        return arguments;
    }

    private Object resolveParameter(GeneratorParameterContext parameterContext) {
        for (Object extension : allExtensions()) {
            if (extension instanceof ParameterResolver) {
                ParameterResolver resolver = (ParameterResolver) extension;
                if (resolver.supportsParameter(parameterContext, extensionContext)) {
                    return resolver.resolveParameter(parameterContext, extensionContext);
                }
            }
        }
        throw new ParameterResolutionException(
                "No registered extension can resolve parameter ["
                        + parameterContext.getParameter()
                        + "] of ["
                        + parameterContext.getParameter().getDeclaringExecutable()
                        + "].");
    }

    private static List<Class<?>> classHierarchy(Class<?> testClass) {
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> clazz = testClass;
                clazz != null && clazz != Object.class;
                clazz = clazz.getSuperclass()) {
            hierarchy.add(clazz);
        }
        // Topmost ancestor first.
        Collections.reverse(hierarchy);
        return hierarchy;
    }

    /**
     * A minimal {@link ExtensionContext} that provides just enough behavior for the extensions used
     * by the migration tests, most importantly a working {@link ExtensionContext.Store}.
     */
    private static final class GeneratorExtensionContext implements ExtensionContext {

        private final Class<?> testClass;
        private final Map<Namespace, GeneratorStore> stores = new ConcurrentHashMap<>();
        private Object testInstance;

        private GeneratorExtensionContext(Class<?> testClass) {
            this.testClass = testClass;
        }

        private void setTestInstance(Object testInstance) {
            this.testInstance = testInstance;
        }

        private void close() throws Exception {
            for (GeneratorStore store : stores.values()) {
                store.close();
            }
        }

        @Override
        public Optional<ExtensionContext> getParent() {
            return Optional.empty();
        }

        @Override
        public ExtensionContext getRoot() {
            return this;
        }

        @Override
        public String getUniqueId() {
            return testClass.getName();
        }

        @Override
        public String getDisplayName() {
            return testClass.getSimpleName();
        }

        @Override
        public Set<String> getTags() {
            return Collections.emptySet();
        }

        @Override
        public Optional<AnnotatedElement> getElement() {
            return Optional.of(testClass);
        }

        @Override
        public Optional<Class<?>> getTestClass() {
            return Optional.of(testClass);
        }

        @Override
        public Optional<TestInstance.Lifecycle> getTestInstanceLifecycle() {
            return Optional.empty();
        }

        @Override
        public Optional<Object> getTestInstance() {
            return Optional.ofNullable(testInstance);
        }

        @Override
        public Optional<TestInstances> getTestInstances() {
            return Optional.empty();
        }

        @Override
        public Optional<Method> getTestMethod() {
            return Optional.empty();
        }

        @Override
        public Optional<Throwable> getExecutionException() {
            return Optional.empty();
        }

        @Override
        public Optional<String> getConfigurationParameter(String key) {
            return Optional.empty();
        }

        @Override
        public <T> Optional<T> getConfigurationParameter(
                String key, Function<String, T> transformer) {
            return Optional.empty();
        }

        @Override
        public void publishReportEntry(Map<String, String> map) {
            // Report entries are not collected during snapshot generation.
        }

        @Override
        public Store getStore(Namespace namespace) {
            return stores.computeIfAbsent(namespace, ignored -> new GeneratorStore());
        }

        @Override
        public ExecutionMode getExecutionMode() {
            return ExecutionMode.SAME_THREAD;
        }

        @Override
        public ExecutableInvoker getExecutableInvoker() {
            throw new UnsupportedOperationException(
                    "ExecutableInvoker is not supported during migration test data generation.");
        }
    }

    /** A minimal {@link ExtensionContext.Store} backed by a map. */
    private static final class GeneratorStore implements ExtensionContext.Store {

        private final Map<Object, Object> values = new ConcurrentHashMap<>();

        @Override
        public Object get(Object key) {
            return values.get(key);
        }

        @Override
        public <V> V get(Object key, Class<V> requiredType) {
            return requiredType.cast(values.get(key));
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> Object getOrComputeIfAbsent(K key, Function<K, V> defaultCreator) {
            return values.computeIfAbsent(key, mapKey -> defaultCreator.apply((K) mapKey));
        }

        @Override
        public <K, V> V getOrComputeIfAbsent(
                K key, Function<K, V> defaultCreator, Class<V> requiredType) {
            return requiredType.cast(getOrComputeIfAbsent(key, defaultCreator));
        }

        @Override
        public void put(Object key, Object value) {
            values.put(key, value);
        }

        @Override
        public Object remove(Object key) {
            return values.remove(key);
        }

        @Override
        public <V> V remove(Object key, Class<V> requiredType) {
            return requiredType.cast(values.remove(key));
        }

        private void close() throws Exception {
            Throwable firstError = null;
            for (Object value : values.values()) {
                if (value instanceof CloseableResource) {
                    try {
                        ((CloseableResource) value).close();
                    } catch (Throwable t) {
                        if (firstError == null) {
                            firstError = t;
                        } else {
                            firstError.addSuppressed(t);
                        }
                    }
                }
            }
            if (firstError != null) {
                throw new Exception("Failed to close stored resources.", firstError);
            }
        }
    }

    /** A minimal {@link ParameterContext} wrapping a single {@link Parameter}. */
    private static final class GeneratorParameterContext implements ParameterContext {

        private final Parameter parameter;
        private final int index;
        private final Optional<Object> target;

        private GeneratorParameterContext(Parameter parameter, int index, Optional<Object> target) {
            this.parameter = parameter;
            this.index = index;
            this.target = target;
        }

        @Override
        public Parameter getParameter() {
            return parameter;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public Optional<Object> getTarget() {
            return target;
        }

        @Override
        public boolean isAnnotated(Class<? extends Annotation> annotationType) {
            return parameter.isAnnotationPresent(annotationType);
        }

        @Override
        public <A extends Annotation> Optional<A> findAnnotation(Class<A> annotationType) {
            return Optional.ofNullable(parameter.getAnnotation(annotationType));
        }

        @Override
        public <A extends Annotation> List<A> findRepeatableAnnotations(Class<A> annotationType) {
            return Arrays.asList(parameter.getAnnotationsByType(annotationType));
        }
    }
}
