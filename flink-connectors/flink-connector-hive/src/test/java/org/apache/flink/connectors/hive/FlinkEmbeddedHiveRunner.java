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

package org.apache.flink.connectors.hive;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.io.Resources;

import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.HiveShellContainer;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import com.klarna.reflection.ReflectionUtils;
import org.junit.Ignore;
import org.junit.internal.AssumptionViolatedException;
import org.junit.internal.runners.model.EachTestNotifier;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.reflections.ReflectionUtils.withAnnotation;

/**
 * JUnit 4 runner that runs hive sql on a HiveServer residing in this JVM. No external dependencies
 * needed. Inspired by StandaloneHiveRunner.java (almost copied).
 */
public class FlinkEmbeddedHiveRunner extends BlockJUnit4ClassRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEmbeddedHiveRunner.class);
    private HiveShellContainer container;
    private final HiveRunnerConfig config = new HiveRunnerConfig();
    protected HiveServerContext context;

    public FlinkEmbeddedHiveRunner(Class<?> clazz) throws InitializationError {
        super(clazz);
    }

    @Override
    protected List<TestRule> classRules() {
        // need to load hive runner config before the context is inited
        loadAnnotatesHiveRunnerConfig(getTestClass().getJavaClass());
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        context = new FlinkEmbeddedHiveServerContext(temporaryFolder, config);
        List<TestRule> rules = super.classRules();
        ExternalResource hiveShell =
                new ExternalResource() {
                    @Override
                    protected void before() throws Throwable {
                        container =
                                createHiveServerContainer(getTestClass().getJavaClass(), context);
                    }

                    @Override
                    protected void after() {
                        tearDown();
                    }
                };
        rules.add(hiveShell);
        rules.add(temporaryFolder);
        return rules;
    }

    @Override
    protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
        Description description = describeChild(method);
        if (method.getAnnotation(Ignore.class) != null) {
            notifier.fireTestIgnored(description);
        } else {
            EachTestNotifier eachNotifier = new EachTestNotifier(notifier, description);
            eachNotifier.fireTestStarted();
            try {
                runTestMethod(method, eachNotifier);
            } finally {
                eachNotifier.fireTestFinished();
            }
        }
    }

    /** Runs a {@link Statement} that represents a leaf (aka atomic) test. */
    private void runTestMethod(FrameworkMethod method, EachTestNotifier notifier) {
        Statement statement = methodBlock(method);

        try {
            statement.evaluate();
        } catch (AssumptionViolatedException e) {
            notifier.addFailedAssumption(e);
        } catch (Throwable e) {
            notifier.addFailure(e);
        }
    }

    private void tearDown() {
        if (container != null) {
            LOGGER.info("Tearing down {}", getName());
            try {
                container.tearDown();
            } catch (Throwable e) {
                LOGGER.warn("Tear down failed: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Traverses the test class annotations. Will inject a HiveShell in the test case that envelopes
     * the HiveServer.
     */
    private HiveShellContainer createHiveServerContainer(
            final Class testClass, HiveServerContext context) throws Exception {

        context.init();

        final HiveServerContainer hiveServerContainer = new HiveServerContainer(context);

        HiveShellBuilder hiveShellBuilder = new HiveShellBuilder();
        HiveRunnerShim hiveRunnerShim = HiveRunnerShimLoader.load();
        hiveRunnerShim.setCommandShellEmulation(hiveShellBuilder, config);

        HiveShellField shellSetter = loadScriptsUnderTest(testClass, hiveShellBuilder);

        hiveShellBuilder.setHiveServerContainer(hiveServerContainer);

        loadAnnotatedResources(testClass, hiveShellBuilder);

        loadAnnotatedProperties(testClass, hiveShellBuilder);

        loadAnnotatedSetupScripts(testClass, hiveShellBuilder);

        // Build shell
        final HiveShellContainer shell = hiveShellBuilder.buildShell();

        // Set shell
        shellSetter.setShell(shell);

        if (shellSetter.isAutoStart()) {
            shell.start();
        }

        return shell;
    }

    private void loadAnnotatesHiveRunnerConfig(Class testClass) {
        Set<Field> fields =
                ReflectionUtils.getAllFields(testClass, withAnnotation(HiveRunnerSetup.class));
        Preconditions.checkState(
                fields.size() <= 1,
                "Exact one field of type HiveRunnerConfig should to be annotated with @HiveRunnerSetup");

        // Override the config with test case config. Taking care to not replace the config instance
        // since it
        // has been passes around and referenced by some of the other test rules.
        if (!fields.isEmpty()) {
            Field field = fields.iterator().next();
            Preconditions.checkState(
                    ReflectionUtils.isOfType(field, HiveRunnerConfig.class),
                    "Field annotated with @HiveRunnerSetup should be of type HiveRunnerConfig");
            config.override(
                    ReflectionUtils.getStaticFieldValue(
                            testClass, field.getName(), HiveRunnerConfig.class));
        }
    }

    private HiveShellField loadScriptsUnderTest(
            final Class testClass, HiveShellBuilder hiveShellBuilder) {
        try {
            Set<Field> fields =
                    ReflectionUtils.getAllFields(testClass, withAnnotation(HiveSQL.class));

            Preconditions.checkState(
                    fields.size() == 1, "Exactly one field should to be annotated with @HiveSQL");

            final Field field = fields.iterator().next();
            List<Path> scripts = new ArrayList<>();
            HiveSQL annotation = field.getAnnotation(HiveSQL.class);
            for (String scriptFilePath : annotation.files()) {
                Path file = Paths.get(Resources.getResource(scriptFilePath).toURI());
                Preconditions.checkState(Files.exists(file), "File " + file + " does not exist");
                scripts.add(file);
            }

            Charset charset =
                    annotation.encoding().equals("")
                            ? Charset.defaultCharset()
                            : Charset.forName(annotation.encoding());

            final boolean isAutoStart = annotation.autoStart();

            hiveShellBuilder.setScriptsUnderTest(scripts, charset);

            return new HiveShellField() {
                @Override
                public void setShell(HiveShell shell) {
                    ReflectionUtils.setStaticField(testClass, field.getName(), shell);
                }

                @Override
                public boolean isAutoStart() {
                    return isAutoStart;
                }
            };
        } catch (Throwable t) {
            throw new IllegalArgumentException(
                    "Failed to init field annotated with @HiveSQL: " + t.getMessage(), t);
        }
    }

    private void loadAnnotatedSetupScripts(Class testClass, HiveShellBuilder hiveShellBuilder) {
        Set<Field> setupScriptFields =
                ReflectionUtils.getAllFields(testClass, withAnnotation(HiveSetupScript.class));
        for (Field setupScriptField : setupScriptFields) {
            if (ReflectionUtils.isOfType(setupScriptField, String.class)) {
                String script =
                        ReflectionUtils.getStaticFieldValue(
                                testClass, setupScriptField.getName(), String.class);
                hiveShellBuilder.addSetupScript(script);
            } else if (ReflectionUtils.isOfType(setupScriptField, File.class)
                    || ReflectionUtils.isOfType(setupScriptField, Path.class)) {
                Path path = getMandatoryPathFromField(testClass, setupScriptField);
                hiveShellBuilder.addSetupScript(readAll(path));
            } else {
                throw new IllegalArgumentException(
                        "Field annotated with @HiveSetupScript currently only supports type String, File and Path");
            }
        }
    }

    private static String readAll(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read " + path + ": " + e.getMessage(), e);
        }
    }

    private void loadAnnotatedResources(Class testClass, HiveShellBuilder workFlowBuilder)
            throws IOException {
        Set<Field> fields =
                ReflectionUtils.getAllFields(testClass, withAnnotation(HiveResource.class));

        for (Field resourceField : fields) {

            HiveResource annotation = resourceField.getAnnotation(HiveResource.class);
            String targetFile = annotation.targetFile();

            if (ReflectionUtils.isOfType(resourceField, String.class)) {
                String data =
                        ReflectionUtils.getStaticFieldValue(
                                testClass, resourceField.getName(), String.class);
                workFlowBuilder.addResource(targetFile, data);
            } else if (ReflectionUtils.isOfType(resourceField, File.class)
                    || ReflectionUtils.isOfType(resourceField, Path.class)) {
                Path dataFile = getMandatoryPathFromField(testClass, resourceField);
                workFlowBuilder.addResource(targetFile, dataFile);
            } else {
                throw new IllegalArgumentException(
                        "Fields annotated with @HiveResource currently only supports field type String, File or Path");
            }
        }
    }

    private Path getMandatoryPathFromField(Class testClass, Field resourceField) {
        Path path;
        if (ReflectionUtils.isOfType(resourceField, File.class)) {
            File dataFile =
                    ReflectionUtils.getStaticFieldValue(
                            testClass, resourceField.getName(), File.class);
            path = Paths.get(dataFile.toURI());
        } else if (ReflectionUtils.isOfType(resourceField, Path.class)) {
            path =
                    ReflectionUtils.getStaticFieldValue(
                            testClass, resourceField.getName(), Path.class);
        } else {
            throw new IllegalArgumentException(
                    "Only Path or File type is allowed on annotated field " + resourceField);
        }

        Preconditions.checkArgument(Files.exists(path), "File %s does not exist", path);
        return path;
    }

    private void loadAnnotatedProperties(Class testClass, HiveShellBuilder workFlowBuilder) {
        for (Field hivePropertyField :
                ReflectionUtils.getAllFields(testClass, withAnnotation(HiveProperties.class))) {
            Preconditions.checkState(
                    ReflectionUtils.isOfType(hivePropertyField, Map.class),
                    "Field annotated with @HiveProperties should be of type Map<String, String>");
            workFlowBuilder.putAllProperties(
                    ReflectionUtils.getStaticFieldValue(
                            testClass, hivePropertyField.getName(), Map.class));
        }
    }

    /**
     * Used as a handle for the HiveShell field in the test case so that we may set it once the
     * HiveShell has been instantiated.
     */
    interface HiveShellField {
        void setShell(HiveShell shell);

        boolean isAutoStart();
    }
}
