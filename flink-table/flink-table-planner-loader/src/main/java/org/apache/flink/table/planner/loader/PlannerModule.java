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

package org.apache.flink.table.planner.loader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.core.classloading.SubmoduleClassLoader;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Module holder that loads the flink-table-planner module in a separate classpath.
 *
 * <p>This loader expects the flink-table-planner jar to be accessible via {@link
 * ClassLoader#getResource(String)}. It will extract the jar into a temporary directory and create a
 * new {@link SubmoduleClassLoader} to load the various planner factories from that jar.
 */
class PlannerModule {

    /**
     * The name of the table planner dependency jar, bundled with flink-table-planner-loader module
     * artifact.
     */
    static final String FLINK_TABLE_PLANNER_FAT_JAR = "flink-table-planner.jar";

    private static final String HINT_USAGE =
            "mvn clean package -pl flink-table/flink-table-planner,flink-table/flink-table-planner-loader -DskipTests";

    private static final String[] OWNER_CLASSPATH =
            Stream.concat(
                            Arrays.stream(CoreOptions.PARENT_FIRST_LOGGING_PATTERNS),
                            Stream.of(
                                    // These packages are shipped either by
                                    // flink-table-runtime or flink-dist itself
                                    "org.codehaus.janino",
                                    "org.codehaus.commons",
                                    "org.apache.commons.lang3",
                                    "org.apache.commons.math3",
                                    // with hive dialect, hadoop jar should be in classpath,
                                    // also, we should make it loaded by owner classloader,
                                    // otherwise, it'll throw class not found exception
                                    // when initialize HiveParser which requires hadoop
                                    "org.apache.hadoop"))
                    .toArray(String[]::new);

    private static final String[] COMPONENT_CLASSPATH = new String[] {"org.apache.flink"};

    private static final Map<String, String> KNOWN_MODULE_ASSOCIATIONS = new HashMap<>();

    static {
        KNOWN_MODULE_ASSOCIATIONS.put("org.apache.flink.table.runtime", "flink-table-runtime");
        KNOWN_MODULE_ASSOCIATIONS.put("org.apache.flink.formats.raw", "flink-table-runtime");

        KNOWN_MODULE_ASSOCIATIONS.put("org.codehaus.janino", "flink-table-runtime");
        KNOWN_MODULE_ASSOCIATIONS.put("org.codehaus.commons", "flink-table-runtime");
        KNOWN_MODULE_ASSOCIATIONS.put(
                "org.apache.flink.table.shaded.com.jayway", "flink-table-runtime");
    }

    private final PlannerComponentClassLoader submoduleClassLoader;

    private PlannerModule() {
        try {
            final ClassLoader flinkClassLoader = PlannerModule.class.getClassLoader();

            final Path tmpDirectory =
                    Paths.get(ConfigurationUtils.parseTempDirectories(new Configuration())[0]);
            Files.createDirectories(FileUtils.getTargetPathIfContainsSymbolicPath(tmpDirectory));
            final Path tempFile =
                    Files.createFile(
                            tmpDirectory.resolve(
                                    "flink-table-planner_" + UUID.randomUUID() + ".jar"));

            final InputStream resourceStream =
                    flinkClassLoader.getResourceAsStream(FLINK_TABLE_PLANNER_FAT_JAR);
            if (resourceStream == null) {
                throw new TableException(
                        String.format(
                                "Flink Table planner could not be found. If this happened while running a test in the IDE, "
                                        + "run '%s' on the command-line, "
                                        + "or add a test dependency on the flink-table-planner-loader test-jar.",
                                HINT_USAGE));
            }

            IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));
            tempFile.toFile().deleteOnExit();

            this.submoduleClassLoader =
                    new PlannerComponentClassLoader(
                            new URL[] {tempFile.toUri().toURL()},
                            flinkClassLoader,
                            OWNER_CLASSPATH,
                            COMPONENT_CLASSPATH,
                            KNOWN_MODULE_ASSOCIATIONS);
        } catch (IOException e) {
            throw new TableException(
                    "Could not initialize the table planner components loader.", e);
        }
    }

    public void addUrlToClassLoader(URL url) {
        // add the url to component url
        this.submoduleClassLoader.addURL(url);
    }

    // Singleton lazy initialization

    private static class PlannerComponentsHolder {
        private static final PlannerModule INSTANCE = new PlannerModule();
    }

    public static PlannerModule getInstance() {
        return PlannerComponentsHolder.INSTANCE;
    }

    // load methods for various components provided by the planner

    public ExecutorFactory loadExecutorFactory() {
        return FactoryUtil.discoverFactory(
                this.submoduleClassLoader,
                ExecutorFactory.class,
                ExecutorFactory.DEFAULT_IDENTIFIER);
    }

    public PlannerFactory loadPlannerFactory() {
        return FactoryUtil.discoverFactory(
                this.submoduleClassLoader, PlannerFactory.class, PlannerFactory.DEFAULT_IDENTIFIER);
    }

    /**
     * A class loader extending {@link ComponentClassLoader} which overwrites method{@link #addURL}
     * to enable it can add url to component classloader.
     */
    private static class PlannerComponentClassLoader extends ComponentClassLoader {

        public PlannerComponentClassLoader(
                URL[] classpath,
                ClassLoader ownerClassLoader,
                String[] ownerFirstPackages,
                String[] componentFirstPackages,
                Map<String, String> knownPackagePrefixesModuleAssociation) {
            super(
                    classpath,
                    ownerClassLoader,
                    ownerFirstPackages,
                    componentFirstPackages,
                    knownPackagePrefixesModuleAssociation);
        }

        @Override
        public void addURL(URL url) {
            super.addURL(url);
        }
    }
}
