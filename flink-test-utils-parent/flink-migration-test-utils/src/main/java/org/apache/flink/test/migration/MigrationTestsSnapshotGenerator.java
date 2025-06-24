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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class aims to generate the snapshots for all the state migration tests. A migration test
 * should implement {@link MigrationTest} interface and its name should match {@code
 * *(Test|ITCase)(.java|.scala)}. For scala tests, we also require the class name is the same with
 * the containing filename.
 */
public class MigrationTestsSnapshotGenerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(MigrationTestsSnapshotGenerator.class);

    private static final String DEFAULT_PATH_PREFIXES = "src/test/java,src/test/scala";

    private static final Option OPTION_HELP =
            Option.builder()
                    .longOpt("help")
                    .required(false)
                    .hasArg(false)
                    .desc("print this help information.")
                    .build();

    private static final Option OPTION_DIR =
            Option.builder()
                    .longOpt("dir")
                    .required()
                    .hasArg()
                    .desc("The root directory for scanning. Required.")
                    .build();

    private static final Option OPTION_VERSION =
            Option.builder()
                    .longOpt("version")
                    .required()
                    .hasArg()
                    .desc("The version to generate. Required.")
                    .build();

    private static final Option OPTION_PREFIXES =
            Option.builder()
                    .longOpt("prefixes")
                    .required(false)
                    .hasArg()
                    .desc(
                            "The prefix paths to scan under the root directory, separated by \",\". Default to \""
                                    + DEFAULT_PATH_PREFIXES
                                    + "\"")
                    .build();

    private static final Option OPTION_CLASSES =
            Option.builder()
                    .longOpt("classes")
                    .required(false)
                    .hasArg()
                    .desc(
                            "The specified qualified class name to generate test data, "
                                    + "separated by \",\". This option has a higher priority than the prefix option.")
                    .build();

    private static final Pattern VERSION_PATTERN = Pattern.compile("v?([0-9]+)[._]([0-9]+)");

    private static final String CLASS_NAME_GROUP = "className";
    private static final Pattern CLASS_NAME_PATTERN =
            Pattern.compile("(?<" + CLASS_NAME_GROUP + ">[a-zA-Z0-9]*(Test|ITCase))(.java|.scala)");

    public static void main(String[] args) throws Throwable {
        // Detect for --help first. We cannot directly use the command
        // line parser since it always detects the required options.
        for (String s : args) {
            if (s.equals("--" + OPTION_HELP.getLongOpt())) {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.setOptionComparator(null);
                helpFormatter.printHelp(
                        "java " + MigrationTestsSnapshotGenerator.class.getName(), createOptions());
                return;
            }
        }

        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        File rootDirectory = new File(commandLine.getOptionValue(OPTION_DIR));
        if (!rootDirectory.exists() || !rootDirectory.isDirectory()) {
            throw new FileNotFoundException(
                    rootDirectory + " does not exist or is not a directory.");
        }

        String versionName = commandLine.getOptionValue(OPTION_VERSION);
        Matcher versionMatcher = VERSION_PATTERN.matcher(versionName);
        if (!versionMatcher.matches()) {
            throw new IllegalArgumentException(
                    "Version "
                            + versionName
                            + "could not be parsed, "
                            + "please specify the version with format like 1.17, 1_17, v1_17, v1.17");
        }
        FlinkVersion version =
                FlinkVersion.valueOf(
                        Integer.parseInt(versionMatcher.group(1)),
                        Integer.parseInt(versionMatcher.group(2)));

        LOG.info("Start test data generating for module {} and version {}", rootDirectory, version);
        List<Class<?>> migrationTests;
        if (commandLine.hasOption(OPTION_CLASSES)) {
            migrationTests =
                    loadSpecifiedMigrationTests(
                            commandLine.getOptionValue(OPTION_CLASSES).split(",\\s*"));
        } else {
            String[] prefixes =
                    commandLine
                            .getOptionValue(OPTION_PREFIXES, DEFAULT_PATH_PREFIXES)
                            .split(",\\s*");
            migrationTests = findMigrationTests(rootDirectory.getAbsolutePath(), prefixes);
        }

        for (Class<?> migrationTestClass : migrationTests) {
            LOG.info("Start test data generating for {}", migrationTestClass.getName());
            SnapshotGeneratorUtils.executeGenerate(migrationTestClass, version);
            LOG.info("Finish test data generating for {}", migrationTestClass.getName());
        }
    }

    private static Options createOptions() {
        Options options = new Options();

        // The root directory for the scanning
        options.addOption(OPTION_HELP);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_VERSION);
        options.addOption(OPTION_PREFIXES);
        options.addOption(OPTION_CLASSES);
        return options;
    }

    private static List<Class<?>> loadSpecifiedMigrationTests(String[] specifiedClassNames)
            throws Exception {
        List<Class<?>> migrationTestClasses = new ArrayList<>();
        for (String name : specifiedClassNames) {
            try {
                Class<?> clazz = Class.forName(name);
                if (MigrationTest.class.isAssignableFrom(clazz)) {
                    migrationTestClasses.add(clazz);
                }
            } catch (ClassNotFoundException e) {
                LOG.warn("Class " + name + " does not exist.");
            }
        }
        return migrationTestClasses;
    }

    private static List<Class<?>> findMigrationTests(String rootDirectory, String[] prefixes)
            throws Exception {
        List<Class<?>> migrationTestClasses = new ArrayList<>();
        for (String prefix : prefixes) {
            recursivelyFindMigrationTests(rootDirectory, prefix, "", migrationTestClasses);
        }

        return migrationTestClasses;
    }

    private static void recursivelyFindMigrationTests(
            String rootDirectory, String prefix, String packageName, List<Class<?>> result)
            throws Exception {
        File codeDirectory = new File(rootDirectory + File.separator + prefix);
        if (!codeDirectory.exists()) {
            LOG.debug("{} doesn't exist and will be skipped.", codeDirectory);
            return;
        }

        // Search all the directories
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(codeDirectory.toPath())) {
            for (Path entry : stream) {
                File file = entry.toFile();
                if (file.isDirectory()) {
                    recursivelyFindMigrationTests(
                            rootDirectory,
                            prefix + File.separator + file.getName(),
                            qualifiedName(packageName, file.getName()),
                            result);
                } else {
                    Matcher m = CLASS_NAME_PATTERN.matcher(file.getName());
                    if (m.matches()) {
                        String className = qualifiedName(packageName, m.group(CLASS_NAME_GROUP));

                        // We require the class name to match the file name
                        // for both java and scala.
                        try {
                            Class<?> clazz =
                                    Class.forName(
                                            className,
                                            false,
                                            MigrationTestsSnapshotGenerator.class.getClassLoader());
                            if (MigrationTest.class.isAssignableFrom(clazz)) {
                                result.add(Class.forName(className));
                            }
                        } catch (ClassNotFoundException e) {
                            LOG.warn(
                                    "The class "
                                            + className
                                            + " is not found, which might be excluded from the classpath.");
                        }
                    }
                }
            }
        }
    }

    private static String qualifiedName(String packageName, String simpleName) {
        return packageName.isEmpty() ? simpleName : packageName + "." + simpleName;
    }
}
