/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.cli.CommandLine;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYCLIENTEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYTHON_PATH;
import static org.apache.flink.python.PythonOptions.PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES_DISTRIBUTED_CACHE_INFO;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO;

/**
 * Utility class for Python dependency management. The dependencies will be registered at the
 * distributed cache.
 */
@Internal
public class PythonDependencyUtils {

    public static final String FILE = "file";
    public static final String CACHE = "cache";
    public static final String FILE_DELIMITER = ",";
    public static final String PARAM_DELIMITER = "#";
    private static final String HASH_ALGORITHM = "SHA-256";

    /**
     * Adds python dependencies to registered cache file list according to given configuration and
     * returns a new configuration which contains the metadata of the registered python
     * dependencies.
     *
     * @param cachedFiles The list used to store registered cached files.
     * @param config The configuration which contains python dependency configuration.
     * @return A new configuration which contains the metadata of the registered python
     *     dependencies.
     */
    public static Configuration configurePythonDependencies(
            List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles,
            ReadableConfig config) {
        final PythonDependencyManager pythonDependencyManager =
                new PythonDependencyManager(cachedFiles, config);
        final Configuration pythonDependencyConfig = new Configuration();
        pythonDependencyManager.applyToConfiguration(pythonDependencyConfig);
        return pythonDependencyConfig;
    }

    public static Configuration parsePythonDependencyConfiguration(CommandLine commandLine) {
        Configuration config = new Configuration();
        if (commandLine.hasOption(PYFILES_OPTION.getOpt())) {
            config.set(
                    PythonOptions.PYTHON_FILES,
                    commandLine.getOptionValue(PYFILES_OPTION.getOpt()));
        }
        if (commandLine.hasOption(PYREQUIREMENTS_OPTION.getOpt())) {
            config.set(
                    PythonOptions.PYTHON_REQUIREMENTS,
                    commandLine.getOptionValue(PYREQUIREMENTS_OPTION.getOpt()));
        }
        if (commandLine.hasOption(PYARCHIVE_OPTION.getOpt())) {
            config.set(
                    PythonOptions.PYTHON_ARCHIVES,
                    commandLine.getOptionValue(PYARCHIVE_OPTION.getOpt()));
        }
        if (commandLine.hasOption(PYEXEC_OPTION.getOpt())) {
            config.set(
                    PythonOptions.PYTHON_EXECUTABLE,
                    commandLine.getOptionValue(PYEXEC_OPTION.getOpt()));
        }
        if (commandLine.hasOption(PYCLIENTEXEC_OPTION.getOpt())) {
            config.set(
                    PythonOptions.PYTHON_CLIENT_EXECUTABLE,
                    commandLine.getOptionValue(PYCLIENTEXEC_OPTION.getOpt()));
        }
        if (commandLine.hasOption(PYTHON_PATH.getOpt())) {
            config.set(PythonOptions.PYTHON_PATH, commandLine.getOptionValue(PYTHON_PATH.getOpt()));
        }

        return config;
    }

    public static void merge(Configuration config, Configuration pythonConfiguration) {
        Configuration toMerge = new Configuration(pythonConfiguration);
        if (toMerge.contains(PythonOptions.PYTHON_FILES)) {
            if (config.contains(PythonOptions.PYTHON_FILES)) {
                config.set(
                        PythonOptions.PYTHON_FILES,
                        String.join(
                                FILE_DELIMITER,
                                toMerge.get(PythonOptions.PYTHON_FILES),
                                config.get(PythonOptions.PYTHON_FILES)));
            } else {
                config.set(PythonOptions.PYTHON_FILES, toMerge.get(PythonOptions.PYTHON_FILES));
            }
            toMerge.removeConfig(PythonOptions.PYTHON_FILES);
        }
        if (toMerge.contains(PythonOptions.PYTHON_ARCHIVES)) {
            if (config.contains(PythonOptions.PYTHON_ARCHIVES)) {
                config.set(
                        PythonOptions.PYTHON_ARCHIVES,
                        String.join(
                                FILE_DELIMITER,
                                toMerge.get(PythonOptions.PYTHON_ARCHIVES),
                                config.get(PythonOptions.PYTHON_ARCHIVES)));
            } else {
                config.set(
                        PythonOptions.PYTHON_ARCHIVES, toMerge.get(PythonOptions.PYTHON_ARCHIVES));
            }
            toMerge.removeConfig(PythonOptions.PYTHON_ARCHIVES);
        }
        config.addAll(toMerge);
    }

    /** Helper class for Python dependency management. */
    private static class PythonDependencyManager {

        private static final String PYTHON_FILE_PREFIX = "python_file";
        private static final String PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file";
        private static final String PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache";
        private static final String PYTHON_ARCHIVE_PREFIX = "python_archive";

        private final List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles;
        private final ReadableConfig config;

        private PythonDependencyManager(
                List<Tuple2<String, DistributedCache.DistributedCacheEntry>> cachedFiles,
                ReadableConfig config) {
            this.cachedFiles = cachedFiles;
            this.config = config;
        }

        /**
         * Adds a Python dependency which could be .py files, Python packages(.zip, .egg etc.) or
         * local directories. The dependencies will be added to the PYTHONPATH of the Python UDF
         * worker and the local Py4J python client.
         *
         * @param filePath The path of the Python dependency.
         */
        private void addPythonFile(Configuration pythonDependencyConfig, String filePath) {
            Preconditions.checkNotNull(filePath);
            String fileKey = generateUniqueFileKey(PYTHON_FILE_PREFIX, filePath);
            registerCachedFileIfNotExist(filePath, fileKey);
            if (!pythonDependencyConfig.contains(PYTHON_FILES_DISTRIBUTED_CACHE_INFO)) {
                pythonDependencyConfig.set(
                        PYTHON_FILES_DISTRIBUTED_CACHE_INFO, new LinkedHashMap<>());
            }
            pythonDependencyConfig
                    .get(PYTHON_FILES_DISTRIBUTED_CACHE_INFO)
                    .put(fileKey, new File(filePath).getName());
        }

        /**
         * Specifies the third-party dependencies via a requirements file. These dependencies will
         * be installed by the command "pip install -r [requirements file]" before launching the
         * Python UDF worker.
         *
         * @param requirementsFilePath The path of the requirements file.
         */
        private void setPythonRequirements(
                Configuration pythonDependencyConfig, String requirementsFilePath) {
            setPythonRequirements(pythonDependencyConfig, requirementsFilePath, null);
        }

        /**
         * Specifies the third-party dependencies via a requirements file. The
         * `requirementsCachedDir` will be uploaded to support offline installation. These
         * dependencies will be installed by the command "pip install -r [requirements file]
         * --find-links [requirements cached dir]" before launching the Python UDF worker.
         *
         * @param requirementsFilePath The path of the requirements file.
         * @param requirementsCachedDir The path of the requirements cached directory.
         */
        private void setPythonRequirements(
                Configuration pythonDependencyConfig,
                String requirementsFilePath,
                @Nullable String requirementsCachedDir) {
            Preconditions.checkNotNull(requirementsFilePath);
            if (!pythonDependencyConfig.contains(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)) {
                pythonDependencyConfig.set(
                        PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO, new HashMap<>());
            }
            pythonDependencyConfig.get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO).clear();
            removeCachedFilesByPrefix(PYTHON_REQUIREMENTS_FILE_PREFIX);
            removeCachedFilesByPrefix(PYTHON_REQUIREMENTS_CACHE_PREFIX);

            String fileKey =
                    generateUniqueFileKey(PYTHON_REQUIREMENTS_FILE_PREFIX, requirementsFilePath);
            registerCachedFileIfNotExist(requirementsFilePath, fileKey);
            pythonDependencyConfig
                    .get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                    .put(FILE, fileKey);

            if (requirementsCachedDir != null) {
                String cacheDirKey =
                        generateUniqueFileKey(
                                PYTHON_REQUIREMENTS_CACHE_PREFIX, requirementsCachedDir);
                registerCachedFileIfNotExist(requirementsCachedDir, cacheDirKey);
                pythonDependencyConfig
                        .get(PYTHON_REQUIREMENTS_FILE_DISTRIBUTED_CACHE_INFO)
                        .put(CACHE, cacheDirKey);
            }
        }

        /**
         * Adds a Python archive file (zip format). The file will be extracted and moved to a
         * dedicated directory under the working directory of the Python UDF workers. The param
         * `targetDir` is the name of the dedicated directory. The Python UDFs and the config option
         * "python.executable" could access the extracted files via relative path.
         *
         * @param archivePath The path of the archive file.
         * @param targetDir The name of the target directory.
         */
        private void addPythonArchive(
                Configuration pythonDependencyConfig, String archivePath, String targetDir) {
            Preconditions.checkNotNull(archivePath);
            if (!pythonDependencyConfig.contains(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO)) {
                pythonDependencyConfig.set(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO, new HashMap<>());
            }
            String fileKey =
                    generateUniqueFileKey(
                            PYTHON_ARCHIVE_PREFIX, archivePath + PARAM_DELIMITER + targetDir);
            registerCachedFileIfNotExist(archivePath, fileKey);
            pythonDependencyConfig
                    .get(PYTHON_ARCHIVES_DISTRIBUTED_CACHE_INFO)
                    .put(fileKey, targetDir);
        }

        private void applyToConfiguration(Configuration pythonDependencyConfig) {
            config.getOptional(PythonOptions.PYTHON_FILES)
                    .ifPresent(
                            pyFiles -> {
                                for (String filePath : pyFiles.split(FILE_DELIMITER)) {
                                    addPythonFile(pythonDependencyConfig, filePath);
                                }
                            });

            config.getOptional(PythonOptions.PYTHON_REQUIREMENTS)
                    .ifPresent(
                            pyRequirements -> {
                                if (pyRequirements.contains(PARAM_DELIMITER)) {
                                    String[] requirementFileAndCache =
                                            pyRequirements.split(PARAM_DELIMITER, 2);
                                    setPythonRequirements(
                                            pythonDependencyConfig,
                                            requirementFileAndCache[0],
                                            requirementFileAndCache[1]);
                                } else {
                                    setPythonRequirements(pythonDependencyConfig, pyRequirements);
                                }
                            });

            config.getOptional(PythonOptions.PYTHON_ARCHIVES)
                    .ifPresent(
                            pyArchives -> {
                                for (String archive : pyArchives.split(FILE_DELIMITER)) {
                                    String archivePath;
                                    String targetDir;
                                    if (archive.contains(PARAM_DELIMITER)) {
                                        String[] filePathAndTargetDir =
                                                archive.split(PARAM_DELIMITER, 2);
                                        archivePath = filePathAndTargetDir[0];
                                        targetDir =
                                                new File(archivePath).getName()
                                                        + PARAM_DELIMITER
                                                        + filePathAndTargetDir[1];
                                    } else {
                                        archivePath = archive;
                                        targetDir = new File(archivePath).getName();
                                    }
                                    addPythonArchive(
                                            pythonDependencyConfig, archivePath, targetDir);
                                }
                            });

            config.getOptional(PYTHON_EXECUTABLE)
                    .ifPresent(e -> pythonDependencyConfig.set(PYTHON_EXECUTABLE, e));

            config.getOptional(PYTHON_CLIENT_EXECUTABLE)
                    .ifPresent(e -> pythonDependencyConfig.set(PYTHON_CLIENT_EXECUTABLE, e));

            config.getOptional(PythonOptions.PYTHON_PATH)
                    .ifPresent(
                            pyPath ->
                                    pythonDependencyConfig.set(PythonOptions.PYTHON_PATH, pyPath));
        }

        private String generateUniqueFileKey(String prefix, String hashString) {
            MessageDigest messageDigest;
            try {
                messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            messageDigest.update(hashString.getBytes(StandardCharsets.UTF_8));

            return String.format(
                    "%s_%s", prefix, StringUtils.byteToHexString(messageDigest.digest()));
        }

        private void registerCachedFileIfNotExist(String filePath, String fileKey) {
            if (cachedFiles.stream().noneMatch(t -> t.f0.equals(fileKey))) {
                cachedFiles.add(
                        new Tuple2<>(
                                fileKey,
                                new DistributedCache.DistributedCacheEntry(filePath, false)));
            }
        }

        private void removeCachedFilesByPrefix(String prefix) {
            cachedFiles.removeAll(
                    cachedFiles.stream()
                            .filter(t -> t.f0.matches("^" + prefix + "_[a-z0-9]{64}$"))
                            .collect(Collectors.toSet()));
        }
    }
}
