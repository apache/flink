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

package org.apache.flink.python.env;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.python.util.CompressionUtils;
import org.apache.flink.python.util.PythonEnvironmentManagerUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.python.util.PythonDependencyUtils.PARAM_DELIMITER;

/**
 * The base class of python environment manager which is used to create the PythonEnvironment object
 * used to execute Python functions.
 */
@Internal
public abstract class AbstractPythonEnvironmentManager implements PythonEnvironmentManager {

    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractPythonEnvironmentManager.class);

    private static final long CHECK_INTERVAL = 20;
    private static final long CHECK_TIMEOUT = 1000;

    private transient Thread shutdownHook;

    protected transient PythonLeasedResource resource;

    protected final PythonDependencyInfo dependencyInfo;

    private final Map<String, String> systemEnv;

    private final String[] tmpDirectories;
    private final JobID jobID;

    @VisibleForTesting public static final String PYTHON_REQUIREMENTS_DIR = "python-requirements";

    @VisibleForTesting
    public static final String PYTHON_REQUIREMENTS_FILE = "_PYTHON_REQUIREMENTS_FILE";

    @VisibleForTesting
    public static final String PYTHON_REQUIREMENTS_CACHE = "_PYTHON_REQUIREMENTS_CACHE";

    @VisibleForTesting
    public static final String PYTHON_REQUIREMENTS_INSTALL_DIR = "_PYTHON_REQUIREMENTS_INSTALL_DIR";

    @VisibleForTesting public static final String PYTHON_WORKING_DIR = "_PYTHON_WORKING_DIR";

    @VisibleForTesting public static final String PYTHON_FILES_DIR = "python-files";

    @VisibleForTesting public static final String PYTHON_ARCHIVES_DIR = "python-archives";

    @VisibleForTesting
    public static final String PYFLINK_GATEWAY_DISABLED = "PYFLINK_GATEWAY_DISABLED";

    public AbstractPythonEnvironmentManager(
            PythonDependencyInfo dependencyInfo,
            String[] tmpDirectories,
            Map<String, String> systemEnv,
            JobID jobID) {
        this.dependencyInfo = Objects.requireNonNull(dependencyInfo);
        this.tmpDirectories = Objects.requireNonNull(tmpDirectories);
        this.systemEnv = Objects.requireNonNull(systemEnv);
        this.jobID = Objects.requireNonNull(jobID);
    }

    @Override
    public void open() throws Exception {
        resource =
                PythonEnvResources.getOrAllocateSharedResource(
                        jobID,
                        jobID -> {
                            String baseDirectory = createBaseDirectory(tmpDirectories);

                            File baseDirectoryFile = new File(baseDirectory);
                            if (!baseDirectoryFile.exists() && !baseDirectoryFile.mkdir()) {
                                throw new IOException(
                                        "Could not create the base directory: " + baseDirectory);
                            }

                            Map<String, String> env = constructEnvironmentVariables(baseDirectory);
                            installRequirements(baseDirectory, env);
                            return Tuple2.of(baseDirectory, env);
                        });
        shutdownHook =
                ShutdownHookUtil.addShutdownHook(
                        this, AbstractPythonEnvironmentManager.class.getSimpleName(), LOG);
    }

    @Override
    public void close() throws Exception {
        try {
            PythonEnvResources.release(jobID);
        } finally {
            if (shutdownHook != null) {
                ShutdownHookUtil.removeShutdownHook(
                        shutdownHook, AbstractPythonEnvironmentManager.class.getSimpleName(), LOG);
                shutdownHook = null;
            }
        }
    }

    @VisibleForTesting
    public String getBaseDirectory() {
        return resource.baseDirectory;
    }

    @VisibleForTesting
    public Map<String, String> getPythonEnv() {
        return resource.env;
    }

    /**
     * Constructs the environment variables which is used to launch the python UDF worker.
     *
     * @return The environment variables which contain the paths of the python dependencies.
     */
    @VisibleForTesting
    public Map<String, String> constructEnvironmentVariables(String baseDirectory)
            throws IOException {
        Map<String, String> env = new HashMap<>(this.systemEnv);

        constructFilesDirectory(env, baseDirectory);

        if (dependencyInfo.getPythonPath().isPresent()) {
            appendToPythonPath(
                    env, Collections.singletonList(dependencyInfo.getPythonPath().get()));
        }

        LOG.info("PYTHONPATH of python worker: {}", env.get("PYTHONPATH"));

        constructRequirementsDirectory(env, baseDirectory);

        constructArchivesDirectory(env, baseDirectory);

        // set BOOT_LOG_DIR.
        env.put("BOOT_LOG_DIR", baseDirectory);

        // disable the launching of gateway server to prevent from this dead loop:
        // launch UDF worker -> import udf -> import job code
        //        ^                                    | (If the job code is not enclosed in a
        //        |                                    |  if name == 'main' statement)
        //        |                                    V
        // execute job in local mode <- launch gateway server and submit job to local executor
        env.put(PYFLINK_GATEWAY_DISABLED, "true");

        // set the path of python interpreter, it will be used to execute the udf worker.
        env.put("python", dependencyInfo.getPythonExec());
        LOG.info("Python interpreter path: {}", dependencyInfo.getPythonExec());
        return env;
    }

    private static String createBaseDirectory(String[] tmpDirectories) throws IOException {
        Random rnd = new Random();
        // try to find a unique file name for the base directory
        int maxAttempts = 10;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            String directory = tmpDirectories[rnd.nextInt(tmpDirectories.length)];
            File baseDirectory = new File(directory, "python-dist-" + UUID.randomUUID().toString());
            if (baseDirectory.mkdirs()) {
                return baseDirectory.getAbsolutePath();
            }
        }

        throw new IOException(
                "Could not find a unique directory name in '"
                        + Arrays.toString(tmpDirectories)
                        + "' for storing the generated files of python dependency.");
    }

    private void installRequirements(String baseDirectory, Map<String, String> env)
            throws IOException {
        // Directory for storing the installation result of the requirements file.
        String requirementsDirectory =
                String.join(File.separator, baseDirectory, PYTHON_REQUIREMENTS_DIR);
        if (dependencyInfo.getRequirementsFilePath().isPresent()) {
            LOG.info("Trying to pip install the Python requirements...");
            PythonEnvironmentManagerUtils.pipInstallRequirements(
                    dependencyInfo.getRequirementsFilePath().get(),
                    dependencyInfo.getRequirementsCacheDir().orElse(null),
                    requirementsDirectory,
                    dependencyInfo.getPythonExec(),
                    env);
        }
    }

    private void constructFilesDirectory(Map<String, String> env, String baseDirectory)
            throws IOException {
        // link or copy python files to filesDirectory and add them to PYTHONPATH
        List<String> pythonFilePaths = new ArrayList<>();

        // Directory for storing the uploaded python files.
        String filesDirectory = String.join(File.separator, baseDirectory, PYTHON_FILES_DIR);

        for (Map.Entry<String, String> entry : dependencyInfo.getPythonFiles().entrySet()) {
            // The origin file name will be wiped when downloaded from the distributed cache,
            // restore the origin name to
            // make sure the python files could be imported.
            // The path of the restored python file will be as following:
            // ${baseDirectory}/${PYTHON_FILES_DIR}/${distributedCacheFileName}/${originFileName}
            String distributedCacheFileName = new File(entry.getKey()).getName();
            String originFileName = entry.getValue();

            Path target =
                    FileSystems.getDefault()
                            .getPath(filesDirectory, distributedCacheFileName, originFileName);
            if (!target.getParent().toFile().mkdirs()) {
                throw new IOException(
                        String.format(
                                "Could not create the directory: %s !",
                                target.getParent().toString()));
            }
            Path src = FileSystems.getDefault().getPath(entry.getKey());
            try {
                Files.createSymbolicLink(target, src);
            } catch (IOException e) {
                LOG.warn(
                        String.format(
                                "Could not create the symbolic link of: %s, the link path is %s, fallback to copy.",
                                src, target),
                        e);
                FileUtils.copy(
                        new org.apache.flink.core.fs.Path(src.toUri()),
                        new org.apache.flink.core.fs.Path(target.toUri()),
                        false);
            }

            File pythonFile = new File(entry.getKey());
            String pythonPath;
            if (pythonFile.isFile() && originFileName.endsWith(".py")) {
                // If the python file is file with suffix .py, add the parent directory to
                // PYTHONPATH.
                pythonPath = String.join(File.separator, filesDirectory, distributedCacheFileName);
            } else if (pythonFile.isFile() && originFileName.endsWith(".zip")) {
                // Expand the zip file and add the root directory to PYTHONPATH
                // as not all zip files are importable
                org.apache.flink.core.fs.Path targetDirectory =
                        new org.apache.flink.core.fs.Path(
                                filesDirectory,
                                String.join(
                                        File.separator,
                                        distributedCacheFileName,
                                        originFileName.substring(
                                                0, originFileName.lastIndexOf("."))));
                FileUtils.expandDirectory(
                        new org.apache.flink.core.fs.Path(pythonFile.getAbsolutePath()),
                        targetDirectory);
                pythonPath = targetDirectory.toString();
            } else {
                pythonPath =
                        String.join(
                                File.separator,
                                filesDirectory,
                                distributedCacheFileName,
                                originFileName);
            }
            pythonFilePaths.add(pythonPath);
        }
        appendToPythonPath(env, pythonFilePaths);
    }

    private void constructRequirementsDirectory(Map<String, String> env, String baseDirectory)
            throws IOException {
        String requirementsDirectory =
                String.join(File.separator, baseDirectory, PYTHON_REQUIREMENTS_DIR);
        if (dependencyInfo.getRequirementsFilePath().isPresent()) {
            File requirementsDirectoryFile = new File(requirementsDirectory);
            if (!requirementsDirectoryFile.mkdirs()) {
                throw new IOException(
                        String.format(
                                "Creating the requirements target directory: %s failed!",
                                requirementsDirectory));
            }

            env.put(PYTHON_REQUIREMENTS_FILE, dependencyInfo.getRequirementsFilePath().get());
            LOG.info(
                    "Requirements.txt of python worker: {}",
                    dependencyInfo.getRequirementsFilePath().get());

            if (dependencyInfo.getRequirementsCacheDir().isPresent()) {
                env.put(PYTHON_REQUIREMENTS_CACHE, dependencyInfo.getRequirementsCacheDir().get());
                LOG.info(
                        "Requirements cache dir of python worker: {}",
                        dependencyInfo.getRequirementsCacheDir().get());
            }

            env.put(PYTHON_REQUIREMENTS_INSTALL_DIR, requirementsDirectory);
            LOG.info("Requirements install directory of python worker: {}", requirementsDirectory);
        }
    }

    private void constructArchivesDirectory(Map<String, String> env, String baseDirectory)
            throws IOException {
        // Directory for storing the extracted result of the archive files.
        String archivesDirectory = String.join(File.separator, baseDirectory, PYTHON_ARCHIVES_DIR);

        if (!dependencyInfo.getArchives().isEmpty()) {
            // set the archives directory as the working directory, then user could access the
            // content of the archives via relative path
            env.put(PYTHON_WORKING_DIR, archivesDirectory);
            LOG.info("Python working dir of python worker: {}", archivesDirectory);

            // extract archives to archives directory
            for (Map.Entry<String, String> entry : dependencyInfo.getArchives().entrySet()) {
                String srcFilePath = entry.getKey();

                String originalFileName;
                String targetDirName;
                if (entry.getValue().contains(PARAM_DELIMITER)) {
                    String[] filePathAndTargetDir = entry.getValue().split(PARAM_DELIMITER, 2);
                    originalFileName = filePathAndTargetDir[0];
                    targetDirName = filePathAndTargetDir[1];
                } else {
                    originalFileName = entry.getValue();
                    targetDirName = originalFileName;
                }

                String targetDirPath =
                        String.join(File.separator, archivesDirectory, targetDirName);
                CompressionUtils.extractFile(srcFilePath, targetDirPath, originalFileName);
            }
        }
    }

    private static void appendToPythonPath(
            Map<String, String> env, List<String> pythonDependencies) {
        if (pythonDependencies.isEmpty()) {
            return;
        }

        String pythonDependencyPath = String.join(File.pathSeparator, pythonDependencies);
        String pythonPath = env.get("PYTHONPATH");
        if (Strings.isNullOrEmpty(pythonPath)) {
            env.put("PYTHONPATH", pythonDependencyPath);
        } else {
            env.put(
                    "PYTHONPATH",
                    String.join(File.pathSeparator, pythonDependencyPath, pythonPath));
        }
    }

    private static final class PythonEnvResources {

        private static final ReentrantLock lock = new ReentrantLock();

        @GuardedBy("lock")
        private static final Map<Object, PythonLeasedResource> reservedResources = new HashMap<>();

        static PythonLeasedResource getOrAllocateSharedResource(
                Object type,
                FunctionWithException<Object, Tuple2<String, Map<String, String>>, Exception>
                        initializer)
                throws Exception {
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted which preparing python environment.");
            }

            try {
                PythonLeasedResource resource = reservedResources.get(type);
                if (resource == null) {
                    resource = createResource(initializer, type);
                    reservedResources.put(type, resource);
                }
                resource.incRef();
                return resource;
            } finally {
                lock.unlock();
            }
        }

        public static void release(JobID jobID) throws Exception {
            lock.lock();
            try {
                final PythonLeasedResource resource = reservedResources.get(jobID);
                if (resource == null) {
                    return;
                }
                resource.decRef();
                if (resource.refCount == 0) {
                    reservedResources.remove(jobID);
                    resource.close();
                }

            } finally {
                lock.unlock();
            }
        }

        private static PythonLeasedResource createResource(
                FunctionWithException<Object, Tuple2<String, Map<String, String>>, Exception>
                        initializer,
                Object type)
                throws Exception {
            Tuple2<String, Map<String, String>> resource = initializer.apply(type);
            String baseDirectory = resource.f0;
            Map<String, String> env = resource.f1;
            return new PythonLeasedResource(baseDirectory, env);
        }
    }

    /**
     * Python lease resource which includes environment variables and working directory of execution
     * python environment.
     */
    public static final class PythonLeasedResource implements AutoCloseable {
        public final Map<String, String> env;

        /** The base directory of the Python Environment. */
        public final String baseDirectory;

        /** Keep track of the number of threads sharing this Python environment resources. */
        private long refCount = 0;

        PythonLeasedResource(String baseDirectory, Map<String, String> env) {
            this.baseDirectory = baseDirectory;
            this.env = env;
        }

        void incRef() {
            this.refCount += 1;
        }

        void decRef() {
            Preconditions.checkState(refCount > 0);
            this.refCount -= 1;
        }

        @Override
        public void close() throws Exception {
            int retries = 0;
            while (true) {
                try {
                    FileUtils.deleteDirectory(new File(baseDirectory));
                    break;
                } catch (Throwable t) {
                    retries++;
                    if (retries <= CHECK_TIMEOUT / CHECK_INTERVAL) {
                        LOG.warn(
                                String.format(
                                        "Failed to delete the working directory %s of the Python UDF worker. Retrying...",
                                        baseDirectory),
                                t);
                    } else {
                        LOG.warn(
                                String.format(
                                        "Failed to delete the working directory %s of the Python UDF worker.",
                                        baseDirectory),
                                t);
                        break;
                    }
                }
            }
        }
    }
}
