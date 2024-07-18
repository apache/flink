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

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.EntropyInjectingFileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.PathsCopyingFileSystem;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.RefCountedFileWithStream;
import org.apache.flink.core.fs.RefCountedTmpFileCreator;
import org.apache.flink.fs.s3.common.token.AbstractS3DelegationTokenReceiver;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;
import org.apache.flink.fs.s3.common.writer.S3RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionWithException;

import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ACCESS_KEY;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.ENDPOINT;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_EXTRA_ARGS;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.S5CMD_PATH;
import static org.apache.flink.fs.s3.common.AbstractS3FileSystemFactory.SECRET_KEY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of the Flink {@link org.apache.flink.core.fs.FileSystem} interface for S3. This
 * class implements the common behavior implemented directly by Flink and delegates common calls to
 * an implementation of Hadoop's filesystem abstraction.
 *
 * <p>Optionally this {@link FlinkS3FileSystem} can use <a href="https://github.com/peak/s5cmd">the
 * s5cmd tool</a> to speed up copying files.
 */
public class FlinkS3FileSystem extends HadoopFileSystem
        implements EntropyInjectingFileSystem, PathsCopyingFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkS3FileSystem.class);

    private static final long PROCESS_KILL_SLEEP_TIME_MS = 50L;

    @Nullable private final String entropyInjectionKey;

    private final int entropyLength;

    // ------------------- Recoverable Writer Parameters -------------------

    /** The minimum size of a part in the multipart upload, except for the last part: 5 MIBytes. */
    public static final long S3_MULTIPART_MIN_PART_SIZE = 5L << 20;

    private final String localTmpDir;

    private final FunctionWithException<File, RefCountedFileWithStream, IOException> tmpFileCreator;

    @Nullable private final S3AccessHelper s3AccessHelper;

    private final Executor uploadThreadPool;

    private final long s3uploadPartSize;

    private final int maxConcurrentUploadsPerStream;

    @Nullable private final S5CmdConfiguration s5CmdConfiguration;

    /** POJO representing parameters to configure s5cmd. */
    public static class S5CmdConfiguration {
        private final String path;
        private final List<String> args;
        @Nullable private final String accessArtifact;
        @Nullable private final String secretArtifact;
        @Nullable private final String endpoint;

        /** All parameters can be empty. */
        public S5CmdConfiguration(
                String path,
                String args,
                @Nullable String accessArtifact,
                @Nullable String secretArtifact,
                @Nullable String endpoint) {
            if (!path.isEmpty()) {
                File s5CmdFile = new File(path);
                checkArgument(s5CmdFile.isFile(), "Unable to find s5cmd binary under [%s]", path);
                checkArgument(
                        s5CmdFile.canExecute(), "s5cmd binary under [%s] is not executable", path);
            }
            this.path = path;
            this.args = Arrays.asList(args.split("\\s+"));
            this.accessArtifact = accessArtifact;
            this.secretArtifact = secretArtifact;
            this.endpoint = endpoint;
        }

        public static Optional<S5CmdConfiguration> of(Configuration flinkConfig) {
            return flinkConfig
                    .getOptional(S5CMD_PATH)
                    .map(
                            s ->
                                    new S5CmdConfiguration(
                                            s,
                                            flinkConfig.getString(S5CMD_EXTRA_ARGS),
                                            flinkConfig.get(ACCESS_KEY),
                                            flinkConfig.get(SECRET_KEY),
                                            flinkConfig.get(ENDPOINT)));
        }

        private void configureEnvironment(Map<String, String> environment) {
            Credentials credentials = AbstractS3DelegationTokenReceiver.getCredentials();
            if (credentials != null) {
                maybeSetEnvironmentVariable(
                        environment, "AWS_ACCESS_KEY_ID", credentials.getAccessKeyId());
                maybeSetEnvironmentVariable(
                        environment, "AWS_SECRET_ACCESS_KEY", credentials.getSecretAccessKey());
                maybeSetEnvironmentVariable(
                        environment, "AWS_SESSION_TOKEN", credentials.getSessionToken());
            } else {
                maybeSetEnvironmentVariable(environment, "AWS_ACCESS_KEY_ID", accessArtifact);
                maybeSetEnvironmentVariable(environment, "AWS_SECRET_ACCESS_KEY", secretArtifact);
                maybeSetEnvironmentVariable(environment, "S3_ENDPOINT_URL", endpoint);
            }
        }

        private static void maybeSetEnvironmentVariable(
                Map<String, String> environment, String key, @Nullable String value) {
            if (value == null) {
                return;
            }
            String oldValue = environment.put(key, value);
            if (oldValue != null) {
                LOG.warn(
                        "FlinkS3FileSystem configuration overwrote environment "
                                + "variable's [{}] old value [{}] with [{}]",
                        key,
                        oldValue,
                        value);
            }
        }

        @Override
        public String toString() {
            return "S5CmdConfiguration{"
                    + "path='"
                    + path
                    + '\''
                    + ", args="
                    + args
                    + ", accessArtifact='"
                    + (accessArtifact == null ? null : "****")
                    + '\''
                    + ", secretArtifact='"
                    + (secretArtifact == null ? null : "****")
                    + '\''
                    + ", endpoint='"
                    + endpoint
                    + '\''
                    + '}';
        }
    }

    /**
     * Creates a FlinkS3FileSystem based on the given Hadoop S3 file system. The given Hadoop file
     * system object is expected to be initialized already.
     *
     * <p>This constructor additionally configures the entropy injection for the file system.
     *
     * @param hadoopS3FileSystem The Hadoop FileSystem that will be used under the hood.
     * @param s5CmdConfiguration Configuration of the s5cmd.
     * @param entropyInjectionKey The substring that will be replaced by entropy or removed.
     * @param entropyLength The number of random alphanumeric characters to inject as entropy.
     */
    public FlinkS3FileSystem(
            FileSystem hadoopS3FileSystem,
            @Nullable S5CmdConfiguration s5CmdConfiguration,
            String localTmpDirectory,
            @Nullable String entropyInjectionKey,
            int entropyLength,
            @Nullable S3AccessHelper s3UploadHelper,
            long s3uploadPartSize,
            int maxConcurrentUploadsPerStream) {

        super(hadoopS3FileSystem);

        this.s5CmdConfiguration = s5CmdConfiguration;

        if (entropyInjectionKey != null && entropyLength <= 0) {
            throw new IllegalArgumentException(
                    "Entropy length must be >= 0 when entropy injection key is set");
        }

        this.entropyInjectionKey = entropyInjectionKey;
        this.entropyLength = entropyLength;

        // recoverable writer parameter configuration initialization
        this.localTmpDir = Preconditions.checkNotNull(localTmpDirectory);
        this.tmpFileCreator = RefCountedTmpFileCreator.inDirectories(new File(localTmpDirectory));
        this.s3AccessHelper = s3UploadHelper;
        this.uploadThreadPool = Executors.newCachedThreadPool();

        checkArgument(s3uploadPartSize >= S3_MULTIPART_MIN_PART_SIZE);
        this.s3uploadPartSize = s3uploadPartSize;
        this.maxConcurrentUploadsPerStream = maxConcurrentUploadsPerStream;
        LOG.info("Created Flink S3 FS, s5Cmd configuration: {}", s5CmdConfiguration);
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean canCopyPaths(Path source, Path destination) {
        return canCopyPaths();
    }

    private boolean canCopyPaths() {
        return s5CmdConfiguration != null;
    }

    @Override
    public void copyFiles(List<CopyRequest> requests, ICloseableRegistry closeableRegistry)
            throws IOException {
        checkState(canCopyPaths(), "#downloadFiles has been called illegally");
        List<String> artefacts = new ArrayList<>();
        artefacts.add(s5CmdConfiguration.path);
        artefacts.addAll(s5CmdConfiguration.args);
        artefacts.add("run");
        castSpell(convertToSpells(requests), closeableRegistry, artefacts.toArray(new String[0]));
    }

    private List<String> convertToSpells(List<CopyRequest> requests) throws IOException {
        List<String> spells = new ArrayList<>();
        for (CopyRequest request : requests) {
            Files.createDirectories(Paths.get(request.getDestination().toUri()).getParent());
            spells.add(
                    String.format(
                            "cp %s %s",
                            request.getSource().toUri().toString(),
                            request.getDestination().getPath()));
        }
        return spells;
    }

    private void castSpell(
            List<String> spells, ICloseableRegistry closeableRegistry, String... artefacts)
            throws IOException {
        LOG.info("Casting spell: {}", Arrays.toString(artefacts));
        int exitCode = 0;
        final AtomicReference<IOException> maybeCloseableRegistryException =
                new AtomicReference<>();

        // Setup temporary working directory for the process
        File tmpWorkingDir = new File(localTmpDir, "s5cmd_" + UUID.randomUUID());
        java.nio.file.Path tmpWorkingPath = Files.createDirectories(tmpWorkingDir.toPath());

        try {
            // Redirect the process input/output to files. Communicating directly through a
            // stream can lead to blocking and undefined behavior if the underlying process is
            // killed (known Java problem).
            ProcessBuilder hogwart = new ProcessBuilder(artefacts).directory(tmpWorkingDir);
            s5CmdConfiguration.configureEnvironment(hogwart.environment());
            File inScrolls = new File(tmpWorkingDir, "s5cmd_input");
            Preconditions.checkState(inScrolls.createNewFile());
            File outScrolls = new File(tmpWorkingDir, "s5cmd_output");
            Preconditions.checkState(outScrolls.createNewFile());

            FileUtils.writeFileUtf8(
                    inScrolls,
                    String.join(System.lineSeparator(), spells)
                            // a line separator after the last string is necessary
                            // because the file content serves as input to a process
                            // and similar to input from a terminal it needs a newline to take
                            // effect
                            + System.lineSeparator());

            final Process wizard =
                    hogwart.redirectErrorStream(true)
                            .redirectInput(inScrolls)
                            .redirectOutput(outScrolls)
                            .start();

            try {
                exitCode = wizard.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                destroyProcess(wizard);
                throw new IOException(createSpellErrorMessage(exitCode, outScrolls, artefacts), e);
            }
            if (exitCode != 0) {
                throw new IOException(
                        createSpellErrorMessage(exitCode, outScrolls, artefacts),
                        maybeCloseableRegistryException.get());
            }
        } finally {
            IOUtils.deleteFileQuietly(tmpWorkingPath);
        }
    }

    private static void destroyProcess(Process processToDestroy) {

        LOG.info("Destroying s5cmd copy process.");
        processToDestroy.destroy();

        // Needed for some operating systems to destroy the process
        IOUtils.closeAllQuietly(
                processToDestroy.getInputStream(),
                processToDestroy.getOutputStream(),
                processToDestroy.getErrorStream());

        sleepForProcessTermination(processToDestroy);

        if (!processToDestroy.isAlive()) {
            // We are done.
            return;
        }
        LOG.info("Forcibly destroying s5cmd copy process.");
        processToDestroy.destroyForcibly();

        sleepForProcessTermination(processToDestroy);

        if (processToDestroy.isAlive()) {
            LOG.warn("Could not destroy s5cmd copy process.");
        }
    }

    private static void sleepForProcessTermination(Process processToDestroy) {
        if (processToDestroy.isAlive()) {
            try {
                // Give the process a little bit of time to gracefully shut down
                Thread.sleep(PROCESS_KILL_SLEEP_TIME_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private String createSpellErrorMessage(int exitCode, File outFile, String... artefacts) {
        String output = "Unknown: cannot read copy process output.";
        try {
            output = FileUtils.readFileUtf8(outFile);
        } catch (IOException e) {
            LOG.info("Error while reading s5cmd output from file {}.", outFile, e);
        }

        return new StringBuilder()
                .append("Failed to cast s5cmd spell [")
                .append(String.join(" ", artefacts))
                .append("]")
                .append(String.format(" [exit code = %d]", exitCode))
                .append(" [cfg: ")
                .append(s5CmdConfiguration)
                .append("]")
                .append(" maybe due to:\n")
                .append(output)
                .toString();
    }

    @Nullable
    @Override
    public String getEntropyInjectionKey() {
        return entropyInjectionKey;
    }

    @Override
    public String generateEntropy() {
        return StringUtils.generateRandomAlphanumericString(
                ThreadLocalRandom.current(), entropyLength);
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }

    public String getLocalTmpDir() {
        return localTmpDir;
    }

    @Override
    public RecoverableWriter createRecoverableWriter() throws IOException {
        if (s3AccessHelper == null) {
            // this is the case for Presto
            throw new UnsupportedOperationException(
                    "This s3 file system implementation does not support recoverable writers.");
        }

        return S3RecoverableWriter.writer(
                getHadoopFileSystem(),
                tmpFileCreator,
                s3AccessHelper,
                uploadThreadPool,
                s3uploadPartSize,
                maxConcurrentUploadsPerStream);
    }
}
