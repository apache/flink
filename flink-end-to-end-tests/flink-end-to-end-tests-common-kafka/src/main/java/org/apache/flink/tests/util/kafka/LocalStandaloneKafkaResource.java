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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.activation.OperatingSystemRestriction;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.util.FileUtils;
import org.apache.flink.util.OperatingSystem;

import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link KafkaResource} that downloads kafka and sets up a local kafka cluster with the bundled
 * zookeeper.
 */
public class LocalStandaloneKafkaResource implements KafkaResource {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneKafkaResource.class);
    private static final Pattern ZK_DATA_DIR_PATTERN = Pattern.compile(".*(dataDir=).*");
    private static final Pattern KAFKA_LOG_DIR_PATTERN = Pattern.compile(".*(log\\.dirs=).*");

    private static final String ZOOKEEPER_HOST = "localhost";
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String ZOOKEEPER_ADDRESS = ZOOKEEPER_HOST + ':' + ZOOKEEPER_PORT;
    private static final String KAFKA_HOST = "localhost";
    private static final int KAFKA_PORT = 9092;
    private static final String KAFKA_ADDRESS = KAFKA_HOST + ':' + KAFKA_PORT;

    private final TemporaryFolder tmp = new TemporaryFolder();

    private final DownloadCache downloadCache = DownloadCache.get();
    private final String kafkaVersion;
    private Path kafkaDir;
    @Nullable private Path logBackupDirectory;

    LocalStandaloneKafkaResource(final String kafkaVersion, @Nullable Path logBackupDirectory) {
        OperatingSystemRestriction.forbid(
                String.format(
                        "The %s relies on UNIX utils and shell scripts.",
                        getClass().getSimpleName()),
                OperatingSystem.WINDOWS);
        this.kafkaVersion = kafkaVersion;
        this.logBackupDirectory = logBackupDirectory;
    }

    private static String getKafkaDownloadUrl(final String kafkaVersion) {
        return String.format(
                "https://archive.apache.org/dist/kafka/%s/kafka_2.11-%s.tgz",
                kafkaVersion, kafkaVersion);
    }

    @Override
    public void before() throws Exception {
        tmp.create();
        downloadCache.before();

        this.kafkaDir = tmp.newFolder("kafka").toPath().toAbsolutePath();
        setupKafkaDist();
        setupKafkaCluster();
    }

    private void setupKafkaDist() throws IOException {
        final Path downloadDirectory = tmp.newFolder("getOrDownload").toPath();
        final Path kafkaArchive =
                downloadCache.getOrDownload(getKafkaDownloadUrl(kafkaVersion), downloadDirectory);

        LOG.info("Kafka location: {}", kafkaDir.toAbsolutePath());
        AutoClosableProcess.runBlocking(
                CommandLineWrapper.tar(kafkaArchive)
                        .extract()
                        .zipped()
                        .strip(1)
                        .targetDir(kafkaDir)
                        .build());

        LOG.info("Updating ZooKeeper properties");
        FileUtils.replace(
                kafkaDir.resolve(Paths.get("config", "zookeeper.properties")),
                ZK_DATA_DIR_PATTERN,
                matcher ->
                        matcher.replaceAll("$1" + kafkaDir.resolve("zookeeper").toAbsolutePath()));

        LOG.info("Updating Kafka properties");
        FileUtils.replace(
                kafkaDir.resolve(Paths.get("config", "server.properties")),
                KAFKA_LOG_DIR_PATTERN,
                matcher -> matcher.replaceAll("$1" + kafkaDir.resolve("kafka").toAbsolutePath()));
    }

    private void setupKafkaCluster() throws IOException {
        LOG.info("Starting zookeeper");
        AutoClosableProcess.runBlocking(
                kafkaDir.resolve(Paths.get("bin", "zookeeper-server-start.sh")).toString(),
                "-daemon",
                kafkaDir.resolve(Paths.get("config", "zookeeper.properties")).toString());
        LOG.info("Starting kafka");
        AutoClosableProcess.runBlocking(
                kafkaDir.resolve(Paths.get("bin", "kafka-server-start.sh")).toString(),
                "-daemon",
                kafkaDir.resolve(Paths.get("config", "server.properties")).toString());

        while (!isZookeeperRunning(kafkaDir)) {
            try {
                LOG.info("Waiting for ZooKeeper to start.");
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        while (!isKafkaRunning(kafkaDir)) {
            try {
                LOG.info("Waiting for Kafka to start.");
                Thread.sleep(500L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void afterTestSuccess() {
        shutdownResource();
        downloadCache.afterTestSuccess();
        tmp.delete();
    }

    @Override
    public void afterTestFailure() {
        shutdownResource();
        backupLogs();
        downloadCache.afterTestFailure();
        tmp.delete();
    }

    private void shutdownResource() {
        try {
            AutoClosableProcess.runBlocking(
                    kafkaDir.resolve(Paths.get("bin", "kafka-server-stop.sh")).toString());
            while (isKafkaRunning(kafkaDir)) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (IOException ioe) {
            LOG.warn("Error while shutting down kafka.", ioe);
        }
        try {
            AutoClosableProcess.runBlocking(
                    kafkaDir.resolve(Paths.get("bin", "zookeeper-server-stop.sh")).toString());
            while (isZookeeperRunning(kafkaDir)) {
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (IOException ioe) {
            LOG.warn("Error while shutting down zookeeper.", ioe);
        }
    }

    private void backupLogs() {
        if (logBackupDirectory != null) {
            final Path targetDirectory =
                    logBackupDirectory.resolve("kafka-" + UUID.randomUUID().toString());
            try {
                Files.createDirectories(targetDirectory);
                TestUtils.copyDirectory(kafkaDir.resolve("logs"), targetDirectory);
                LOG.info("Backed up logs to {}.", targetDirectory);
            } catch (IOException e) {
                LOG.warn("An error has occurred while backing up logs to {}.", targetDirectory, e);
            }
        }
    }

    private static boolean isZookeeperRunning(final Path kafkaDir) {
        try {
            queryBrokerStatus(kafkaDir, line -> {});
            return true;
        } catch (final IOException ioe) {
            // we get an exception if zookeeper isn't running
            return false;
        }
    }

    private static boolean isKafkaRunning(final Path kafkaDir) throws IOException {
        try {
            final AtomicBoolean atomicBrokerStarted = new AtomicBoolean(false);
            queryBrokerStatus(
                    kafkaDir,
                    line -> {
                        atomicBrokerStarted.compareAndSet(false, line.contains("\"port\":"));
                    });
            return atomicBrokerStarted.get();
        } catch (final IOException ioe) {
            // we get an exception if zookeeper isn't running
            return false;
        }
    }

    private static void queryBrokerStatus(
            final Path kafkaDir, final Consumer<String> stderrProcessor) throws IOException {
        AutoClosableProcess.create(
                        kafkaDir.resolve(Paths.get("bin", "zookeeper-shell.sh")).toString(),
                        ZOOKEEPER_ADDRESS,
                        "get",
                        "/brokers/ids/0")
                .setStdoutProcessor(stderrProcessor)
                .runBlocking();
    }

    @Override
    public void createTopic(int replicationFactor, int numPartitions, String topic)
            throws IOException {
        AutoClosableProcess.runBlocking(
                kafkaDir.resolve(Paths.get("bin", "kafka-topics.sh")).toString(),
                "--create",
                "--zookeeper",
                ZOOKEEPER_ADDRESS,
                "--replication-factor",
                String.valueOf(replicationFactor),
                "--partitions",
                String.valueOf(numPartitions),
                "--topic",
                topic);
    }

    @Override
    public void sendMessages(String topic, String... messages) throws IOException {
        List<String> args = createSendMessageArguments(topic);
        sendMessagesAndWait(args, messages);
    }

    @Override
    public void sendKeyedMessages(String topic, String keySeparator, String... messages)
            throws IOException {
        List<String> args = new ArrayList<>(createSendMessageArguments(topic));
        args.add("--property");
        args.add("parse.key=true");
        args.add("--property");
        args.add("key.separator=" + keySeparator);

        sendMessagesAndWait(args, messages);
    }

    private List<String> createSendMessageArguments(String topic) {
        return Arrays.asList(
                kafkaDir.resolve(Paths.get("bin", "kafka-console-producer.sh")).toString(),
                "--broker-list",
                KAFKA_ADDRESS,
                "--topic",
                topic);
    }

    private void sendMessagesAndWait(List<String> kafkaArgs, String... messages)
            throws IOException {
        try (AutoClosableProcess autoClosableProcess =
                AutoClosableProcess.runNonBlocking(kafkaArgs.toArray(new String[0]))) {

            try (PrintStream printStream =
                    new PrintStream(
                            autoClosableProcess.getProcess().getOutputStream(),
                            true,
                            StandardCharsets.UTF_8.name())) {
                for (final String message : messages) {
                    printStream.println(message);
                }
                printStream.flush();
            }

            try {
                // wait until the process shuts down on it's own
                // this is the only reliable way to ensure the producer has actually processed our
                // input
                autoClosableProcess.getProcess().waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<String> readMessage(int expectedNumMessages, String groupId, String topic)
            throws IOException {
        final List<String> messages =
                Collections.synchronizedList(new ArrayList<>(expectedNumMessages));

        try (final AutoClosableProcess kafka =
                AutoClosableProcess.create(
                                kafkaDir.resolve(Paths.get("bin", "kafka-console-consumer.sh"))
                                        .toString(),
                                "--bootstrap-server",
                                KAFKA_ADDRESS,
                                "--from-beginning",
                                "--max-messages",
                                String.valueOf(expectedNumMessages),
                                "--topic",
                                topic,
                                "--consumer-property",
                                "group.id=" + groupId)
                        .setStdoutProcessor(messages::add)
                        .runNonBlocking()) {

            final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(120));
            while (deadline.hasTimeLeft() && messages.size() < expectedNumMessages) {
                try {
                    LOG.info(
                            "Waiting for messages. Received {}/{}.",
                            messages.size(),
                            expectedNumMessages);
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (messages.size() != expectedNumMessages) {
                throw new IOException("Could not read expected number of messages.");
            }
            return messages;
        }
    }

    @Override
    public void setNumPartitions(int numPartitions, String topic) throws IOException {
        AutoClosableProcess.runBlocking(
                kafkaDir.resolve(Paths.get("bin", "kafka-topics.sh")).toString(),
                "--alter",
                "--topic",
                topic,
                "--partitions",
                String.valueOf(numPartitions),
                "--zookeeper",
                ZOOKEEPER_ADDRESS);
    }

    @Override
    public int getNumPartitions(String topic) throws IOException {
        final Pattern partitionCountPattern = Pattern.compile(".*PartitionCount:\\s*([0-9]+).*");
        final AtomicReference<Integer> partitionCountFound = new AtomicReference<>(-1);
        AutoClosableProcess.create(
                        kafkaDir.resolve(Paths.get("bin", "kafka-topics.sh")).toString(),
                        "--describe",
                        "--topic",
                        topic,
                        "--zookeeper",
                        ZOOKEEPER_ADDRESS)
                .setStdoutProcessor(
                        line -> {
                            final Matcher matcher = partitionCountPattern.matcher(line);
                            if (matcher.matches()) {
                                partitionCountFound.compareAndSet(
                                        -1, Integer.parseInt(matcher.group(1)));
                            }
                        })
                .runBlocking();
        return partitionCountFound.get();
    }

    @Override
    public long getPartitionOffset(String topic, int partition) throws IOException {
        final Pattern partitionOffsetPattern = Pattern.compile(".*:.*:([0-9]+)");
        final AtomicReference<Integer> partitionOffsetFound = new AtomicReference<>(-1);
        AutoClosableProcess.create(
                        kafkaDir.resolve(Paths.get("bin", "kafka-run-class.sh")).toString(),
                        "kafka.tools.GetOffsetShell",
                        "--broker-list",
                        KAFKA_ADDRESS,
                        "--topic",
                        topic,
                        "--partitions",
                        String.valueOf(partition),
                        "--time",
                        "-1")
                .setStdoutProcessor(
                        line -> {
                            final Matcher matcher = partitionOffsetPattern.matcher(line);
                            if (matcher.matches()) {
                                partitionOffsetFound.compareAndSet(
                                        -1, Integer.parseInt(matcher.group(1)));
                            }
                        })
                .runBlocking();

        final int partitionOffset = partitionOffsetFound.get();
        if (partitionOffset == -1) {
            throw new IOException("Could not determine partition offset.");
        }
        return partitionOffset;
    }

    @Override
    public Collection<InetSocketAddress> getBootstrapServerAddresses() {
        return Collections.singletonList(
                InetSocketAddress.createUnresolved(KAFKA_HOST, KAFKA_PORT));
    }

    @Override
    public InetSocketAddress getZookeeperAddress() {
        return InetSocketAddress.createUnresolved(KAFKA_HOST, KAFKA_PORT);
    }
}
