/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to terminate a given {@link Process} when exiting a try-with-resources statement.
 */
public class AutoClosableProcess implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AutoClosableProcess.class);

    private final Process process;

    private AutoClosableProcess(final Process process) {
        Preconditions.checkNotNull(process);
        this.process = process;
    }

    public Process getProcess() {
        return process;
    }

    public static AutoClosableProcess runNonBlocking(String... commands) throws IOException {
        return create(commands).runNonBlocking();
    }

    public static void runBlocking(String... commands) throws IOException {
        create(commands).runBlocking();
    }

    public static AutoClosableProcessBuilder create(String... commands) {
        return new AutoClosableProcessBuilder(commands);
    }

    /** Builder for most sophisticated processes. */
    public static final class AutoClosableProcessBuilder {
        private final String[] commands;
        private Consumer<String> stdoutProcessor = LOG::debug;
        private Consumer<String> stderrProcessor = LOG::debug;
        private Consumer<Map<String, String>> envProcessor = map -> {};
        private @Nullable String[] stdInputs;

        AutoClosableProcessBuilder(final String... commands) {
            this.commands = commands;
        }

        public AutoClosableProcessBuilder setEnv(final Consumer<Map<String, String>> envProcessor) {
            this.envProcessor = envProcessor;
            return this;
        }

        public AutoClosableProcessBuilder setStdoutProcessor(
                final Consumer<String> stdoutProcessor) {
            this.stdoutProcessor = stdoutProcessor;
            return this;
        }

        public AutoClosableProcessBuilder setStderrProcessor(
                final Consumer<String> stderrProcessor) {
            this.stderrProcessor = stderrProcessor;
            return this;
        }

        public AutoClosableProcessBuilder setStdInputs(final String... inputLines) {
            checkNotNull(inputLines);
            checkArgument(inputLines.length >= 1);
            this.stdInputs = inputLines;
            return this;
        }

        public void runBlocking() throws IOException {
            runBlocking(Duration.ofSeconds(30));
        }

        public void runBlocking(final Duration timeout) throws IOException {
            final StringWriter sw = new StringWriter();
            try (final PrintWriter printer = new PrintWriter(sw)) {
                final Process process =
                        createProcess(
                                commands,
                                stdoutProcessor,
                                line -> {
                                    stderrProcessor.accept(line);
                                    printer.println(line);
                                },
                                envProcessor,
                                stdInputs);

                try (AutoClosableProcess autoProcess = new AutoClosableProcess(process)) {
                    final boolean success =
                            process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
                    if (!success) {
                        throw new TimeoutException(
                                "Process exceeded timeout of " + timeout.getSeconds() + "seconds.");
                    }

                    if (process.exitValue() != 0) {
                        throw new IOException(
                                "Process execution failed due error. Error output:" + sw);
                    }
                } catch (TimeoutException | InterruptedException e) {
                    throw new IOException("Process failed due to timeout.");
                }
            }
        }

        public void runBlockingWithRetry(
                final int maxRetries, final Duration attemptTimeout, final Duration globalTimeout)
                throws IOException {
            int retries = 0;
            final Deadline globalDeadline = Deadline.fromNow(globalTimeout);

            while (true) {
                try {
                    runBlocking(attemptTimeout);
                    break;
                } catch (Exception e) {
                    if (++retries > maxRetries || !globalDeadline.hasTimeLeft()) {
                        String errMsg =
                                String.format(
                                        "Process (%s) exceeded timeout (%s) or number of retries (%s).",
                                        Arrays.toString(commands),
                                        globalTimeout.toMillis(),
                                        maxRetries);
                        throw new IOException(errMsg, e);
                    }
                }
            }
        }

        public AutoClosableProcess runNonBlocking() throws IOException {
            return new AutoClosableProcess(
                    createProcess(
                            commands, stdoutProcessor, stderrProcessor, envProcessor, stdInputs));
        }
    }

    private static Process createProcess(
            final String[] commands,
            Consumer<String> stdoutProcessor,
            Consumer<String> stderrProcessor,
            Consumer<Map<String, String>> envProcessor,
            @Nullable String[] stdInputs)
            throws IOException {
        final ProcessBuilder processBuilder = new ProcessBuilder();
        LOG.debug("Creating process: {}", Arrays.toString(commands));
        processBuilder.command(commands);
        envProcessor.accept(processBuilder.environment());

        final Process process = processBuilder.start();

        consumeOutput(process.getInputStream(), stdoutProcessor);
        consumeOutput(process.getErrorStream(), stderrProcessor);
        if (stdInputs != null) {
            produceInput(process.getOutputStream(), stdInputs);
        }

        return process;
    }

    private static void consumeOutput(
            final InputStream stream, final Consumer<String> streamConsumer) {
        new Thread(
                        () -> {
                            try (BufferedReader bufferedReader =
                                    new BufferedReader(
                                            new InputStreamReader(
                                                    stream, StandardCharsets.UTF_8))) {
                                String line;
                                while ((line = bufferedReader.readLine()) != null) {
                                    streamConsumer.accept(line);
                                }
                            } catch (IOException e) {
                                LOG.error("Failure while processing process stdout/stderr.", e);
                            }
                        })
                .start();
    }

    private static void produceInput(final OutputStream stream, final String[] inputLines) {
        new Thread(
                        () -> {
                            // try with resource will close the OutputStream automatically,
                            // usually the process terminal will also be finished then.
                            try (PrintStream printStream =
                                    new PrintStream(stream, true, StandardCharsets.UTF_8.name())) {
                                for (String line : inputLines) {
                                    printStream.println(line);
                                }
                            } catch (IOException e) {
                                LOG.error("Failure while processing process stdin.", e);
                            }
                        })
                .start();
    }

    @Override
    public void close() throws IOException {
        if (process.isAlive()) {
            process.destroy();
            try {
                process.waitFor(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
