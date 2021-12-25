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

package org.apache.flink.test.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.CommonTestUtils.PipeForwarder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map.Entry;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility class wrapping {@link ProcessBuilder} and pre-configuring it with common options. */
public class TestProcessBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TestProcessBuilder.class);

    private final String javaCommand = checkNotNull(getJavaCommandPath());

    private final ArrayList<String> jvmArgs = new ArrayList<>();
    private final ArrayList<String> mainClassArgs = new ArrayList<>();

    private final String mainClass;

    private MemorySize jvmMemory = MemorySize.parse("80mb");

    private boolean withCleanEnvironment = false;

    public TestProcessBuilder(String mainClass) throws IOException {
        File tempLogFile =
                File.createTempFile(getClass().getSimpleName() + "-", "-log4j.properties");
        tempLogFile.deleteOnExit();
        CommonTestUtils.printLog4jDebugConfig(tempLogFile);

        jvmArgs.add("-Dlog.level=DEBUG");
        jvmArgs.add("-Dlog4j.configurationFile=file:" + tempLogFile.getAbsolutePath());
        jvmArgs.add("-classpath");
        jvmArgs.add(getCurrentClasspath());

        this.mainClass = mainClass;
    }

    public TestProcess start() throws IOException {
        final ArrayList<String> commands = new ArrayList<>();

        commands.add(javaCommand);
        commands.add(String.format("-Xms%dm", jvmMemory.getMebiBytes()));
        commands.add(String.format("-Xmx%dm", jvmMemory.getMebiBytes()));
        commands.addAll(jvmArgs);
        commands.add(mainClass);
        commands.addAll(mainClassArgs);

        StringWriter processOutput = new StringWriter();
        StringWriter errorOutput = new StringWriter();
        LOG.info("Starting process with commands {}", commands);
        final ProcessBuilder processBuilder = new ProcessBuilder(commands);
        if (withCleanEnvironment) {
            processBuilder.environment().clear();
        }
        Process process = processBuilder.start();
        new PipeForwarder(process.getInputStream(), processOutput);
        new PipeForwarder(process.getErrorStream(), errorOutput);

        return new TestProcess(process, processOutput, errorOutput);
    }

    public TestProcessBuilder setJvmMemory(MemorySize jvmMemory) {
        this.jvmMemory = jvmMemory;
        return this;
    }

    public TestProcessBuilder addJvmArg(String arg) {
        jvmArgs.add(arg);
        return this;
    }

    public TestProcessBuilder addMainClassArg(String arg) {
        mainClassArgs.add(arg);
        return this;
    }

    public TestProcessBuilder addConfigAsMainClassArgs(Configuration config) {
        for (Entry<String, String> keyValue : config.toMap().entrySet()) {
            addMainClassArg("--" + keyValue.getKey());
            addMainClassArg(keyValue.getValue());
        }
        return this;
    }

    public TestProcessBuilder withCleanEnvironment() {
        withCleanEnvironment = true;
        return this;
    }

    /** {@link Process} with it's {@code processOutput}. */
    public static class TestProcess {
        private final Process process;
        private final StringWriter processOutput;
        private final StringWriter errorOutput;

        public TestProcess(Process process, StringWriter processOutput, StringWriter errorOutput) {
            this.process = process;
            this.processOutput = processOutput;
            this.errorOutput = errorOutput;
        }

        public Process getProcess() {
            return process;
        }

        public StringWriter getProcessOutput() {
            return processOutput;
        }

        public StringWriter getErrorOutput() {
            return errorOutput;
        }

        public void destroy() {
            process.destroy();
        }
    }
}
