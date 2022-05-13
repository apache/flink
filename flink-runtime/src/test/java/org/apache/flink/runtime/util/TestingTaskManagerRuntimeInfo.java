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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;

import java.io.File;
import java.net.InetAddress;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** TaskManagerRuntimeInfo implementation for testing purposes. */
public class TestingTaskManagerRuntimeInfo implements TaskManagerRuntimeInfo {

    private final Configuration configuration;
    private final String[] tmpDirectories;
    private final String taskManagerExternalAddress;
    private final File tmpWorkingDirectory;

    public TestingTaskManagerRuntimeInfo() {
        this(
                new Configuration(),
                EnvironmentInformation.getTemporaryFileDirectory()
                        .split(",|" + File.pathSeparator));
    }

    public TestingTaskManagerRuntimeInfo(Configuration configuration) {
        this(configuration, EnvironmentInformation.getTemporaryFileDirectory());
    }

    public TestingTaskManagerRuntimeInfo(Configuration configuration, File tmpWorkingDirectory) {
        this(
                configuration,
                new String[] {EnvironmentInformation.getTemporaryFileDirectory()},
                InetAddress.getLoopbackAddress().getHostAddress(),
                tmpWorkingDirectory);
    }

    public TestingTaskManagerRuntimeInfo(Configuration configuration, String tmpDirectory) {
        this(configuration, new String[] {checkNotNull(tmpDirectory)});
    }

    public TestingTaskManagerRuntimeInfo(Configuration configuration, String[] tmpDirectories) {
        this(
                configuration,
                tmpDirectories,
                InetAddress.getLoopbackAddress().getHostAddress(),
                new File(
                        EnvironmentInformation.getTemporaryFileDirectory(),
                        "tmp_" + UUID.randomUUID()));
    }

    public TestingTaskManagerRuntimeInfo(
            Configuration configuration,
            String[] tmpDirectories,
            String taskManagerExternalAddress,
            File tmpWorkingDirectory) {
        this.configuration = configuration;
        this.tmpDirectories = tmpDirectories;
        this.taskManagerExternalAddress = taskManagerExternalAddress;
        this.tmpWorkingDirectory = tmpWorkingDirectory;
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public String[] getTmpDirectories() {
        return tmpDirectories;
    }

    @Override
    public boolean shouldExitJvmOnOutOfMemoryError() {
        // never kill the JVM in tests
        return false;
    }

    @Override
    public String getTaskManagerExternalAddress() {
        return taskManagerExternalAddress;
    }

    @Override
    public File getTmpWorkingDirectory() {
        return tmpWorkingDirectory;
    }
}
