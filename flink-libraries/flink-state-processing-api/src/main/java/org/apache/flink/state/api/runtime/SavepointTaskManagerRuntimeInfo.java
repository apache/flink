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

package org.apache.flink.state.api.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Preconditions;

import java.io.File;

/**
 * A minimally implemented {@link TaskManagerRuntimeInfo} that provides the functionality required
 * to run the {@code state-processor-api}.
 */
class SavepointTaskManagerRuntimeInfo implements TaskManagerRuntimeInfo {
    private final File tmpWorkingDirectory;

    SavepointTaskManagerRuntimeInfo(File tmpWorkingDirectory) {
        this.tmpWorkingDirectory = Preconditions.checkNotNull(tmpWorkingDirectory);
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration();
    }

    @Override
    public String[] getTmpDirectories() {
        throw new UnsupportedOperationException(
                "Tmp directories are not available with the SavepointTaskManagerRuntimeInfo");
    }

    @Override
    public boolean shouldExitJvmOnOutOfMemoryError() {
        return false;
    }

    @Override
    public String getTaskManagerExternalAddress() {
        throw new UnsupportedOperationException(
                "Getting external address of task manager is not supported in SavepointTaskManagerRuntimeInfo");
    }

    @Override
    public File getTmpWorkingDirectory() {
        return tmpWorkingDirectory;
    }
}
