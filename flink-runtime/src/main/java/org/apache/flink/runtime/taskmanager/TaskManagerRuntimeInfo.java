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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

/** Interface to access {@link TaskExecutor} information. */
public interface TaskManagerRuntimeInfo {

    /**
     * Gets the configuration that the TaskManager was started with.
     *
     * @return The configuration that the TaskManager was started with.
     */
    Configuration getConfiguration();

    /**
     * Gets the list of temporary file directories.
     *
     * @return The list of temporary file directories.
     */
    String[] getTmpDirectories();

    /**
     * Checks whether the TaskManager should exit the JVM when the task thread throws an
     * OutOfMemoryError.
     *
     * @return True to terminate the JVM on an OutOfMemoryError, false otherwise.
     */
    boolean shouldExitJvmOnOutOfMemoryError();

    /**
     * Gets the external address of the TaskManager.
     *
     * @return The external address of the TaskManager.
     */
    String getTaskManagerExternalAddress();

    /**
     * Gets the bind address of the Taskmanager.
     *
     * @return The bind address of the TaskManager.
     */
    default String getTaskManagerBindAddress() {
        return getConfiguration().getString(TaskManagerOptions.BIND_HOST);
    }
}
