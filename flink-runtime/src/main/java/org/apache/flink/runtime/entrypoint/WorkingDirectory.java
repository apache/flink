/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Class that manages a working directory for a process/instance. When being instantiated, this
 * class makes sure that the specified working directory exists.
 */
public class WorkingDirectory {
    private final File root;
    private final File tmp;
    private final File localState;
    private final File blobStorage;

    private WorkingDirectory(File root) throws IOException {
        this.root = root;
        createDirectory(root);

        this.tmp = new File(root, "tmp");
        createDirectory(tmp);
        FileUtils.cleanDirectory(tmp);

        localState = new File(root, "localState");
        createDirectory(localState);

        blobStorage = new File(root, "blobStorage");
        createDirectory(blobStorage);
    }

    private static void createDirectory(File directory) throws IOException {
        if (!directory.mkdirs() && !directory.exists()) {
            throw new IOException(
                    String.format("Could not create the working directory %s.", directory));
        }
    }

    public void delete() throws IOException {
        FileUtils.deleteDirectory(root);
    }

    public File getTmpDirectory() {
        return tmp;
    }

    public File getLocalStateDirectory() {
        return localState;
    }

    public WorkingDirectory createSubWorkingDirectory(String directoryName) throws IOException {
        return createIn(root, directoryName);
    }

    @Override
    public String toString() {
        return String.format("WorkingDirectory(%s)", root.toString());
    }

    public static WorkingDirectory create(File workingDirectory) throws IOException {
        return new WorkingDirectory(workingDirectory);
    }

    public static WorkingDirectory createIn(File parentDirectory, String directoryName)
            throws IOException {
        return create(new File(parentDirectory, directoryName));
    }

    public File getBlobStorageDirectory() {
        return blobStorage;
    }
}
