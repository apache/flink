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

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.FileStatus;

/**
 * A wrapper of {@link FileStatus} just for ForSt. It delegates all the methods from {@link
 * FileStatus} and provide a version of primitive types. This class is used by JNI.
 */
public class ForStFileStatus {

    private final FileStatus fileStatus;

    public ForStFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public long getLen() {
        return fileStatus.getLen();
    }

    public long getBlockSize() {
        return fileStatus.getBlockSize();
    }

    public short getReplication() {
        return fileStatus.getReplication();
    }

    public long getModificationTime() {
        return fileStatus.getModificationTime();
    }

    public long getAccessTime() {
        return fileStatus.getAccessTime();
    }

    public boolean isDir() {
        return fileStatus.isDir();
    }

    public String getPath() {
        return fileStatus.getPath().toString();
    }
}
