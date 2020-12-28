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

package org.apache.flink.examples.java.distcp;

import org.apache.flink.core.fs.Path;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/** A Java POJO that represents a task for copying a single file. */
public class FileCopyTask implements Serializable {

    private static final long serialVersionUID = -8760082278978316032L;

    private final Path path;
    private final String relativePath;

    public FileCopyTask(Path path, String relativePath) {
        if (StringUtils.isEmpty(relativePath)) {
            throw new IllegalArgumentException("Relative path should not be empty for: " + path);
        }
        this.path = path;
        this.relativePath = relativePath;
    }

    public Path getPath() {
        return path;
    }

    public String getRelativePath() {
        return relativePath;
    }

    @Override
    public String toString() {
        return "FileCopyTask{" + "path=" + path + ", relativePath='" + relativePath + '\'' + '}';
    }
}
