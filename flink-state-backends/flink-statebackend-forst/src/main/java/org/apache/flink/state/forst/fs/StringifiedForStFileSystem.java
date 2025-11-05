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

import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * A {@link ForStFlinkFileSystem} stringifies all the parameters of all methods. This class is used
 * by JNI.
 */
public class StringifiedForStFileSystem {

    private ForStFlinkFileSystem fileSystem;

    public StringifiedForStFileSystem(ForStFlinkFileSystem fileSystem) {
        this.fileSystem = ForStFileSystemUtils.tryDecorate(fileSystem);
    }

    public static StringifiedForStFileSystem get(String uri) throws IOException {
        return new StringifiedForStFileSystem(ForStFlinkFileSystem.get(URI.create(uri)));
    }

    public boolean exists(final String path) throws IOException {
        return fileSystem.exists(new Path(path));
    }

    public ForStFileStatus getFileStatus(String path) throws IOException {
        return new ForStFileStatus(fileSystem.getFileStatus(new Path(path)));
    }

    public ForStFileStatus[] listStatus(String path) throws IOException {
        return Arrays.stream(fileSystem.listStatus(new Path(path)))
                .map(ForStFileStatus::new)
                .toArray(ForStFileStatus[]::new);
    }

    public boolean delete(String path, boolean recursive) throws IOException {
        return fileSystem.delete(new Path(path), recursive);
    }

    public boolean mkdirs(String path) throws IOException {
        return fileSystem.mkdirs(new Path(path));
    }

    public boolean rename(String src, String dst) throws IOException {
        return fileSystem.rename(new Path(src), new Path(dst));
    }

    public ByteBufferReadableFSDataInputStream open(String path) throws IOException {
        return fileSystem.open(new Path(path));
    }

    public ByteBufferWritableFSDataOutputStream create(String path) throws IOException {
        return fileSystem.create(new Path(path));
    }

    public int link(String src, String dst) throws IOException {
        return fileSystem.link(new Path(src), new Path(dst));
    }
}
