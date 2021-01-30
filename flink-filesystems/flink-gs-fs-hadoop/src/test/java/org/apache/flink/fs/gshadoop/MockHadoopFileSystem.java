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

package org.apache.flink.fs.gshadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/** Mock implementation of a hadoop file system. */
class MockHadoopFileSystem extends FileSystem {

    private URI uri;
    Configuration conf;

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        this.uri = uri;
        this.conf = conf;
    }

    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public FSDataOutputStream create(
            Path path,
            FsPermission fsPermission,
            boolean b,
            int i,
            short i1,
            long l,
            Progressable progressable)
            throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable)
            throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        throw new NotImplementedException();
    }

    @Override
    public void setWorkingDirectory(Path path) {
        throw new NotImplementedException();
    }

    @Override
    public Path getWorkingDirectory() {
        throw new NotImplementedException();
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        throw new NotImplementedException();
    }
}
