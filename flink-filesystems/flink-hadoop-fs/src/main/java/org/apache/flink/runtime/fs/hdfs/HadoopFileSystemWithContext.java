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

package org.apache.flink.runtime.fs.hdfs;

import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.WrappingProxy;

import java.io.IOException;
import java.net.URI;

import static org.apache.flink.runtime.util.HadoopUtils.setCallerContext;

/** {@link FileSystem} implementation wrapping an {@link HadoopFileSystem} with context. */
public class HadoopFileSystemWithContext extends FileSystem implements WrappingProxy<FileSystem> {

    private final HadoopFileSystem hadoopFileSystem;
    private final FileSystemContext context;

    public HadoopFileSystemWithContext(
            HadoopFileSystem hadoopFileSystem, FileSystemContext context) {
        this.hadoopFileSystem = hadoopFileSystem;
        this.context = context;
    }

    private void setCallerContext() {
        HadoopUtils.setCallerContext(context);
    }

    @Override
    public Path getWorkingDirectory() {
        setCallerContext();
        return hadoopFileSystem.getWorkingDirectory();
    }

    @Override
    public Path getHomeDirectory() {
        setCallerContext();
        return hadoopFileSystem.getHomeDirectory();
    }

    @Override
    public URI getUri() {
        setCallerContext();
        return hadoopFileSystem.getUri();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        setCallerContext();
        return hadoopFileSystem.getFileStatus(f);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)
            throws IOException {
        setCallerContext();
        return hadoopFileSystem.getFileBlockLocations(file, start, len);
    }

    @Override
    public HadoopDataInputStream open(Path f, int bufferSize) throws IOException {
        setCallerContext();
        return hadoopFileSystem.open(f, bufferSize);
    }

    @Override
    public HadoopDataInputStream open(Path f) throws IOException {
        setCallerContext();
        return hadoopFileSystem.open(f);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        setCallerContext();
        return hadoopFileSystem.listStatus(f);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        setCallerContext();
        return hadoopFileSystem.delete(f, recursive);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        setCallerContext();
        return hadoopFileSystem.mkdirs(f);
    }

    @Override
    public HadoopDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException {
        setCallerContext();
        return hadoopFileSystem.create(f, overwriteMode);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        setCallerContext();
        return hadoopFileSystem.rename(src, dst);
    }

    @Override
    public boolean isDistributedFS() {
        setCallerContext();
        return hadoopFileSystem.isDistributedFS();
    }

    @Override
    public FileSystem getWrappedDelegate() {
        setCallerContext();
        return hadoopFileSystem;
    }
}
