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

package org.apache.flink.state.forst;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * A local file system that follows the directory semantics of S3-like object stores (the Presto and
 * the native Flink S3 file systems): directories are not real entities, {@link #mkdirs} is a no-op
 * that reports success, writing a file implicitly creates its parent "prefix", and a directory only
 * exists while at least one file exists somewhere below it.
 */
public class ObjectStoreLikeFileSystem extends LocalFileSystem {

    public static final String SCHEME = "objstore";

    @Override
    public URI getUri() {
        return URI.create(SCHEME + ":///");
    }

    @Override
    public boolean mkdirs(Path f) {
        return true;
    }

    @Override
    public FSDataOutputStream create(Path filePath, WriteMode overwrite) throws IOException {
        createPhysicalParent(filePath);
        return super.create(filePath, overwrite);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        createPhysicalParent(dst);
        return super.rename(src, dst);
    }

    @Override
    public boolean exists(Path f) {
        return existsAsObjectOrPrefix(pathToFile(f));
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        File file = pathToFile(f);
        if (!existsAsObjectOrPrefix(file)) {
            throw new FileNotFoundException("No object or prefix for " + f);
        }
        return new LocalFileStatus(file, this);
    }

    @Override
    public FileStatus[] listStatus(Path f) {
        File dir = pathToFile(f);
        if (!existsAsObjectOrPrefix(dir)) {
            return new FileStatus[0];
        }
        if (dir.isFile()) {
            return new FileStatus[] {new LocalFileStatus(dir, this)};
        }
        List<FileStatus> result = new ArrayList<>();
        File[] children = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                if (existsAsObjectOrPrefix(child)) {
                    result.add(new LocalFileStatus(child, this));
                }
            }
        }
        return result.toArray(new FileStatus[0]);
    }

    private void createPhysicalParent(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null) {
            super.mkdirs(parent);
        }
    }

    private static boolean existsAsObjectOrPrefix(File file) {
        if (file.isFile()) {
            return true;
        }
        File[] children = file.listFiles();
        if (children == null) {
            return false;
        }
        for (File child : children) {
            if (existsAsObjectOrPrefix(child)) {
                return true;
            }
        }
        return false;
    }

    /** Factory for {@link ObjectStoreLikeFileSystem}, registered for tests via ServiceLoader. */
    public static final class Factory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI fsUri) {
            return new ObjectStoreLikeFileSystem();
        }
    }
}
