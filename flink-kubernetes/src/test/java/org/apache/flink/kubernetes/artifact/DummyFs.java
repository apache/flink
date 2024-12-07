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

package org.apache.flink.kubernetes.artifact;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;

import java.io.IOException;
import java.net.URI;

/** Dummy filesystem to test local artifact upload. */
public class DummyFs extends LocalFileSystem {

    static final URI FS_URI = URI.create("dummyfs:///");

    private int existsCallCounter;

    private int createCallCounter;

    @Override
    public URI getUri() {
        return FS_URI;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        ++existsCallCounter;
        return super.exists(f);
    }

    @Override
    public FSDataOutputStream create(Path filePath, WriteMode overwrite) throws IOException {
        ++createCallCounter;
        return super.create(filePath, overwrite);
    }

    public void resetCallCounters() {
        createCallCounter = 0;
        existsCallCounter = 0;
    }

    public int getExistsCallCounter() {
        return existsCallCounter;
    }

    public int getCreateCallCounter() {
        return createCallCounter;
    }
}
