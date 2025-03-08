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

package org.apache.flink.state.forst.fs.filemapping;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FSDataOutputStreamWrapper;

import java.io.IOException;

/** A {@link FSDataOutputStream} that is associated with a {@link MappingEntry}. */
public class FSDataOutputStreamWithEntry extends FSDataOutputStreamWrapper {

    private final MappingEntry entry;

    public FSDataOutputStreamWithEntry(FSDataOutputStream outputStream, MappingEntry entry) {
        super(outputStream);
        this.entry = entry;
    }

    @Override
    public void close() throws IOException {
        // Synchronize on the entry to ensure the atomicity of endWriting and close
        // also see invokers of MappingEntry.isWriting
        synchronized (entry) {
            super.close();
            entry.endWriting();
        }
    }
}
