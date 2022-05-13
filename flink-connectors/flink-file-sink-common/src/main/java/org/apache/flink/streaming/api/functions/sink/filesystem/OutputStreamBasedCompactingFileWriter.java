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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The compactors use the {@link OutputStreamBasedCompactingFileWriter} to directly write a
 * compacting file as an {@link OutputStream}.
 */
@Internal
public interface OutputStreamBasedCompactingFileWriter extends CompactingFileWriter {
    /**
     * Gets the output stream underlying the writer. The close method of the returned stream should
     * never be called.
     *
     * @return The output stream to write the compacting file.
     * @throws IOException Thrown if acquiring the stream fails.
     */
    OutputStream asOutputStream() throws IOException;
}
