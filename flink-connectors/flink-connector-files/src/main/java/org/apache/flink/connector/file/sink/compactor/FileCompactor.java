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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;

import java.io.Serializable;
import java.util.List;

/**
 * The {@link FileCompactor} is responsible for compacting files into one file.
 *
 * <p>The {@link FileCompactor} should declare which type of {@link CompactingFileWriter} is
 * required, and invoke the writer correspondingly.
 */
@PublicEvolving
public interface FileCompactor extends Serializable {

    /** @return the {@link CompactingFileWriter} type the compactor will use. */
    CompactingFileWriter.Type getWriterType();

    /**
     * Compact the given files into one file.
     *
     * @param inputFiles the files to be compacted.
     * @param writer the writer to write the compacted file.
     * @throws Exception Thrown if an exception occurs during the compacting.
     */
    void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception;
}
