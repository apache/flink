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
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedCompactingFileWriter;
import org.apache.flink.util.CloseShieldOutputStream;

import java.io.OutputStream;
import java.util.List;

/**
 * Base class for {@link FileCompactor} implementations that use the {@link
 * OutputStreamBasedCompactingFileWriter}.
 */
@PublicEvolving
public abstract class OutputStreamBasedFileCompactor implements FileCompactor {
    @Override
    public final CompactingFileWriter.Type getWriterType() {
        return CompactingFileWriter.Type.OUTPUT_STREAM;
    }

    @Override
    public void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception {
        // The outputStream returned by OutputStreamBasedCompactingFileWriter#asOutputStream should
        // not be closed here.
        CloseShieldOutputStream outputStream =
                new CloseShieldOutputStream(
                        ((OutputStreamBasedCompactingFileWriter) writer).asOutputStream());
        doCompact(inputFiles, outputStream);
    }

    protected abstract void doCompact(List<Path> inputFiles, OutputStream outputStream)
            throws Exception;
}
