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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.io.OutputStream;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A simple {@link OutputStreamBasedFileCompactor} implementation that directly copy the content of
 * the only input file to the output.
 */
@Internal
public class IdenticalFileCompactor extends ConcatFileCompactor {

    public IdenticalFileCompactor() {
        super();
    }

    public void compact(List<Path> inputFiles, OutputStream outputStream) throws Exception {
        checkState(inputFiles.size() == 1, "IdenticalFileCompactor can only copy one input file");
        super.compact(inputFiles, outputStream);
    }
}
