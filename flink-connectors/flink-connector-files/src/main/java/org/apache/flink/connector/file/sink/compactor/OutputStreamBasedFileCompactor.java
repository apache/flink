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
import org.apache.flink.util.CloseShieldOutputStream;

import java.io.OutputStream;
import java.util.List;

/**
 * Base class for {@link FileCompactor} implementations that write the compacting file by a output
 * stream.
 */
@PublicEvolving
public abstract class OutputStreamBasedFileCompactor implements FileCompactor {

    public void compact(List<Path> inputFiles, OutputStream outputStream) throws Exception {
        // The outputStream should not be closed here.
        CloseShieldOutputStream shieldOutputStream = new CloseShieldOutputStream(outputStream);
        doCompact(inputFiles, shieldOutputStream);
    }

    protected abstract void doCompact(List<Path> inputFiles, OutputStream outputStream)
            throws Exception;
}
