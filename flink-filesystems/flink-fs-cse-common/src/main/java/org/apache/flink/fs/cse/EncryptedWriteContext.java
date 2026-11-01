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

package org.apache.flink.fs.cse;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.Immutable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Carries all context required to open an encrypted blob for writing.
 *
 * @see CseStreamFactory#openEncryptedWrite(org.apache.flink.core.fs.OutputStreamOpener,
 *     EncryptedWriteContext)
 */
@Internal
@Experimental
@Immutable
public interface EncryptedWriteContext {

    /** Returns the cloud object path. */
    String getFilePath();

    /**
     * Creates an {@link EncryptedWriteContext} for the given file path.
     *
     * @param filePath the cloud object path; must not be null
     * @return a new immutable {@link EncryptedWriteContext}
     */
    static EncryptedWriteContext of(final String filePath) {
        checkNotNull(filePath, "filePath");
        return () -> filePath;
    }
}
