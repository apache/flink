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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Cloud-agnostic opener that creates an output stream for a write operation.
 *
 * <p>The implementation captures all cloud-specific state (client, path, bucket) in its closure.
 * The {@link WriteContext} provides metadata to attach to the cloud object before the stream is
 * opened.
 */
@Internal
@Experimental
@NotThreadSafe
@FunctionalInterface
public interface OutputStreamOpener {

    /**
     * Opens an output stream for the write operation described by {@code ctx}.
     *
     * @param ctx context for this write, including metadata to attach to the cloud object
     * @return an output stream for writing the file content
     * @throws IOException if the stream cannot be opened
     */
    OutputStream open(WriteContext ctx) throws IOException;
}
