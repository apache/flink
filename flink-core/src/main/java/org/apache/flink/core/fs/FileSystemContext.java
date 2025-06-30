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

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A context for file system operations that can be used to set caller context or other metadata.
 * This context is thread-local and can be initialized for each thread.
 *
 * <p>It is used to provide additional information to the file system operations, such as the caller
 * context in Hadoop's HDFS.
 */
@Experimental
public class FileSystemContext {
    private static final ThreadLocal<FileSystemContext> CONTEXTS = new ThreadLocal<>();

    @Internal
    public static void initializeContextForThread(String context) {
        final FileSystemContext oldContext = CONTEXTS.get();

        checkState(
                null == oldContext,
                "Found an existing FileSystem context for this thread: %s "
                        + "This may indicate an accidental repeated initialization, or a leak of the"
                        + "(Inheritable)ThreadLocal through a ThreadPool.",
                oldContext);

        final FileSystemContext newContext = new FileSystemContext(context);
        CONTEXTS.set(newContext);
    }

    @Internal
    public static void clearContext() {
        FileSystemContext context1 = CONTEXTS.get();
        if (null != context1) {
            CONTEXTS.remove();
        }
    }

    public static FileSystem addContext(FileSystem fs) {
        final FileSystemContext ctx = CONTEXTS.get();
        return ctx != null ? ((ContextFileSystem) fs).addContext(fs, ctx) : fs;
    }

    private final String context;

    public FileSystemContext(String context) {
        this.context = context;
    }

    public String getContext() {
        return context;
    }
}
