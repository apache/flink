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
    private static final ThreadLocal<FileSystemContext> CONTEXTS = new InheritableThreadLocal<>();

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

        try {
            Class<?> callerContextClass = Class.forName("org.apache.hadoop.ipc.CallerContext");
            Class<?> builderClass = Class.forName("org.apache.hadoop.ipc.CallerContext$Builder");
            Object builderInst = builderClass.getConstructor(String.class).newInstance(context);
            Object hdfsContext = builderClass.getMethod("build").invoke(builderInst);
            callerContextClass
                    .getMethod("setCurrent", callerContextClass)
                    .invoke(null, hdfsContext);
        } catch (ClassNotFoundException
                | java.lang.reflect.InvocationTargetException
                | IllegalAccessException
                | NoSuchMethodException
                | InstantiationException e) {
            throw new RuntimeException(e);
        }
        CONTEXTS.set(newContext);
    }

    static FileSystem wrapWithContextWhenActivated(FileSystem fs) {
        final FileSystemContext ctx = CONTEXTS.get();
        return ctx != null && fs instanceof ContextWrapperFileSystem
                ? ((ContextWrapperFileSystem) fs).wrap(fs, ctx)
                : fs;
    }

    private final String name;

    private FileSystemContext(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
