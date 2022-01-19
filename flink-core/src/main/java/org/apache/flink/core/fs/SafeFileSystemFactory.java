/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.WrappingProxy;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A wrapper around {@link FileSystemFactory} that ensures the same classloader is used for all
 * {@link FileSystem} operations and that we don't leak the user classloader via inherited {@link
 * java.security.ProtectionDomain protection domains}.
 *
 * <h1>Avoiding protection domains leaks
 *
 * <p>To understand the protection domain leaks, let's revisit how the classloading works.
 *
 * <p>Every class that is loaded is assigned with a {@link java.security.ProtectionDomain protection
 * domain}. The {@code protection domain} contains reference to a {@link java.security.CodeSource}
 * (location of the class file + signing certificates), reference to a {@link ClassLoader} that has
 * loaded it, {@link java.security.Principal list of principals} and {@link java.security.Permission
 * permissions}.
 *
 * <p>The problematic part is the reference to the {@link ClassLoader} that has loaded our class.
 * The only way to get rid of the reference, would involve re-implementing a good part of the low
 * level ClassLoaders, because the important parts are intentionally marked as final.
 *
 * <p>Now to the actual problem. There is a thing called {@link java.security.AccessControlContext},
 * which can be obtained by calling {@link java.security.AccessController#getContext()}. This
 * context contains list of the current protection domains.
 *
 * <p>In pseudo-code, the list of protection domain is obtained as follows:
 *
 * <pre>
 * {@code getCurrentStackTrace() | getStackTraceElementClass | Class#getProtectionDomain | uniq (as in bash)}
 * </pre>
 *
 * <p>When a new thread is spawned it inherits the {@link java.security.AccessControlContext} of the
 * caller, which means that if we spawn any thread that outlives a job from a place that has been
 * called by user code (there is anything loaded by user classloader on stack), we'll leak the
 * reference to user classloader to this newly created thread. This happens especially with hadoop
 * filesystem, where the initialization of the filesystem spawn "cleaner threads" that are attached
 * to the JVM lifecycle.
 *
 * <p>The way to work around this, is to spawn initialize filesystems in a "safe" thread, that has
 * no potentially leaky protection domains, that the new thread can inherit.
 */
public class SafeFileSystemFactory implements FileSystemFactory {

    private static final ExecutorService EXECUTOR =
            Executors.newSingleThreadExecutor(
                    new ExecutorThreadFactory.Builder()
                            .setPoolName("filesystem-factory")
                            .setExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                            .build());

    static {
        EXECUTOR.execute(
                () -> {
                    // No-op. This is just to warm up thread in the right context.
                });
    }

    public static SafeFileSystemFactory of(final FileSystemFactory inner) {
        return new SafeFileSystemFactory(inner, inner.getClass().getClassLoader());
    }

    private final FileSystemFactory inner;
    private final ClassLoader loader;

    private SafeFileSystemFactory(final FileSystemFactory inner, final ClassLoader loader) {
        this.inner = inner;
        this.loader = loader;
    }

    @Override
    public String getScheme() {
        return inner.getScheme();
    }

    @Override
    public ClassLoader getClassLoader() {
        return inner.getClassLoader();
    }

    @Override
    public void configure(final Configuration config) {
        inner.configure(config);
    }

    @Override
    public FileSystem create(final URI fsUri) throws IOException {
        final Future<FileSystem> future =
                EXECUTOR.submit(
                        () -> {
                            synchronized (inner) {
                                try (TemporaryClassLoaderContext ignored =
                                        TemporaryClassLoaderContext.of(loader)) {
                                    return new ClassLoaderFixingFileSystem(
                                            inner.create(fsUri), loader);
                                }
                            }
                        });
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new IOException(
                    String.format("Interrupted while creating the '%s' file system.", fsUri));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException(
                    String.format("Unable to create the '%s' file system.", fsUri), e.getCause());
        }
    }

    @Override
    public String toString() {
        return String.format("Plugin %s", inner.getClass().getName());
    }

    static class ClassLoaderFixingFileSystem extends FileSystem
            implements WrappingProxy<FileSystem> {
        private final FileSystem inner;
        private final ClassLoader loader;

        private ClassLoaderFixingFileSystem(final FileSystem inner, final ClassLoader loader) {
            this.inner = inner;
            this.loader = loader;
        }

        @Override
        public Path getWorkingDirectory() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getWorkingDirectory();
            }
        }

        @Override
        public Path getHomeDirectory() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getHomeDirectory();
            }
        }

        @Override
        public URI getUri() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getUri();
            }
        }

        @Override
        public FileStatus getFileStatus(final Path f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getFileStatus(f);
            }
        }

        @Override
        public BlockLocation[] getFileBlockLocations(
                final FileStatus file, final long start, final long len) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getFileBlockLocations(file, start, len);
            }
        }

        @Override
        public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.open(f, bufferSize);
            }
        }

        @Override
        public FSDataInputStream open(final Path f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.open(f);
            }
        }

        @Override
        public RecoverableWriter createRecoverableWriter() throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.createRecoverableWriter();
            }
        }

        @Override
        public FileStatus[] listStatus(final Path f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.listStatus(f);
            }
        }

        @Override
        public boolean exists(final Path f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.exists(f);
            }
        }

        @Override
        public boolean delete(final Path f, final boolean recursive) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.delete(f, recursive);
            }
        }

        @Override
        public boolean mkdirs(final Path f) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.mkdirs(f);
            }
        }

        @Override
        public FSDataOutputStream create(final Path f, final WriteMode overwriteMode)
                throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.create(f, overwriteMode);
            }
        }

        @Override
        public boolean isDistributedFS() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.isDistributedFS();
            }
        }

        @Override
        public FileSystemKind getKind() {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.getKind();
            }
        }

        @Override
        public boolean rename(final Path src, final Path dst) throws IOException {
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
                return inner.rename(src, dst);
            }
        }

        @Override
        public FileSystem getWrappedDelegate() {
            return inner;
        }
    }
}
