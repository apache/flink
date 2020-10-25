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
import org.apache.flink.util.TemporaryClassLoaderContext;

import java.io.IOException;
import java.net.URI;

/**
 * A wrapper around {@link FileSystemFactory} that ensures the plugin classloader is used for all {@link FileSystem}
 * operations.
 */
public class PluginFileSystemFactory implements FileSystemFactory {
	private final FileSystemFactory inner;
	private final ClassLoader loader;

	private PluginFileSystemFactory(final FileSystemFactory inner, final ClassLoader loader) {
		this.inner = inner;
		this.loader = loader;
	}

	public static PluginFileSystemFactory of(final FileSystemFactory inner) {
		return new PluginFileSystemFactory(inner, inner.getClass().getClassLoader());
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
		try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(loader)) {
			return new ClassLoaderFixingFileSystem(inner.create(fsUri), loader);
		}
	}

	@Override
	public String toString() {
		return String.format("Plugin %s", inner.getClass().getName());
	}

	static class ClassLoaderFixingFileSystem extends FileSystem {
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
			final FileStatus file,
			final long start,
			final long len) throws IOException {
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
		public FSDataOutputStream create(final Path f, final WriteMode overwriteMode) throws IOException {
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

		public FileSystem getInner() {
			return inner;
		}
	}
}
