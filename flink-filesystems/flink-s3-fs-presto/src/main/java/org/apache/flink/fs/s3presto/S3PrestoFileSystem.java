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

package org.apache.flink.fs.s3presto;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.WriteOptions;
import org.apache.flink.runtime.fs.hdfs.HadoopBlockLocation;
import org.apache.flink.runtime.fs.hdfs.HadoopDataInputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopDataOutputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopFileStatus;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A Flink FileSystem against S3, wrapping the Presto Hadoop S3 File System implementation.
 *
 * <p>This class bases heavily on the {@link org.apache.flink.runtime.fs.hdfs.HadoopFileSystem} class.
 * Code is copied here for the sake of minimal changes to the original class within a minor release.
 */
class S3PrestoFileSystem extends FileSystem {

	/** The wrapped Hadoop File System. */
	private final org.apache.hadoop.fs.FileSystem fs;

	@Nullable
	private final String entropyInjectionKey;

	private final int entropyLength;

	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
	 */
	public S3PrestoFileSystem(org.apache.hadoop.fs.FileSystem hadoopFileSystem) {
		this(hadoopFileSystem, null, -1);
	}

	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * <p>This constructor additionally configures the entropy injection for the file system.
	 *
	 * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
	 * @param entropyInjectionKey The substring that will be replaced by entropy or removed.
	 * @param entropyLength The number of random alphanumeric characters to inject as entropy.
	 */
	public S3PrestoFileSystem(
			org.apache.hadoop.fs.FileSystem hadoopFileSystem,
			@Nullable String entropyInjectionKey,
			int entropyLength) {

		if (entropyInjectionKey != null && entropyLength <= 0) {
			throw new IllegalArgumentException("Entropy length must be >= 0 when entropy injection key is set");
		}

		this.fs = checkNotNull(hadoopFileSystem, "hadoopFileSystem");
		this.entropyInjectionKey = entropyInjectionKey;
		this.entropyLength = entropyLength;
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	public org.apache.hadoop.fs.FileSystem getHadoopFileSystem() {
		return fs;
	}

	@Nullable
	public String getEntropyInjectionKey() {
		return entropyInjectionKey;
	}

	public int getEntropyLength() {
		return entropyLength;
	}

	// ------------------------------------------------------------------------
	//  file system methods
	// ------------------------------------------------------------------------

	@Override
	public Path getWorkingDirectory() {
		return new Path(fs.getWorkingDirectory().toUri());
	}

	public Path getHomeDirectory() {
		return new Path(fs.getHomeDirectory().toUri());
	}

	@Override
	public URI getUri() {
		return fs.getUri();
	}

	@Override
	public FileStatus getFileStatus(final Path f) throws IOException {
		org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(toHadoopPath(f));
		return new HadoopFileStatus(status);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(
			final FileStatus file,
			final long start,
			final long len) throws IOException {

		if (!(file instanceof HadoopFileStatus)) {
			throw new IOException("file is not an instance of HadoopFileStatus");
		}

		final HadoopFileStatus f = (HadoopFileStatus) file;

		final org.apache.hadoop.fs.BlockLocation[] blkLocations = fs.getFileBlockLocations(f.getInternalFileStatus(),
				start, len);

		// Wrap up HDFS specific block location objects
		final HadoopBlockLocation[] distBlkLocations = new HadoopBlockLocation[blkLocations.length];
		for (int i = 0; i < distBlkLocations.length; i++) {
			distBlkLocations[i] = new HadoopBlockLocation(blkLocations[i]);
		}

		return distBlkLocations;
	}

	@Override
	public HadoopDataInputStream open(final Path f, final int bufferSize) throws IOException {
		final org.apache.hadoop.fs.Path path = toHadoopPath(f);
		final org.apache.hadoop.fs.FSDataInputStream fdis = this.fs.open(path, bufferSize);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	public HadoopDataInputStream open(final Path f) throws IOException {
		final org.apache.hadoop.fs.Path path = toHadoopPath(f);
		final org.apache.hadoop.fs.FSDataInputStream fdis = fs.open(path);
		return new HadoopDataInputStream(fdis);
	}

	@Override
	public HadoopDataOutputStream create(final Path f, final WriteMode overwrite) throws IOException {
		final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream =
				fs.create(toHadoopPath(f), overwrite == WriteMode.OVERWRITE);
		return new HadoopDataOutputStream(fsDataOutputStream);
	}

	@Override
	public FSDataOutputStream create(final Path f, final WriteOptions options) throws IOException {
		final org.apache.hadoop.fs.Path path = options.isInjectEntropy()
				? toHadoopPathInjectEntropy(f)
				: toHadoopPath(f);

		final org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream = fs.create(
				path, options.getOverwrite() == WriteMode.OVERWRITE);

		return new HadoopDataOutputStream(fsDataOutputStream);
	}

	@Override
	public boolean delete(final Path f, final boolean recursive) throws IOException {
		return fs.delete(toHadoopPath(f), recursive);
	}

	@Override
	public boolean exists(Path f) throws IOException {
		return fs.exists(toHadoopPath(f));
	}

	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {
		final org.apache.hadoop.fs.FileStatus[] hadoopFiles = fs.listStatus(toHadoopPath(f));
		final FileStatus[] files = new FileStatus[hadoopFiles.length];

		// Convert types
		for (int i = 0; i < files.length; i++) {
			files[i] = new HadoopFileStatus(hadoopFiles[i]);
		}

		return files;
	}

	@Override
	public boolean mkdirs(final Path f) throws IOException {
		return fs.mkdirs(toHadoopPath(f));
	}

	@Override
	public boolean rename(final Path src, final Path dst) throws IOException {
		return fs.rename(toHadoopPath(src), toHadoopPath(dst));
	}

	@SuppressWarnings("deprecation")
	@Override
	public long getDefaultBlockSize() {
		return fs.getDefaultBlockSize();
	}

	@Override
	public boolean isDistributedFS() {
		return true;
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.OBJECT_STORE;
	}

	// ------------------------------------------------------------------------
	//  entropy utilities
	// ------------------------------------------------------------------------

	@VisibleForTesting
	org.apache.hadoop.fs.Path toHadoopPath(Path path) throws IOException {
		return rewritePathForEntropyKey(path, false);
	}

	@VisibleForTesting
	org.apache.hadoop.fs.Path toHadoopPathInjectEntropy(Path path) throws IOException {
		return rewritePathForEntropyKey(path, true);
	}

	private org.apache.hadoop.fs.Path rewritePathForEntropyKey(Path path, boolean addEntropy) throws IOException {
		if (entropyInjectionKey == null) {
			return convertToHadoopPath(path);
		}
		else {
			final URI originalUri = path.toUri();
			final String checkpointPath = originalUri.getPath();

			final int indexOfKey = checkpointPath.indexOf(entropyInjectionKey);
			if (indexOfKey == -1) {
				return convertToHadoopPath(path);
			}
			else {
				final StringBuilder buffer = new StringBuilder(checkpointPath.length());
				buffer.append(checkpointPath, 0, indexOfKey);

				if (addEntropy) {
					StringUtils.appendRandomAlphanumericString(ThreadLocalRandom.current(), buffer, entropyLength);
				}

				buffer.append(checkpointPath, indexOfKey + entropyInjectionKey.length(), checkpointPath.length());

				final String rewrittenPath = buffer.toString();
				try {
					return convertToHadoopPath(new URI(
							originalUri.getScheme(),
							originalUri.getAuthority(),
							rewrittenPath,
							originalUri.getQuery(),
							originalUri.getFragment()));
				}
				catch (URISyntaxException e) {
					// this should actually never happen, because the URI was valid before
					throw new IOException("URI format error while processing path for entropy injection", e);
				}
			}
		}
	}

	private static org.apache.hadoop.fs.Path convertToHadoopPath(URI uri) {
		return new org.apache.hadoop.fs.Path(uri);
	}

	private static org.apache.hadoop.fs.Path convertToHadoopPath(Path path) {
		return convertToHadoopPath(path.toUri());
	}
}
