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

/*
 * Parts of earlier versions of this file were based on source code from the
 * Hadoop Project (http://hadoop.apache.org/), licensed by the Apache Software Foundation (ASF)
 * under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The class {@code LocalFileSystem} is an implementation of the {@link FileSystem} interface
 * for the local file system of the machine where the JVM runs.
 */
@Internal
public class LocalFileSystem extends FileSystem {

	private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystem.class);

	/** The URI representing the local file system. */
	private static final URI LOCAL_URI = OperatingSystem.isWindows() ? URI.create("file:/") : URI.create("file:///");

	/** The shared instance of the local file system. */
	private static final LocalFileSystem INSTANCE = new LocalFileSystem();

	/** Path pointing to the current working directory.
	 * Because Paths are not immutable, we cannot cache the proper path here */
	private final URI workingDir;

	/** Path pointing to the current user home directory.
	 * Because Paths are not immutable, we cannot cache the proper path here. */
	private final URI homeDir;

	/** The host name of this machine. */
	private final String hostName;

	/**
	 * Constructs a new <code>LocalFileSystem</code> object.
	 */
	public LocalFileSystem() {
		this.workingDir = new File(System.getProperty("user.dir")).toURI();
		this.homeDir = new File(System.getProperty("user.home")).toURI();

		String tmp = "unknownHost";
		try {
			tmp = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			LOG.error("Could not resolve local host", e);
		}
		this.hostName = tmp;
	}

	// ------------------------------------------------------------------------

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return new BlockLocation[] {
				new LocalBlockLocation(hostName, file.getLen())
		};
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		final File path = pathToFile(f);
		if (path.exists()) {
			return new LocalFileStatus(path, this);
		}
		else {
			throw new FileNotFoundException("File " + f + " does not exist or the user running "
					+ "Flink ('" + System.getProperty("user.name") + "') has insufficient permissions to access it.");
		}
	}

	@Override
	public URI getUri() {
		return LOCAL_URI;
	}

	@Override
	public Path getWorkingDirectory() {
		return new Path(workingDir);
	}

	@Override
	public Path getHomeDirectory() {
		return new Path(homeDir);
	}

	@Override
	public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
		return open(f);
	}

	@Override
	public FSDataInputStream open(final Path f) throws IOException {
		final File file = pathToFile(f);
		return new LocalDataInputStream(file);
	}

	@Override
	public LocalRecoverableWriter createRecoverableWriter() throws IOException {
		return new LocalRecoverableWriter(this);
	}

	@Override
	public boolean exists(Path f) throws IOException {
		final File path = pathToFile(f);
		return path.exists();
	}

	@Override
	public FileStatus[] listStatus(final Path f) throws IOException {

		final File localf = pathToFile(f);
		FileStatus[] results;

		if (!localf.exists()) {
			return null;
		}
		if (localf.isFile()) {
			return new FileStatus[] { new LocalFileStatus(localf, this) };
		}

		final String[] names = localf.list();
		if (names == null) {
			return null;
		}
		results = new FileStatus[names.length];
		for (int i = 0; i < names.length; i++) {
			results[i] = getFileStatus(new Path(f, names[i]));
		}

		return results;
	}

	@Override
	public boolean delete(final Path f, final boolean recursive) throws IOException {

		final File file = pathToFile(f);
		if (file.isFile()) {
			return file.delete();
		} else if ((!recursive) && file.isDirectory()) {
			File[] containedFiles = file.listFiles();
			if (containedFiles == null) {
				throw new IOException("Directory " + file.toString() + " does not exist or an I/O error occurred");
			} else if (containedFiles.length != 0) {
				throw new IOException("Directory " + file.toString() + " is not empty");
			}
		}

		return delete(file);
	}

	/**
	 * Deletes the given file or directory.
	 *
	 * @param f
	 *        the file to be deleted
	 * @return <code>true</code> if all files were deleted successfully, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an error occurred while deleting the files/directories
	 */
	private boolean delete(final File f) throws IOException {

		if (f.isDirectory()) {
			final File[] files = f.listFiles();
			if (files != null) {
				for (File file : files) {
					final boolean del = delete(file);
					if (!del) {
						return false;
					}
				}
			}
		} else {
			return f.delete();
		}

		// Now directory is empty
		return f.delete();
	}

	/**
	 * Recursively creates the directory specified by the provided path.
	 *
	 * @return <code>true</code>if the directories either already existed or have been created successfully,
	 *         <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an error occurred while creating the directory/directories
	 */
	@Override
	public boolean mkdirs(final Path f) throws IOException {
		checkNotNull(f, "path is null");
		return mkdirsInternal(pathToFile(f));
	}

	private boolean mkdirsInternal(File file) throws IOException {
		if (file.isDirectory()) {
				return true;
		}
		else if (file.exists() && !file.isDirectory()) {
			// Important: The 'exists()' check above must come before the 'isDirectory()' check to
			//            be safe when multiple parallel instances try to create the directory

			// exists and is not a directory -> is a regular file
			throw new FileAlreadyExistsException(file.getAbsolutePath());
		}
		else {
			File parent = file.getParentFile();
			return (parent == null || mkdirsInternal(parent)) && (file.mkdir() || file.isDirectory());
		}
	}

	@Override
	public FSDataOutputStream create(final Path filePath, final WriteMode overwrite) throws IOException {
		checkNotNull(filePath, "filePath");

		if (exists(filePath) && overwrite == WriteMode.NO_OVERWRITE) {
			throw new FileAlreadyExistsException("File already exists: " + filePath);
		}

		final Path parent = filePath.getParent();
		if (parent != null && !mkdirs(parent)) {
			throw new IOException("Mkdirs failed to create " + parent);
		}

		final File file = pathToFile(filePath);
		return new LocalDataOutputStream(file);
	}

	@Override
	public boolean rename(final Path src, final Path dst) throws IOException {
		final File srcFile = pathToFile(src);
		final File dstFile = pathToFile(dst);

		final File dstParent = dstFile.getParentFile();

		// Files.move fails if the destination directory doesn't exist
		//noinspection ResultOfMethodCallIgnored -- we don't care if the directory existed or was created
		dstParent.mkdirs();

		try {
			Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			return true;
		}
		catch (NoSuchFileException | AccessDeniedException | DirectoryNotEmptyException | SecurityException ex) {
			// catch the errors that are regular "move failed" exceptions and return false
			return false;
		}
	}

	@Override
	public boolean isDistributedFS() {
		return false;
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.FILE_SYSTEM;
	}

	// ------------------------------------------------------------------------

	/**
	 * Converts the given Path to a File for this file system. If the path is empty,
	 * we will return <tt>new File(".")</tt> instead of <tt>new File("")</tt>, since
	 * the latter returns <tt>false</tt> for <tt>isDirectory</tt> judgement (See issue
	 * https://issues.apache.org/jira/browse/FLINK-18612).
	 */
	public File pathToFile(Path path) {
		String localPath = path.getPath();
		checkState(
			localPath != null,
			"Cannot convert a null path to File");

		if (localPath.length() == 0) {
			return new File(".");
		}

		return new File(localPath);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the URI that represents the local file system.
	 * That URI is {@code "file:/"} on Windows platforms and {@code "file:///"} on other
	 * UNIX family platforms.
	 *
	 * @return The URI that represents the local file system.
	 */
	public static URI getLocalFsURI() {
		return LOCAL_URI;
	}

	/**
	 * Gets the shared instance of this file system.
	 *
	 * @return The shared instance of this file system.
	 */
	public static LocalFileSystem getSharedInstance() {
		return INSTANCE;
	}
}
