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
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.local.LocalFileSystem;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract base class of all file systems used by Flink. This class may be extended to implement
 * distributed file systems, or local file systems. The abstraction by this file system is very simple,
 * and the set of available operations quite limited, to support the common denominator of a wide
 * range of file systems. For example, appending to or mutating existing files is not supported.
 * 
 * <p>Flink implements and supports some file system types directly (for example the default
 * machine-local file system). Other file system types are accessed by an implementation that bridges
 * to the suite of file systems supported by Hadoop (such as for example HDFS).
 * 
 * <h2>Scope and Purpose</h2>
 * 
 * The purpose of this abstraction is used to expose a common and well defined interface for
 * access to files. This abstraction is used both by Flink's fault tolerance mechanism (storing
 * state and recovery data) and by reusable built-in connectors (file sources / sinks).
 * 
 * <p>The purpose of this abstraction is <b>not</b> to give user programs an abstraction with
 * extreme flexibility and control across all possible file systems. That mission would be a folly,
 * as the differences in characteristics of even the most common file systems are already quite
 * large. It is expected that user programs that need specialized functionality of certain file systems
 * in their functions, operations, sources, or sinks instantiate the specialized file system adapters
 * directly.
 * 
 * <h2>Data Persistence Contract</h2>
 * 
 * The FileSystem's {@link FSDataOutputStream output streams} are used to persistently store data,
 * both for results of streaming applications and for fault tolerance and recovery. It is therefore
 * crucial that the persistence semantics of these streams are well defined.
 * 
 * <h3>Definition of Persistence Guarantees</h3>
 * 
 * Data written to an output stream is considered persistent, if two requirements are met:
 * 
 * <ol>
 *     <li><b>Visibility Requirement:</b> It must be guaranteed that all other processes, machines,
 *     virtual machines, containers, etc. that are able to access the file see the data consistently
 *     when given the absolute file path. This requirement is similar to the <i>close-to-open</i>
 *     semantics defined by POSIX, but restricted to the file itself (by its absolute path).</li>
 * 
 *     <li><b>Durability Requirement:</b> The file system's specific durability/persistence requirements
 *     must be met. These are specific to the particular file system. For example the
 *     {@link LocalFileSystem} does not provide any durability guarantees for crashes of both
 *     hardware and operating system, while replicated distributed file systems (like HDFS)
 *     typically guarantee durability in the presence of at most <i>n</i> concurrent node failures,
 *     where <i>n</i> is the replication factor.</li>
 * </ol>
 *
 * <p>Updates to the file's parent directory (such that the file shows up when
 * listing the directory contents) are not required to be complete for the data in the file stream
 * to be considered persistent. This relaxation is important for file systems where updates to
 * directory contents are only eventually consistent.
 * 
 * <p>The {@link FSDataOutputStream} has to guarantee data persistence for the written bytes
 * once the call to {@link FSDataOutputStream#close()} returns.
 *
 * <h3>Examples</h3>
 *
 * <ul>
 *     <li>For <b>fault-tolerant distributed file systems</b>, data is considered persistent once 
 *     it has been received and acknowledged by the file system, typically by having been replicated
 *     to a quorum of machines (<i>durability requirement</i>). In addition the absolute file path
 *     must be visible to all other machines that will potentially access the file (<i>visibility
 *     requirement</i>).
 *
 *     <p>Whether data has hit non-volatile storage on the storage nodes depends on the specific
 *     guarantees of the particular file system.
 *
 *     <p>The metadata updates to the file's parent directory are not required to have reached
 *     a consistent state. It is permissible that some machines see the file when listing the parent
 *     directory's contents while others do not, as long as access to the file by its absolute path
 *     is possible on all nodes.</li>
 *
 *     <li>A <b>local file system</b> must support the POSIX <i>close-to-open</i> semantics.
 *     Because the local file system does not have any fault tolerance guarantees, no further
 *     requirements exist.
 * 
 *     <p>The above implies specifically that data may still be in the OS cache when considered
 *     persistent from the local file system's perspective. Crashes that cause the OS cache to loose
 *     data are considered fatal to the local machine and are not covered by the local file system's
 *     guarantees as defined by Flink.
 * 
 *     <p>That means that computed results, checkpoints, and savepoints that are written only to
 *     the local filesystem are not guaranteed to be recoverable from the local machine's failure,
 *     making local file systems unsuitable for production setups.</li>
 * </ul>
 *
 * <h2>Updating File Contents</h2>
 *
 * Many file systems either do not support overwriting contents of existing files at all, or do
 * not support consistent visibility of the updated contents in that case. For that reason,
 * Flink's FileSystem does not support appending to existing files, or seeking within output streams
 * so that previously written data could be overwritten.
 *
 * <h2>Overwriting Files</h2>
 *
 * Overwriting files is in general possible. A file is overwritten by deleting it and creating
 * a new file. However, certain filesystems cannot make that change synchronously visible
 * to all parties that have access to the file.
 * For example <a href="https://aws.amazon.com/documentation/s3/">Amazon S3</a> guarantees only
 * <i>eventual consistency</i> in the visibility of the file replacement: Some machines may see
 * the old file, some machines may see the new file.
 *
 * <p>To avoid these consistency issues, the implementations of failure/recovery mechanisms in
 * Flink strictly avoid writing to the same file path more than once.
 * 
 * <h2>Thread Safety</h2>
 * 
 * Implementations of {@code FileSystem} must be thread-safe: The same instance of FileSystem
 * is frequently shared across multiple threads in Flink and must be able to concurrently
 * create input/output streams and list file metadata.
 * 
 * <p>The {@link FSDataOutputStream} and {@link FSDataOutputStream} implementations are strictly
 * <b>not thread-safe</b>. Instances of the streams should also not be passed between threads
 * in between read or write operations, because there are no guarantees about the visibility of
 * operations across threads (many operations do not create memory fences).
 * 
 * <h2>Streams Safety Net</h2>
 * 
 * When application code obtains a FileSystem (via {@link FileSystem#get(URI)} or via
 * {@link Path#getFileSystem()}), the FileSystem instantiates a safety net for that FileSystem.
 * The safety net ensures that all streams created from the FileSystem are closed when the
 * application task finishes (or is canceled or failed). That way, the task's threads do not
 * leak connections.
 * 
 * <p>Internal runtime code can explicitly obtain a FileSystem that does not use the safety
 * net via {@link FileSystem#getUnguardedFileSystem(URI)}.
 * 
 * @see FSDataInputStream
 * @see FSDataOutputStream
 */
@Public
public abstract class FileSystem {

	/**
	 * The possible write modes. The write mode decides what happens if a file should be created,
	 * but already exists.
	 */
	public enum WriteMode {

		/** Creates the target file only if no file exists at that path already.
		 * Does not overwrite existing files and directories. */
		NO_OVERWRITE,

		/** Creates a new target file regardless of any existing files or directories.
		 * Existing files and directories will be deleted (recursively) automatically before
		 * creating the new file. */
		OVERWRITE
	}

	// ------------------------------------------------------------------------
	//  File System Implementation Classes
	// ------------------------------------------------------------------------

	private static final String HADOOP_WRAPPER_FILESYSTEM_CLASS = "org.apache.flink.runtime.fs.hdfs.HadoopFileSystem";

	private static final String MAPR_FILESYSTEM_CLASS = "org.apache.flink.runtime.fs.maprfs.MapRFileSystem";

	private static final String HADOOP_WRAPPER_SCHEME = "hdwrapper";

	// ------------------------------------------------------------------------

	/** This lock guards the methods {@link #initOutPathLocalFS(Path, WriteMode, boolean)} and
	 * {@link #initOutPathDistFS(Path, WriteMode, boolean)} which are otherwise susceptible to races */
	private static final ReentrantLock OUTPUT_DIRECTORY_INIT_LOCK = new ReentrantLock(true);

	/** Object used to protect calls to specific methods.*/
	private static final ReentrantLock LOCK = new ReentrantLock(true);

	/** Cache for file systems, by scheme + authority. */
	private static final Map<FSKey, FileSystem> CACHE = new HashMap<>();

	/** Mapping of file system schemes to  the corresponding implementations */
	private static final Map<String, String> FSDIRECTORY = new HashMap<>();

	/** The local file system. Needs to be lazily initialized to avoid that some JVMs deadlock
	 * on static subclass initialization. */
	private static LocalFileSystem LOCAL_FS;

	/** The default filesystem scheme to be used, configured during process-wide initialization.
	 * This value defaults to the local file systems scheme {@code 'file:///'} or
	 * {@code 'file:/'}. */
	private static URI defaultScheme;

	// ------------------------------------------------------------------------
	//  Initialization
	// ------------------------------------------------------------------------

	static {
		FSDIRECTORY.put("hdfs", HADOOP_WRAPPER_FILESYSTEM_CLASS);
		FSDIRECTORY.put("maprfs", MAPR_FILESYSTEM_CLASS);
		FSDIRECTORY.put("file", LocalFileSystem.class.getName());
	}

	/**
	 * <p>
	 * Sets the default filesystem scheme based on the user-specified configuration parameter
	 * <code>fs.default-scheme</code>. By default this is set to <code>file:///</code>
	 * (see {@link ConfigConstants#FILESYSTEM_SCHEME} and
	 * {@link ConfigConstants#DEFAULT_FILESYSTEM_SCHEME}),
	 * and the local filesystem is used.
	 * <p>
	 * As an example, if set to <code>hdfs://localhost:9000/</code>, then an HDFS deployment
	 * with the namenode being on the local node and listening to port 9000 is going to be used.
	 * In this case, a file path specified as <code>/user/USERNAME/in.txt</code>
	 * is going to be transformed into <code>hdfs://localhost:9000/user/USERNAME/in.txt</code>. By
	 * default this is set to <code>file:///</code> which points to the local filesystem.
	 * @param config the configuration from where to fetch the parameter.
	 */
	public static void setDefaultScheme(Configuration config) throws IOException {
		LOCK.lock();
		try {
			if (defaultScheme == null) {
				final String stringifiedUri = config.getString(ConfigConstants.FILESYSTEM_SCHEME, null);
				if (stringifiedUri == null) {
					defaultScheme = LocalFileSystem.getLocalFsURI();
				}
				else {
					try {
						defaultScheme = new URI(stringifiedUri);
					} catch (URISyntaxException e) {
						throw new IOException("The URI used to set the default filesystem " +
								"scheme ('" + stringifiedUri + "') is not valid.");
					}
				}
			}
		}
		finally {
			LOCK.unlock();
		}
	}

	// ------------------------------------------------------------------------
	//  Obtaining File System Instances
	// ------------------------------------------------------------------------

	/**
	 * Returns a reference to the {@link FileSystem} instance for accessing the local file system.
	 *
	 * @return a reference to the {@link FileSystem} instance for accessing the local file system.
	 */
	public static FileSystem getLocalFileSystem() {
		LOCK.lock();
		try {
			if (LOCAL_FS == null) {
				LOCAL_FS = new LocalFileSystem();
			}
			return FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(LOCAL_FS);
		} finally {
			LOCK.unlock();
		}
	}

	@Internal
	public static FileSystem getUnguardedFileSystem(URI uri) throws IOException {
		final URI asked = uri;

		LOCK.lock();
		try {

			if (uri.getScheme() == null) {
				try {
					if (defaultScheme == null) {
						defaultScheme = new URI(ConfigConstants.DEFAULT_FILESYSTEM_SCHEME);
					}

					uri = new URI(defaultScheme.getScheme(), null, defaultScheme.getHost(),
							defaultScheme.getPort(), uri.getPath(), null, null);

				} catch (URISyntaxException e) {
					try {
						if (defaultScheme.getScheme().equals("file")) {
							uri = new URI("file", null,
									new Path(new File(uri.getPath()).getAbsolutePath()).toUri().getPath(), null);
						}
					} catch (URISyntaxException ex) {
						// we tried to repair it, but could not. report the scheme error
						throw new IOException("The URI '" + uri.toString() + "' is not valid.");
					}
				}
			}

			if(uri.getScheme() == null) {
				throw new IOException("The URI '" + uri + "' is invalid.\n" +
						"The fs.default-scheme = " + defaultScheme + ", the requested URI = " + asked +
						", and the final URI = " + uri + ".");
			}

			if (uri.getScheme().equals("file") && uri.getAuthority() != null && !uri.getAuthority().isEmpty()) {
				String supposedUri = "file:///" + uri.getAuthority() + uri.getPath();

				throw new IOException("Found local file path with authority '" + uri.getAuthority() + "' in path '"
						+ uri.toString() + "'. Hint: Did you forget a slash? (correct path would be '" + supposedUri + "')");
			}

			final FSKey key = new FSKey(uri.getScheme(), uri.getAuthority());

			// See if there is a file system object in the cache
			if (CACHE.containsKey(key)) {
				return CACHE.get(key);
			}

			// Try to create a new file system
			final FileSystem fs;

			if (!isFlinkSupportedScheme(uri.getScheme())) {
				// no build in support for this file system. Falling back to Hadoop's FileSystem impl.
				Class<?> wrapperClass = getHadoopWrapperClassNameForFileSystem(uri.getScheme());
				if (wrapperClass != null) {
					// hadoop has support for the FileSystem
					FSKey wrappedKey = new FSKey(HADOOP_WRAPPER_SCHEME + "+" + uri.getScheme(), uri.getAuthority());
					if (CACHE.containsKey(wrappedKey)) {
						return CACHE.get(wrappedKey);
					}
					// cache didn't contain the file system. instantiate it:

					// by now we know that the HadoopFileSystem wrapper can wrap the file system.
					fs = instantiateHadoopFileSystemWrapper(wrapperClass);
					fs.initialize(uri);
					CACHE.put(wrappedKey, fs);

				} else {
					// we can not read from this file system.
					throw new IOException("No file system found with scheme " + uri.getScheme()
							+ ", referenced in file URI '" + uri.toString() + "'.");
				}
			} else {
				// we end up here if we have a file system with build-in flink support.
				String fsClass = FSDIRECTORY.get(uri.getScheme());
				if (fsClass.equals(HADOOP_WRAPPER_FILESYSTEM_CLASS)) {
					fs = instantiateHadoopFileSystemWrapper(null);
				} else {
					fs = instantiateFileSystem(fsClass);
				}
				// Initialize new file system object
				fs.initialize(uri);

				// Add new file system object to cache
				CACHE.put(key, fs);
			}

			return fs;
		}
		finally {
			LOCK.unlock();
		}
	}

	/**
	 * Returns a reference to the {@link FileSystem} instance for accessing the
	 * file system identified by the given {@link URI}.
	 *
	 * @param uri
	 *        the {@link URI} identifying the file system
	 * @return a reference to the {@link FileSystem} instance for accessing the file system identified by the given
	 *         {@link URI}.
	 * @throws IOException
	 *         thrown if a reference to the file system instance could not be obtained
	 */
	public static FileSystem get(URI uri) throws IOException {
		return FileSystemSafetyNet.wrapWithSafetyNetWhenActivated(getUnguardedFileSystem(uri));
	}

	//Class must implement Hadoop FileSystem interface. The class is not avaiable in 'flink-core'.
	private static FileSystem instantiateHadoopFileSystemWrapper(Class<?> wrappedFileSystem) throws IOException {
		try {
			Class<? extends FileSystem> fsClass = getFileSystemByName(HADOOP_WRAPPER_FILESYSTEM_CLASS);
			Constructor<? extends FileSystem> fsClassCtor = fsClass.getConstructor(Class.class);
			return fsClassCtor.newInstance(wrappedFileSystem);
		} catch (Throwable e) {
			throw new IOException("Error loading Hadoop FS wrapper", e);
		}
	}

	private static FileSystem instantiateFileSystem(String className) throws IOException {
		try {
			Class<? extends FileSystem> fsClass = getFileSystemByName(className);
			return fsClass.newInstance();
		}
		catch (ClassNotFoundException e) {
			throw new IOException("Could not load file system class '" + className + '\'', e);
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IOException("Could not instantiate file system class: " + e.getMessage(), e);
		}
	}

	private static HadoopFileSystemWrapper hadoopWrapper;

	private static Class<?> getHadoopWrapperClassNameForFileSystem(String scheme) {
		if (hadoopWrapper == null) {
			try {
				hadoopWrapper = (HadoopFileSystemWrapper) instantiateHadoopFileSystemWrapper(null);
			} catch (IOException e) {
				throw new RuntimeException("Error creating new Hadoop wrapper", e);
			}
		}
		return hadoopWrapper.getHadoopWrapperClassNameForFileSystem(scheme);
	}


	// ------------------------------------------------------------------------
	//  File System Methods
	// ------------------------------------------------------------------------

	/**
	 * Returns the path of the file system's current working directory.
	 *
	 * @return the path of the file system's current working directory
	 */
	public abstract Path getWorkingDirectory();

	/**
	 * Returns the path of the user's home directory in this file system.
	 *
	 * @return the path of the user's home directory in this file system.
	 */
	public abstract Path getHomeDirectory();

	/**
	 * Returns a URI whose scheme and authority identify this file system.
	 *
	 * @return a URI whose scheme and authority identify this file system
	 */
	public abstract URI getUri();

	/**
	 * Called after a new FileSystem instance is constructed.
	 *
	 * @param name
	 *        a {@link URI} whose authority section names the host, port, etc. for this file system
	 */
	public abstract void initialize(URI name) throws IOException;

	/**
	 * Return a file status object that represents the path.
	 *
	 * @param f
	 *        The path we want information from
	 * @return a FileStatus object
	 * @throws FileNotFoundException
	 *         when the path does not exist;
	 *         IOException see specific implementation
	 */
	public abstract FileStatus getFileStatus(Path f) throws IOException;

	/**
	 * Return an array containing hostnames, offset and size of
	 * portions of the given file. For a nonexistent
	 * file or regions, null will be returned.
	 * This call is most helpful with DFS, where it returns
	 * hostnames of machines that contain the given file.
	 * The FileSystem will simply return an elt containing 'localhost'.
	 */
	public abstract BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException;

	/**
	 * Opens an FSDataInputStream at the indicated Path.
	 *
	 * @param f
	 *        the file name to open
	 * @param bufferSize
	 *        the size of the buffer to be used.
	 */
	public abstract FSDataInputStream open(Path f, int bufferSize) throws IOException;

	/**
	 * Opens an FSDataInputStream at the indicated Path.
	 *
	 * @param f
	 *        the file to open
	 */
	public abstract FSDataInputStream open(Path f) throws IOException;

	/**
	 * Return the number of bytes that large input files should be optimally be split into to minimize I/O time.
	 *
	 * @return the number of bytes that large input files should be optimally be split into to minimize I/O time
	 * 
	 * @deprecated This value is no longer used and is meaningless.
	 */
	@Deprecated
	public long getDefaultBlockSize() {
		return 32 * 1024 * 1024; // 32 MB;
	}

	/**
	 * List the statuses of the files/directories in the given path if the path is
	 * a directory.
	 *
	 * @param f
	 *        given path
	 * @return the statuses of the files/directories in the given patch
	 * @throws IOException
	 */
	public abstract FileStatus[] listStatus(Path f) throws IOException;

	/**
	 * Check if exists.
	 *
	 * @param f
	 *        source file
	 */
	public boolean exists(final Path f) throws IOException {
		try {
			return (getFileStatus(f) != null);
		} catch (FileNotFoundException e) {
			return false;
		}
	}

	/**
	 * Delete a file.
	 *
	 * @param f
	 *        the path to delete
	 * @param recursive
	 *        if path is a directory and set to <code>true</code>, the directory is deleted else throws an exception. In
	 *        case of a file the recursive can be set to either <code>true</code> or <code>false</code>
	 * @return <code>true</code> if delete is successful, <code>false</code> otherwise
	 * @throws IOException
	 */
	public abstract boolean delete(Path f, boolean recursive) throws IOException;

	/**
	 * Make the given file and all non-existent parents into directories. Has the semantics of Unix 'mkdir -p'.
	 * Existence of the directory hierarchy is not an error.
	 *
	 * @param f
	 *        the directory/directories to be created
	 * @return <code>true</code> if at least one new directory has been created, <code>false</code> otherwise
	 * @throws IOException
	 *         thrown if an I/O error occurs while creating the directory
	 */
	public abstract boolean mkdirs(Path f) throws IOException;

	/**
	 * Opens an FSDataOutputStream at the indicated Path.
	 * 
	 * <p>This method is deprecated, because most of its parameters are ignored by most file systems.
	 * To control for example the replication factor and block size in the Hadoop Distributed File system,
	 * make sure that the respective Hadoop configuration file is either linked from the Flink configuration,
	 * or in the classpath of either Flink or the user code.
	 *
	 * @param f
	 *        the file name to open
	 * @param overwrite
	 *        if a file with this name already exists, then if true,
	 *        the file will be overwritten, and if false an error will be thrown.
	 * @param bufferSize
	 *        the size of the buffer to be used.
	 * @param replication
	 *        required block replication for the file.
	 * @param blockSize
	 *        the size of the file blocks
	 * 
	 * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because
	 *                     a file already exists at that path and the write mode indicates to not
	 *                     overwrite the file.
	 * 
	 * @deprecated Deprecated because not well supported across types of file systems.
	 *             Control the behavior of specific file systems via configurations instead. 
	 */
	@Deprecated
	public FSDataOutputStream create(
			Path f,
			boolean overwrite,
			int bufferSize,
			short replication,
			long blockSize) throws IOException {

		return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
	}

	/**
	 * Opens an FSDataOutputStream at the indicated Path.
	 *
	 * @param f
	 *        the file name to open
	 * @param overwrite
	 *        if a file with this name already exists, then if true,
	 *        the file will be overwritten, and if false an error will be thrown.
	 * 
	 * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because
	 *                     a file already exists at that path and the write mode indicates to not
	 *                     overwrite the file.
	 * 
	 * @deprecated Use {@link #create(Path, WriteMode)} instead.
	 */
	@Deprecated
	public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
		return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
	}

	/**
	 * Opens an FSDataOutputStream to a new file at the given path.
	 * 
	 * <p>If the file already exists, the behavior depends on the given {@code WriteMode}.
	 * If the mode is set to {@link WriteMode#NO_OVERWRITE}, then this method fails with an
	 * exception.
	 *
	 * @param f The file path to write to
	 * @param overwriteMode The action to take if a file or directory already exists at the given path.
	 * @return The stream to the new file at the target path.
	 * 
	 * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because
	 *                     a file already exists at that path and the write mode indicates to not
	 *                     overwrite the file.
	 */
	public abstract FSDataOutputStream create(Path f, WriteMode overwriteMode) throws IOException;

	/**
	 * Renames the file/directory src to dst.
	 *
	 * @param src
	 *        the file/directory to rename
	 * @param dst
	 *        the new name of the file/directory
	 * @return <code>true</code> if the renaming was successful, <code>false</code> otherwise
	 * @throws IOException
	 */
	public abstract boolean rename(Path src, Path dst) throws IOException;

	/**
	 * Returns true if this is a distributed file system. A distributed file system here means
	 * that the file system is shared among all Flink processes that participate in a cluster or
	 * job and that all these processes can see the same files.
	 *
	 * @return True, if this is a distributed file system, false otherwise.
	 */
	public abstract boolean isDistributedFS();

	// ------------------------------------------------------------------------
	//  output directory initialization
	// ------------------------------------------------------------------------

	/**
	 * Initializes output directories on local file systems according to the given write mode.
	 *
	 * <ul>
	 *   <li>WriteMode.NO_OVERWRITE &amp; parallel output:
	 *     <ul>
	 *       <li>A directory is created if the output path does not exist.</li>
	 *       <li>An existing directory is reused, files contained in the directory are NOT deleted.</li>
	 *       <li>An existing file raises an exception.</li>
	 *     </ul>
	 *   </li>
	 *
	 *   <li>WriteMode.NO_OVERWRITE &amp; NONE parallel output:
	 *     <ul>
	 *       <li>An existing file or directory raises an exception.</li>
	 *     </ul>
	 *   </li>
	 *
	 *   <li>WriteMode.OVERWRITE &amp; parallel output:
	 *     <ul>
	 *       <li>A directory is created if the output path does not exist.</li>
	 *       <li>An existing directory is reused, files contained in the directory are NOT deleted.</li>
	 *       <li>An existing file is deleted and replaced by a new directory.</li>
	 *     </ul>
	 *   </li>
	 *
	 *   <li>WriteMode.OVERWRITE &amp; NONE parallel output:
	 *     <ul>
	 *       <li>An existing file or directory (and all its content) is deleted</li>
	 *     </ul>
	 *   </li>
	 * </ul>
	 * 
	 * <p>Files contained in an existing directory are not deleted, because multiple instances of a
	 * DataSinkTask might call this function at the same time and hence might perform concurrent
	 * delete operations on the file system (possibly deleting output files of concurrently running tasks).
	 * Since concurrent DataSinkTasks are not aware of each other, coordination of delete and create
	 * operations would be difficult.
	 *
	 * @param outPath Output path that should be prepared.
	 * @param writeMode Write mode to consider.
	 * @param createDirectory True, to initialize a directory at the given path, false to prepare space for a file.
	 *    
	 * @return True, if the path was successfully prepared, false otherwise.
	 * @throws IOException Thrown, if any of the file system access operations failed.
	 */
	public boolean initOutPathLocalFS(Path outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		if (isDistributedFS()) {
			return false;
		}

		// NOTE: We actually need to lock here (process wide). Otherwise, multiple threads that
		// concurrently work in this method (multiple output formats writing locally) might end
		// up deleting each other's directories and leave non-retrievable files, without necessarily
		// causing an exception. That results in very subtle issues, like output files looking as if
		// they are not getting created.

		// we acquire the lock interruptibly here, to make sure that concurrent threads waiting
		// here can cancel faster
		try {
			OUTPUT_DIRECTORY_INIT_LOCK.lockInterruptibly();
		}
		catch (InterruptedException e) {
			// restore the interruption state
			Thread.currentThread().interrupt();

			// leave the method - we don't have the lock anyways 
			throw new IOException("The thread was interrupted while trying to initialize the output directory");
		}

		try {
			FileStatus status;
			try {
				status = getFileStatus(outPath);
			}
			catch (FileNotFoundException e) {
				// okay, the file is not there
				status = null;
			}

			// check if path exists
			if (status != null) {
				// path exists, check write mode
				switch (writeMode) {

				case NO_OVERWRITE:
					if (status.isDir() && createDirectory) {
						return true;
					} else {
						// file may not be overwritten
						throw new IOException("File or directory already exists. Existing files and directories " +
								"are not overwritten in " + WriteMode.NO_OVERWRITE.name() + " mode. Use " + 
								WriteMode.OVERWRITE.name() + " mode to overwrite existing files and directories.");
					}

				case OVERWRITE:
					if (status.isDir()) {
						if (createDirectory) {
							// directory exists and does not need to be created
							return true;
						} else {
							// we will write in a single file, delete directory
							try {
								delete(outPath, true);
							}
							catch (IOException e) {
								throw new IOException("Could not remove existing directory '" + outPath + 
										"' to allow overwrite by result file", e);
							}
						}
					}
					else {
						// delete file
						try {
							delete(outPath, false);
						}
						catch (IOException e) {
							throw new IOException("Could not remove existing file '" + outPath +
									"' to allow overwrite by result file/directory", e);
						}
					}
					break;

				default:
					throw new IllegalArgumentException("Invalid write mode: " + writeMode);
				}
			}

			if (createDirectory) {
				// Output directory needs to be created
				if (!exists(outPath)) {
					mkdirs(outPath);
				}

				// double check that the output directory exists
				try {
					return getFileStatus(outPath).isDir();
				}
				catch (FileNotFoundException e) {
					return false;
				}
			}
			else {
				// check that the output path does not exist and an output file
				// can be created by the output format.
				return !exists(outPath);
			}
		}
		finally {
			OUTPUT_DIRECTORY_INIT_LOCK.unlock();
		}
	}

	/**
	 * Initializes output directories on distributed file systems according to the given write mode.
	 *
	 * WriteMode.NO_OVERWRITE &amp; parallel output:
	 *  - A directory is created if the output path does not exist.
	 *  - An existing file or directory raises an exception.
	 *
	 * WriteMode.NO_OVERWRITE &amp; NONE parallel output:
	 *  - An existing file or directory raises an exception.
	 *
	 * WriteMode.OVERWRITE &amp; parallel output:
	 *  - A directory is created if the output path does not exist.
	 *  - An existing directory and its content is deleted and a new directory is created.
	 *  - An existing file is deleted and replaced by a new directory.
	 *
	 *  WriteMode.OVERWRITE &amp; NONE parallel output:
	 *  - An existing file or directory is deleted and replaced by a new directory.
	 *
	 * @param outPath Output path that should be prepared.
	 * @param writeMode Write mode to consider.
	 * @param createDirectory True, to initialize a directory at the given path, false otherwise.
	 *    
	 * @return True, if the path was successfully prepared, false otherwise.
	 * 
	 * @throws IOException Thrown, if any of the file system access operations failed.
	 */
	public boolean initOutPathDistFS(Path outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		if (!isDistributedFS()) {
			return false;
		}

		// NOTE: We actually need to lock here (process wide). Otherwise, multiple threads that
		// concurrently work in this method (multiple output formats writing locally) might end
		// up deleting each other's directories and leave non-retrievable files, without necessarily
		// causing an exception. That results in very subtle issues, like output files looking as if
		// they are not getting created.

		// we acquire the lock interruptibly here, to make sure that concurrent threads waiting
		// here can cancel faster
		try {
			OUTPUT_DIRECTORY_INIT_LOCK.lockInterruptibly();
		}
		catch (InterruptedException e) {
			// restore the interruption state
			Thread.currentThread().interrupt();

			// leave the method - we don't have the lock anyways 
			throw new IOException("The thread was interrupted while trying to initialize the output directory");
		}

		try {
			// check if path exists
			if (exists(outPath)) {
				// path exists, check write mode
				switch(writeMode) {
	
				case NO_OVERWRITE:
					// file or directory may not be overwritten
					throw new IOException("File or directory already exists. Existing files and directories are not overwritten in " +
							WriteMode.NO_OVERWRITE.name() + " mode. Use " + WriteMode.OVERWRITE.name() +
								" mode to overwrite existing files and directories.");
	
				case OVERWRITE:
					// output path exists. We delete it and all contained files in case of a directory.
					try {
						delete(outPath, true);
					} catch (IOException e) {
						// Some other thread might already have deleted the path.
						// If - for some other reason - the path could not be deleted,
						// this will be handled later.
					}
					break;
	
				default:
					throw new IllegalArgumentException("Invalid write mode: "+writeMode);
				}
			}
	
			if (createDirectory) {
				// Output directory needs to be created
				try {
					if (!exists(outPath)) {
						mkdirs(outPath);
					}
				} catch (IOException ioe) {
					// Some other thread might already have created the directory.
					// If - for some other reason - the directory could not be created  
					// and the path does not exist, this will be handled later.
				}
	
				// double check that the output directory exists
				return exists(outPath) && getFileStatus(outPath).isDir();
			}
			else {
				// single file case: check that the output path does not exist and
				// an output file can be created by the output format.
				return !exists(outPath);
			}
		}
		finally {
			OUTPUT_DIRECTORY_INIT_LOCK.unlock();
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static Class<? extends FileSystem> getFileSystemByName(String className) throws ClassNotFoundException {
		return Class.forName(className, true, FileSystem.class.getClassLoader()).asSubclass(FileSystem.class);
	}

	/**
	 * An identifier of a file system, via its scheme and its authority.
	 */
	private static final class FSKey {

		/** The scheme of the file system. */
		private final String scheme;

		/** The authority of the file system. */
		@Nullable
		private final String authority;

		/**
		 * Creates a file system key from a given scheme and an authority.
		 *
		 * @param scheme     The scheme of the file system
		 * @param authority  The authority of the file system
		 */
		public FSKey(String scheme, @Nullable String authority) {
			this.scheme = checkNotNull(scheme, "scheme");
			this.authority = authority;
		}

		@Override
		public boolean equals(final Object obj) {
			if (obj == this) {
				return true;
			}
			else if (obj != null && obj.getClass() == FSKey.class) {
				final FSKey that = (FSKey) obj;
				return this.scheme.equals(that.scheme) &&
						(this.authority == null ? that.authority == null :
								(that.authority != null && this.authority.equals(that.authority)));
			}
			else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 * scheme.hashCode() + 
					(authority == null ? 17 : authority.hashCode());
		}

		@Override
		public String toString() {
			return scheme + "://" + authority;
		}
	}
}
