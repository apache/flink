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


/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OperatingSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * and teh set of allowed operations quite limited, to support the common denominator of a wide
 * range of file systems. For example, appending to or mutating existing files is not supported.
 * 
 * <p>Flink implements and supports some file system types directly (for example the default
 * machine-local file system). Other file system types are accessed by an implementation that bridges
 * to the suite of file systems supported by Hadoop (such as for example HDFS).
 */
@Public
public abstract class FileSystem {

	/**
	 * The possible write modes. The write mode decides what happens if a file should be created,
	 * but already exists.
	 */
	public enum WriteMode {

		/** Creates the target file if it does not exist. Does not overwrite existing files and directories. */
		NO_OVERWRITE,

		/** Creates a new target file regardless of any existing files or directories. Existing files and
		 * directories will be removed/overwritten. */
		OVERWRITE
	}

	// ------------------------------------------------------------------------

	private static final InheritableThreadLocal<SafetyNetCloseableRegistry> REGISTRIES = new InheritableThreadLocal<>();

	private static final String HADOOP_WRAPPER_FILESYSTEM_CLASS = "org.apache.flink.runtime.fs.hdfs.HadoopFileSystem";

	private static final String MAPR_FILESYSTEM_CLASS = "org.apache.flink.runtime.fs.maprfs.MapRFileSystem";

	private static final String HADOOP_WRAPPER_SCHEME = "hdwrapper";

	private static final Logger LOG = LoggerFactory.getLogger(FileSystem.class);

	/** This lock guards the methods {@link #initOutPathLocalFS(Path, WriteMode, boolean)} and
	 * {@link #initOutPathDistFS(Path, WriteMode, boolean)} which are otherwise susceptible to races */
	private static final ReentrantLock OUTPUT_DIRECTORY_INIT_LOCK = new ReentrantLock(true);

	// ------------------------------------------------------------------------

	/**
	 * Create a SafetyNetCloseableRegistry for a Task. This method should be called at the beginning of the task's
	 * main thread.
	 */
	public static void createFileSystemCloseableRegistryForTask() {
		SafetyNetCloseableRegistry oldRegistry = REGISTRIES.get();
		if (null != oldRegistry) {
			IOUtils.closeQuietly(oldRegistry);
			LOG.warn("Found existing SafetyNetCloseableRegistry. Closed and replaced it.");
		}
		SafetyNetCloseableRegistry newRegistry = new SafetyNetCloseableRegistry();
		REGISTRIES.set(newRegistry);
	}

	/**
	 * Create a SafetyNetCloseableRegistry for a Task. This method should be called at the end of the task's
	 * main thread or when the task should be canceled.
	 */
	public static void disposeFileSystemCloseableRegistryForTask() {
		SafetyNetCloseableRegistry registry = REGISTRIES.get();
		if (null != registry) {
			LOG.info("Ensuring all FileSystem streams are closed for {}", Thread.currentThread().getName());
			REGISTRIES.remove();
			IOUtils.closeQuietly(registry);
		}
	}

	private static FileSystem wrapWithSafetyNetWhenInTask(FileSystem fs) {
		SafetyNetCloseableRegistry reg = REGISTRIES.get();
		return reg != null ? new SafetyNetWrapperFileSystem(fs, reg) : fs;
	}

	/** Object used to protect calls to specific methods.*/
	private static final Object SYNCHRONIZATION_OBJECT = new Object();

	/**
	 * Data structure mapping file system keys (scheme + authority) to cached file system objects.
	 */
	private static final Map<FSKey, FileSystem> CACHE = new HashMap<FSKey, FileSystem>();

	/**
	 * Data structure mapping file system schemes to the corresponding implementations
	 */
	private static final Map<String, String> FSDIRECTORY = new HashMap<String, String>();

	static {
		FSDIRECTORY.put("hdfs", HADOOP_WRAPPER_FILESYSTEM_CLASS);
		FSDIRECTORY.put("maprfs", MAPR_FILESYSTEM_CLASS);
		FSDIRECTORY.put("file", LocalFileSystem.class.getName());
	}

	/**
	 * Returns a reference to the {@link FileSystem} instance for accessing the
	 * local file system.
	 *
	 * @return a reference to the {@link FileSystem} instance for accessing the
	 *         local file system.
	 */
	public static FileSystem getLocalFileSystem() {
		// this should really never fail.
		try {
			URI localUri = OperatingSystem.isWindows() ? new URI("file:/") : new URI("file:///");
			return get(localUri);
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot create URI for local file system");
		}
	}

	/**
	 * The default filesystem scheme to be used. This can be specified by the parameter
	 * <code>fs.default-scheme</code> in <code>flink-conf.yaml</code>. By default this is
	 * set to <code>file:///</code> (see {@link ConfigConstants#FILESYSTEM_SCHEME}
	 * and {@link ConfigConstants#DEFAULT_FILESYSTEM_SCHEME}), and uses the local filesystem.
	 */
	private static URI defaultScheme;

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
		synchronized (SYNCHRONIZATION_OBJECT) {
			if (defaultScheme == null) {
				String stringifiedUri = config.getString(ConfigConstants.FILESYSTEM_SCHEME,
					ConfigConstants.DEFAULT_FILESYSTEM_SCHEME);
				try {
					defaultScheme = new URI(stringifiedUri);
				} catch (URISyntaxException e) {
					throw new IOException("The URI used to set the default filesystem " +
						"scheme ('" + stringifiedUri + "') is not valid.");
				}
			}
		}
	}

	public static FileSystem getUnguardedFileSystem(URI uri) throws IOException {
		FileSystem fs;

		URI asked = uri;
		synchronized (SYNCHRONIZATION_OBJECT) {

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
		}

		return fs;
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
		return wrapWithSafetyNetWhenInTask(getUnguardedFileSystem(uri));
	}

	/**
	 * Returns a boolean indicating whether a scheme has built-in Flink support.
	 *
	 * @param scheme
	 *        a file system scheme
	 * @return a boolean indicating whether the provided scheme has built-in Flink support
	 */
	public static boolean isFlinkSupportedScheme(String scheme) {
		return FSDIRECTORY.containsKey(scheme);
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
	 */
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
	 * @throws IOException
	 */
	public abstract FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
			long blockSize) throws IOException;

	/**
	 * Opens an FSDataOutputStream at the indicated Path.
	 *
	 * @param f
	 *        the file name to open
	 * @param overwrite
	 *        if a file with this name already exists, then if true,
	 *        the file will be overwritten, and if false an error will be thrown.
	 * @throws IOException
	 */
	public abstract FSDataOutputStream create(Path f, boolean overwrite) throws IOException;

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
	 * Returns true if this is a distributed file system, false otherwise.
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
	 * This class needs to stay public, because it is detected as part of the public API.
	 */
	public static class FSKey {

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
