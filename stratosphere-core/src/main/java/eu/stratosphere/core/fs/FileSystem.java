/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.core.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.util.ClassUtils;
import eu.stratosphere.util.OperatingSystem;
import eu.stratosphere.util.StringUtils;

/**
 * An abstract base class for a fairly generic file system. It
 * may be implemented as a distributed file system, or as a local
 * one that reflects the locally-connected disk.
 */
public abstract class FileSystem {

	private static final String LOCAL_FILESYSTEM_CLASS = "eu.stratosphere.core.fs.local.LocalFileSystem";
	
	private static final String DISTRIBUTED_FILESYSTEM_CLASS = "eu.stratosphere.runtime.fs.hdfs.DistributedFileSystem";

	private static final String S3_FILESYSTEM_CLASS = "eu.stratosphere.runtime.fs.s3.S3FileSystem";

	
	/** Object used to protect calls to specific methods.*/
	private static final Object SYNCHRONIZATION_OBJECT = new Object();

	/**
	 * Enumeration for write modes. 
	 *
	 */
	public static enum WriteMode {
		
		/** Creates write path if it does not exist. Does not overwrite existing files and directories. */
		NO_OVERWRITE,
		
		/** creates write path if it does not exist. Overwrites existing files and directories. */
		OVERWRITE 
	}
	
	/**
	 * An auxiliary class to identify a file system by its scheme and its authority.
	 */
	public static class FSKey {

		/**
		 * The scheme of the file system.
		 */
		private String scheme;

		/**
		 * The authority of the file system.
		 */
		private String authority;

		/**
		 * Creates a file system key from a given scheme and an
		 * authority.
		 * 
		 * @param scheme
		 *        the scheme of the file system
		 * @param authority
		 *        the authority of the file system
		 */
		public FSKey(final String scheme, final String authority) {
			this.scheme = scheme;
			this.authority = authority;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(final Object obj) {

			if (obj instanceof FSKey) {
				final FSKey key = (FSKey) obj;

				if (!this.scheme.equals(key.scheme)) {
					return false;
				}

				if ((this.authority == null) || (key.authority == null)) {

					if (this.authority == null && key.authority == null) {
						return true;
					}

					return false;
				}

				if (!this.authority.equals(key.authority)) {
					return false;
				}

				return true;
			}

			return false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {

			if (this.scheme != null) {
				return this.scheme.hashCode();
			}

			if (this.authority != null) {
				return this.authority.hashCode();
			}

			return super.hashCode();
		}
	}

	/**
	 * Data structure mapping file system keys (scheme + authority) to cached file system objects.
	 */
	private static final Map<FSKey, FileSystem> CACHE = new HashMap<FSKey, FileSystem>();

	/**
	 * Data structure mapping file system schemes to the corresponding implementations
	 */
	private static final Map<String, String> FSDIRECTORY = new HashMap<String, String>();

	static {
		FSDIRECTORY.put("hdfs", DISTRIBUTED_FILESYSTEM_CLASS);
		FSDIRECTORY.put("file", LOCAL_FILESYSTEM_CLASS);
		FSDIRECTORY.put("s3", S3_FILESYSTEM_CLASS);
	}

	/**
	 * Returns a reference to the {@link FileSystem} instance for accessing the
	 * local file system.
	 * 
	 * @return a reference to the {@link FileSystem} instance for accessing the
	 *         local file system.
	 * @throws IOException
	 *         thrown if a reference to the file system instance could not be obtained
	 */
	public static FileSystem getLocalFileSystem() throws IOException {

		URI localUri;

		try {
			localUri = OperatingSystem.isWindows() ?  new URI("file:/") : new URI("file:///");
		} catch (URISyntaxException e) {
			throw new IOException("Cannot create URI for local file system");
		}

		return get(localUri);
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

		FileSystem fs = null;

		synchronized (SYNCHRONIZATION_OBJECT) {

			if (uri.getScheme() == null) {
				try {
					uri = new URI("file", null, uri.getPath(), null);
				}
				catch (URISyntaxException e) {
					// we tried to repair it, but could not. report the scheme error
					throw new IOException("FileSystem: Scheme is null. file:// or hdfs:// are example schemes. "
							+ "Failed for " + uri.toString() + ".");
				}
			}

			final FSKey key = new FSKey(uri.getScheme(), uri.getAuthority());

			// See if there is a file system object in the cache
			if (CACHE.containsKey(key)) {
				return CACHE.get(key);
			}

			// Try to create a new file system
			if (!FSDIRECTORY.containsKey(uri.getScheme())) {
				throw new IOException("No file system found with scheme " + uri.getScheme()
				+ ". Failed for " + uri.toString() + ".");
			}

			Class<? extends FileSystem> fsClass = null;
			try {
				fsClass = ClassUtils.getFileSystemByName(FSDIRECTORY.get(uri.getScheme()));
			} catch (ClassNotFoundException e1) {
				throw new IOException(StringUtils.stringifyException(e1));
			}

			try {
				fs = fsClass.newInstance();
			}
			catch (InstantiationException e) {
				throw new IOException("Could not instantiate file system class: " + e.getMessage(), e);
			}
			catch (IllegalAccessException e) {
				throw new IOException("Could not instantiate file system class: " + e.getMessage(), e);
			}

			// Initialize new file system object
			fs.initialize(uri);

			// Add new file system object to cache
			CACHE.put(key, fs);
		}

		return fs;
	}

	/**
	 * Returns the path of the file system's current working directory.
	 * 
	 * @return the path of the file system's current working directory
	 */
	public abstract Path getWorkingDirectory();

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
	 * Initializes output directories on local file systems according to the given write mode.
	 * 
	 * WriteMode.CREATE & parallel output:
	 *  - A directory is created if the output path does not exist.
	 *  - An existing directory is reused, files contained in the directory are NOT deleted.
	 *  - An existing file raises an exception.
	 *    
	 * WriteMode.CREATE & NONE parallel output:
	 *  - An existing file or directory raises an exception.
	 *  
	 * WriteMode.OVERWRITE & parallel output:
	 *  - A directory is created if the output path does not exist.
	 *  - An existing directory is reused, files contained in the directory are NOT deleted.
	 *  - An existing file is deleted and replaced by a new directory.
	 *  
	 * WriteMode.OVERWRITE & NONE parallel output:
	 *  - An existing file or directory (and all its content) is deleted
	 * 
	 * Files contained in an existing directory are not deleted, because multiple instances of a 
	 * DataSinkTask might call this function at the same time and hence might perform concurrent 
	 * delete operations on the file system (possibly deleting output files of concurrently running tasks). 
	 * Since concurrent DataSinkTasks are not aware of each other, coordination of delete and create
	 * operations would be difficult.
	 * 
	 * @param outPath Output path that should be prepared.
	 * @param writeMode Write mode to consider. 
	 * @param createDirectory True, to initialize a directory at the given path, false otherwise.
	 * @return True, if the path was successfully prepared, false otherwise.
	 * @throws IOException
	 */
	public boolean initOutPathLocalFS(Path outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		if(this.isDistributedFS()) {
			return false;
		}
		
		// check if path exists
		if(this.exists(outPath)) {
			// path exists, check write mode
			switch(writeMode) {
			case NO_OVERWRITE:
				if(this.getFileStatus(outPath).isDir()) {
					return true;
				} else {
					// file may not be overwritten
					throw new IOException("File or directory already exists. Existing files and directories are not overwritten in " + 
							WriteMode.NO_OVERWRITE.name() + " mode. Use " + WriteMode.OVERWRITE.name() + 
							" mode to overwrite existing files and directories.");
				}
			case OVERWRITE:
				if(this.getFileStatus(outPath).isDir()) {
					if(createDirectory) {
						// directory exists and does not need to be created
						return true;
					} else {
						// we will write in a single file, delete directory (there is also no other thread trying to delete the directory).
						try {
							this.delete(outPath, true);
						} catch(IOException ioe) {
							throw new IOException("Could not prepare output path. ",ioe);
						}
					}
				} else {
					// delete file
					try {
						this.delete(outPath, false);
					} catch(IOException ioe) {
						// Some other thread might already have deleted the file.
						// If - for some other reason - the file could not be deleted,  
						// the error will be handled later.
					}
				}
				break;
			default:
				throw new IllegalArgumentException("Invalid write mode: "+writeMode);
			}
		}
		
		if(createDirectory) {
			// Output directory needs to be created
			try {
				if(!this.exists(outPath)) {
					this.mkdirs(outPath);
				}
			} catch(IOException ioe) {
				// Some other thread might already have created the directory.
				// If - for some other reason - the directory could not be created  
				// and the path does not exist, this will be handled later.
			}
	
			// double check that the output directory exists
			return this.exists(outPath) && this.getFileStatus(outPath).isDir();
		} else {
			
			// check that the output path does not exist and an output file can be created by the output format.
			return !this.exists(outPath);
		}
	}
	
	/**
	 * Initializes output directories on distributed file systems according to the given write mode.
	 * 
	 * WriteMode.CREATE & parallel output:
	 *  - A directory is created if the output path does not exist.
	 *  - An existing file or directory raises an exception.
	 * 
	 * WriteMode.CREATE & NONE parallel output:
	 *  - An existing file or directory raises an exception. 
	 *    
	 * WriteMode.OVERWRITE & parallel output:
	 *  - A directory is created if the output path does not exist.
	 *  - An existing directory and its content is deleted and a new directory is created.
	 *  - An existing file is deleted and replaced by a new directory.
	 *  
	 *  WriteMode.OVERWRITE & NONE parallel output:
	 *  - An existing file or directory is deleted and replaced by a new directory.
	 * 
	 * @param outPath Output path that should be prepared.
	 * @param writeMode Write mode to consider. 
	 * @param createDirectory True, to initialize a directory at the given path, false otherwise.
	 * @return True, if the path was successfully prepared, false otherwise.
	 * @throws IOException
	 */
	public boolean initOutPathDistFS(Path outPath, WriteMode writeMode, boolean createDirectory) throws IOException {
		if(!this.isDistributedFS()) {
			return false;
		}
		
		// check if path exists
		if(this.exists(outPath)) {
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
					this.delete(outPath, true);
				} catch(IOException ioe) {
					// Some other thread might already have deleted the path.
					// If - for some other reason - the path could not be deleted,  
					// this will be handled later.
				}
				break;
			default:
				throw new IllegalArgumentException("Invalid write mode: "+writeMode);
			}
		}
		
		if(createDirectory) {
			// Output directory needs to be created
			try {
				if(!this.exists(outPath)) {
					this.mkdirs(outPath);
				}
			} catch(IOException ioe) {
				// Some other thread might already have created the directory.
				// If - for some other reason - the directory could not be created  
				// and the path does not exist, this will be handled later.
			}
			
			// double check that the output directory exists
			return this.exists(outPath) && this.getFileStatus(outPath).isDir();
		} else {
			
			// check that the output path does not exist and an output file can be created by the output format.
			return !this.exists(outPath);
		}
			
	}
	
	/**
	 * Returns true if this is a distributed file system, false otherwise.
	 * 
	 * @return True if this is a distributed file system, false otherwise.
	 */
	public abstract boolean isDistributedFS();
	
	/**
	 * Returns the number of blocks this file/directory consists of
	 * assuming the file system's standard block size.
	 * 
	 * @param file
	 *        the file
	 * @return the number of block's the file/directory consists of
	 * @throws IOException
	 */
	public int getNumberOfBlocks(final FileStatus file) throws IOException {

		int numberOfBlocks = 0;

		if (file == null) {
			return 0;
		}

		// For a file, this is easy
		if (!file.isDir()) {
			return getNumberOfBlocks(file.getLen(), file.getBlockSize());
		}

		// file is a directory
		final FileStatus[] files = this.listStatus(file.getPath());
		for (int i = 0; i < files.length; i++) {

			if (!files[i].isDir()) {
				numberOfBlocks += getNumberOfBlocks(files[i].getLen(), files[i].getBlockSize());
			}
		}

		return numberOfBlocks;
	}

	private int getNumberOfBlocks(final long length, final long blocksize) {

		if (blocksize != 0) {
			int numberOfBlocks;
			numberOfBlocks = (int) (length / blocksize);

			if ((length % blocksize) != 0) {
				numberOfBlocks++;
			}

			return numberOfBlocks;
		} else {
			return 1;
		}
	}
}
