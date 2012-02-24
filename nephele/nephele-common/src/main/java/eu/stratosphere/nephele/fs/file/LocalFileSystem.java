/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/**
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. 
 */

package eu.stratosphere.nephele.fs.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.fs.BlockLocation;
import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FSDataOutputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.Path;

/**
 * The class <code>LocalFile</code> provides an implementation of the {@link FileSystem} interface for the local file
 * system.
 * 
 * @author warneke
 */
public class LocalFileSystem extends FileSystem {

	/**
	 * Path pointing to the current working directory.
	 */
	private Path workingDir = null;

	/**
	 * The URI representing the local file system.
	 */
	private final URI name = URI.create("file:///");

	/**
	 * The host name of this machine;
	 */
	private final String hostName;

	private static final Log LOG = LogFactory.getLog(LocalFileSystem.class);

	/**
	 * Constructs a new <code>LocalFileSystem</code> object.
	 */
	public LocalFileSystem() {
		this.workingDir = new Path(System.getProperty("user.dir")).makeQualified(this);

		String tmp = "unknownHost";

		try {
			tmp = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			LOG.error(e);
		}

		this.hostName = tmp;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len)
			throws IOException {

		final BlockLocation[] blockLocations = new BlockLocation[1];
		blockLocations[0] = new LocalBlockLocation(this.hostName, file.getLen());

		return blockLocations;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FileStatus getFileStatus(Path f) throws IOException {

		final File path = pathToFile(f);
		if (path.exists()) {
			return new LocalFileStatus(pathToFile(f), this);
		} else {
			throw new FileNotFoundException("File " + f + " does not exist.");
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public URI getUri() {

		return name;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Path getWorkingDirectory() {

		return workingDir;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initialize(final URI name) throws IOException {
		// TODO Auto-generated method stub

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
		// TODO Auto-generated method stub
		return open(f);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataInputStream open(final Path f) throws IOException {

		final File file = pathToFile(f);

		return new LocalDataInputStream(file);
	}

	/**
	 * {@inheritDoc}
	 */
	private File pathToFile(Path path) {

		if (!path.isAbsolute()) {
			path = new Path(getWorkingDirectory(), path);
		}
		return new File(path.toUri().getPath());
	}

	/**
	 * {@inheritDoc}
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean delete(final Path f, final boolean recursive) throws IOException {

		final File file = pathToFile(f);
		if (file.isFile()) {
			return file.delete();
		} else if ((!recursive) && file.isDirectory() && (file.listFiles().length != 0)) {
			throw new IOException("Directory " + file.toString() + " is not empty");
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
			for (int i = 0; i < files.length; i++) {
				final boolean del = delete(files[i]);
				if (del == false) {
					return false;
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
	public boolean mkdirs(final Path f) throws IOException {

		final Path parent = f.getParent();
		final File p2f = pathToFile(f);
		return (parent == null || mkdirs(parent)) && (p2f.mkdir() || p2f.isDirectory());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite, final int bufferSize,
			final short replication, final long blockSize) throws IOException {

		if (exists(f) && !overwrite) {
			throw new IOException("File already exists:" + f);
		}

		final Path parent = f.getParent();
		if (parent != null && !mkdirs(parent)) {
			throw new IOException("Mkdirs failed to create " + parent.toString());
		}

		final File file = pathToFile(f);
		return new LocalDataOutputStream(file);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public FSDataOutputStream create(final Path f, final boolean overwrite) throws IOException {

		return create(f, overwrite, 0, (short) 0, 0);
	}
}
