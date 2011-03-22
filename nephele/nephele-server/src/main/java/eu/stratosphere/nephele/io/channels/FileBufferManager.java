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

package eu.stratosphere.nephele.io.channels;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.util.FileUtils;

/**
 * The file buffer manager is required to manage the mapping between {@link FileBuffer} objects
 * and the concrete files that back the respective buffers. By means of the file buffer manager
 * a sequence of potentially small {@link FileBuffer} objects can be backed by the single potentially
 * larger file. As a result, the system does not need to work a large set of small files but can write to
 * or read from a single file instead. This is potentially more efficient.
 * <p>
 * The file buffer manager continues to append the content of {@link FileBuffer} objects to the same file until the
 * first requires to read data from that file. At that point the file is closed and a new file is opened for writing.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public final class FileBufferManager {

	/**
	 * The logging object.
	 */
	private static final Log LOG = LogFactory.getLog(FileBufferManager.class);

	/**
	 * Stores the location of the directory for temporary files.
	 */
	private final String tmpDir;

	/**
	 * The key to configure the maximum number of simultaneously opened files.
	 */
	private static final String FILE_LIMIT_KEY = "channel.network.spillLimit";

	/**
	 * The default for the maximum number of simultaneously opened files.
	 */
	private static final int DEFAULT_FILE_LIMIT = 32;

	/**
	 * The maximum number of simultaneously opened files allowed.
	 */
	private final int maximumNumberOfOpenedFiles;

	/**
	 * This enumeration indicates the possible states of the write channel.
	 * 
	 * @author warneke
	 */
	private static enum FileEntryStatus {

		CLOSED, WRITING, WRITING_BUT_READ_REQUESTED
	};

	/**
	 * Objects of this class store management information of each channel
	 * pair that uses file buffers.
	 * <p>
	 * This class is thread-safe.
	 * 
	 * @author warneke
	 */
	private static final class FileBufferManagerEntry {

		/**
		 * Current state of the write channel.
		 */
		private FileEntryStatus status = FileEntryStatus.CLOSED;

		/**
		 * Stores whether the data written to disk by this
		 * channel are temporary and can be deleted after the
		 * first read.
		 */
		private final boolean isTemporaryFile;

		private FileChannel fileChannelForReading = null;

		private FileChannel fileChannelForWriting = null;

		/**
		 * The size of the file from which is reading thread
		 * currently reads in bytes.
		 */
		private long fileSizeForReading = -1;

		/**
		 * A list of output files ready to be read.
		 */
		private final Deque<File> filesForReading = new ArrayDeque<File>();

		/**
		 * The location of directory for temporary files.
		 */
		private final String tmpDir;

		private File currentFileForWriting = null;

		/**
		 * Constructs a new entry object.
		 * 
		 * @param isTemporaryFile
		 *        <code>true</code> if the files created by this object are
		 *        temporary and can be deleted after being read once, <code>false</code> otherwise
		 * @param tmpDir
		 *        the location of directory for temporary files
		 */
		private FileBufferManagerEntry(final boolean isTemporaryFile, final String tmpDir) {

			this.isTemporaryFile = isTemporaryFile;
			this.tmpDir = tmpDir;
		}

		/**
		 * Adds an already existing output file to the list
		 * of files which are ready to be read.
		 * 
		 * @param file
		 *        the file to be added to the output file list
		 */
		private synchronized void addFileForReading(File file) {

			this.filesForReading.add(file);
			notify();
		}

		/**
		 * Returns the channel object the reading thread is supposed to use
		 * to consume data from the file.
		 * <p>
		 * This method may block until at least one file to read from is available.
		 * 
		 * @return the channel object to consume data from the file
		 * @throws IOException
		 *         thrown if an error occurs while creating the channel object
		 * @throws InterruptedException
		 *         thrown if the thread is interrupted while waiting for the file resources to become available
		 */
		private synchronized FileChannel getFileChannelForReading() throws IOException, InterruptedException {

			if (this.fileChannelForReading != null) {
				return this.fileChannelForReading;
			}

			try {

				if (this.status == FileEntryStatus.CLOSED) {
					closeCurrentWriteFile();
				}

				while (this.filesForReading.isEmpty()) {
					this.status = FileEntryStatus.WRITING_BUT_READ_REQUESTED;
					this.wait();
				}

				final File file = this.filesForReading.peek();
				this.fileSizeForReading = file.length();
				final FileInputStream fis = new FileInputStream(file);
				this.fileChannelForReading = fis.getChannel();
			} catch (InterruptedException e) {
				LOG.error(e);
			}

			return this.fileChannelForReading;
		}

		private void closeCurrentWriteFile() throws IOException {

			if (this.fileChannelForWriting != null) {

				this.fileChannelForWriting.close();
				this.fileChannelForWriting = null;

				this.filesForReading.add(this.currentFileForWriting);
				this.notify();
				this.currentFileForWriting = null;
			}
		}

		/**
		 * Returns the channel the writing thread is supposed to use to
		 * write data to the file.
		 * 
		 * @return the channel object the writing thread is supposed to use
		 * @throws IOException
		 *         thrown if an error occurs while creating the channel object
		 * @throws InterruptedException
		 *         thrown if the thread is interrupted while waiting for the file resources to become available
		 */
		private synchronized FileChannel getFileChannelForWriting() throws IOException, InterruptedException {

			if (this.fileChannelForWriting == null) {
				final String filename = tmpDir + File.separator + FileUtils.getRandomFilename("fb_");
				this.currentFileForWriting = new File(filename);
				final FileOutputStream fos = new FileOutputStream(this.currentFileForWriting);
				this.fileChannelForWriting = fos.getChannel();
			}

			this.status = FileEntryStatus.WRITING;
			return this.fileChannelForWriting;
		}

		/**
		 * Checks whether the end of the current output file is reached
		 * and potentially deletes the file.
		 * 
		 * @return <code>true</code> if the object can be removed from the data source map, <code>false</code> otherwise
		 * @throws IOException
		 *         thrown if an error occurs while checking for
		 *         end-of-file or deleting it
		 */
		private synchronized boolean checkForEndOfFile() throws IOException {

			if (this.fileChannelForReading.position() >= this.fileSizeForReading) {
				// Close the file
				this.fileChannelForReading.close();
				this.fileChannelForReading = null;
				this.fileSizeForReading = -1;

				final File file = this.filesForReading.pop();
				if (this.isTemporaryFile) {
					// System.out.println("Deleting " + file.getPath());
					file.delete();
				}

				// Check if entry can be removed
				if (this.fileChannelForWriting == null && this.filesForReading.isEmpty()
					&& this.status == FileEntryStatus.CLOSED) {
					return true;
				}
			}

			return false;
		}

		private synchronized void reportEndOfWritePhase() throws IOException {

			if (this.status == FileEntryStatus.CLOSED) {
				throw new IOException("reportEndOfWritePhase is called, but file entry status is CLOSED");
			}

			if (this.status == FileEntryStatus.WRITING_BUT_READ_REQUESTED) {
				closeCurrentWriteFile();
			}

			this.status = FileEntryStatus.CLOSED;
		}
	}

	private Map<ChannelID, FileBufferManagerEntry> dataSources = new HashMap<ChannelID, FileBufferManagerEntry>();

	public FileBufferManager(String tmpDir) {
		this.tmpDir = tmpDir;
		this.maximumNumberOfOpenedFiles = GlobalConfiguration.getInteger(FILE_LIMIT_KEY, DEFAULT_FILE_LIMIT);
	}

	public void registerExternalDataSourceForChannel(ChannelID sourceChannelID, String filename) throws IOException {

		registerExternalDataSourceForChannel(sourceChannelID, new File(filename));
	}

	public void registerExternalDataSourceForChannel(ChannelID sourceChannelID, File file) throws IOException {

		if (!file.exists()) {
			throw new IOException("External data source " + file + " does not exist");
		}

		FileBufferManagerEntry fbme = null;
		synchronized (this.dataSources) {
			fbme = this.dataSources.get(sourceChannelID);
			if (fbme == null) {
				fbme = new FileBufferManagerEntry(false, this.tmpDir);
				this.dataSources.put(sourceChannelID, fbme);
			}
		}

		fbme.addFileForReading(file);
	}

	public FileChannel getFileChannelForReading(ChannelID sourceChannelID) throws IOException, InterruptedException {

		FileBufferManagerEntry fbme = null;
		synchronized (this.dataSources) {
			fbme = this.dataSources.get(sourceChannelID);
		}

		if (fbme == null) {
			final IOException ioe = new IOException("Cannot find data source for channel " + sourceChannelID);
			LOG.error(ioe);
			throw ioe;
		}

		return fbme.getFileChannelForReading();
	}

	public FileChannel getFileChannelForWriting(ChannelID sourceChannelID) throws IOException, InterruptedException {

		FileBufferManagerEntry fbme = null;
		synchronized (this.dataSources) {
			fbme = this.dataSources.get(sourceChannelID);
			if (fbme == null) {
				fbme = new FileBufferManagerEntry(true, this.tmpDir);
				this.dataSources.put(sourceChannelID, fbme);
			}
		}

		return fbme.getFileChannelForWriting();
	}

	public void reportFileBufferAsConsumed(ChannelID sourceChannelID) {

		FileBufferManagerEntry fbme = null;

		// It is important to call checkForEndOfFile from data source synchronized block
		synchronized (this.dataSources) {
			fbme = this.dataSources.get(sourceChannelID);

			if (fbme == null) {
				LOG.error("Cannot find data source for channel " + sourceChannelID + " to mark buffer as consumed");
				return;
			}

			try {
				if (fbme.checkForEndOfFile()) {
					this.dataSources.remove(sourceChannelID);
				}
			} catch (IOException ioe) {
				LOG.error(ioe);
			}
		}
	}

	public void reportEndOfWritePhase(ChannelID sourceChannelID) throws IOException {

		FileBufferManagerEntry fbme = null;

		synchronized (this.dataSources) {
			fbme = this.dataSources.get(sourceChannelID);
		}

		if (fbme == null) {
			throw new IOException("Cannot find file buffer manager entry for source channel " + sourceChannelID);
		}

		fbme.reportEndOfWritePhase();
	}

	public boolean hasResourcesAvailable(final ChannelID sourceChannelID) {

		synchronized (this.dataSources) {
			return (this.dataSources.size() < (this.maximumNumberOfOpenedFiles / 2));
		}
	}
}
