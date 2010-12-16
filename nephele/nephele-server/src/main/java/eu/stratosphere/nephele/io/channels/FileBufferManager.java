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
public class FileBufferManager {

	/**
	 * The logging object.
	 */
	private static final Log LOG = LogFactory.getLog(FileBufferManager.class);

	private final String tmpDir;

	/**
	 * Objects of this class store management information of each channel
	 * pair that uses file buffers.
	 * <p>
	 * This class is thread-safe.
	 * 
	 * @author warneke
	 */
	private class FileBufferManagerEntry {

		/**
		 * Stores whether the reading thread is currently waiting
		 * for an input file to be closed.
		 */
		private boolean readAttemptInProgress = false;

		/**
		 * Stores if there is currently a writing thread which
		 * could write the input file.
		 */
		private boolean writeAttemptInProgress = false;

		/**
		 * Stores whether the data written to disk by this
		 * channel are temporary and can be deleted after the
		 * first read.
		 */
		private final boolean isTemporaryFile;

		/**
		 * The channel which is currently used to
		 * read from an output file.
		 */
		private FileChannel fileChannelForReading = null;

		/**
		 * The channel which is currently used to
		 * write to the input file.
		 */
		private FileChannel fileChannelForWriting = null;

		/**
		 * The size of the file from which is reading thread
		 * currently reads in bytes.
		 */
		private long fileSizeForReading = -1;

		/**
		 * A list of output files ready to be read.
		 */
		private Deque<File> filesForReading = new ArrayDeque<File>();

		/**
		 * The file which is used as the current input file.
		 */
		private File currentFileForWriting = null;

		/**
		 * Constructs a new entry object.
		 * 
		 * @param isTemporaryFile
		 *        <code>true</code> if the files created by this object are
		 *        temporary and can be deleted after being read once, <code>false</code> otherwise
		 */
		private FileBufferManagerEntry(boolean isTemporaryFile) {

			this.isTemporaryFile = isTemporaryFile;
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
		 */
		private synchronized FileChannel getFileChannelForReading() throws IOException {

			if (this.fileChannelForReading != null) {
				return this.fileChannelForReading;
			}

			this.readAttemptInProgress = true;

			try {

				while (this.filesForReading.isEmpty()) {

					// System.out.println("Size: " + this.filesForReading.size() + ", " + this.writeAttemptInProgress +
					// ", " + this.fileChannelForWriting);
					if (!this.writeAttemptInProgress && this.fileChannelForWriting != null) {
						closeCurrentWriteFile();
					} else {
						this.wait();
					}
				}

				final File file = this.filesForReading.peek();
				this.fileSizeForReading = file.length();
				final FileInputStream fis = new FileInputStream(file);
				this.fileChannelForReading = fis.getChannel();
			} catch (InterruptedException e) {
				e.printStackTrace(); // TODO: Handle this correctly
			} finally {
				this.readAttemptInProgress = false;
			}

			return this.fileChannelForReading;
		}

		/**
		 * Returns the channel the writing thread is supposed to use to
		 * write data to the file.
		 * 
		 * @return the channel object the writing thread is supposed to use
		 * @throws IOException
		 *         thrown if an error occurs while creating the channel object
		 */
		private synchronized FileChannel getFileChannelForWriting() throws IOException {

			if (this.fileChannelForWriting == null) {
				final String filename = tmpDir + File.separator + FileUtils.getRandomFilename("fb_");
				this.currentFileForWriting = new File(filename);
				final FileOutputStream fos = new FileOutputStream(this.currentFileForWriting);
				this.fileChannelForWriting = fos.getChannel();
			}

			this.writeAttemptInProgress = true;
			return this.fileChannelForWriting;
		}

		/**
		 * Checks whether the end of the current output file is reached
		 * and potentially deletes the file.
		 * 
		 * @throws IOException
		 *         thrown if an error occurs while checking for
		 *         end-of-file or deleting it
		 */
		private synchronized void checkForEndOfFile() throws IOException {

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
			}
		}

		/**
		 * Closes the current input file and adds it to
		 * the list of files which are ready to be read
		 * from.
		 * 
		 * @throws IOException
		 *         thrown if an error occurs while closing the input file
		 */
		private void closeCurrentWriteFile() throws IOException {

			this.fileChannelForWriting.close();
			this.filesForReading.add(this.currentFileForWriting);
			this.currentFileForWriting = null;
			this.fileChannelForWriting = null;
			notify();
		}

		private synchronized boolean cleanUpPossible() {

			return (this.fileChannelForWriting == null);
		}

		private synchronized void reportEndOfWritePhase() throws IOException {

			if (this.readAttemptInProgress) {
				closeCurrentWriteFile();
			}

			this.writeAttemptInProgress = false;
		}
	}

	private Map<ChannelID, FileBufferManagerEntry> dataSources = new HashMap<ChannelID, FileBufferManagerEntry>();

	public FileBufferManager(String tmpDir) {
		this.tmpDir = tmpDir;
	}

	public void registerExternalDataSourceForChannel(ChannelID sourceChannelID, String filename) throws IOException {

		registerExternalDataSourceForChannel(sourceChannelID, new File(filename));
	}

	public void registerExternalDataSourceForChannel(ChannelID sourceChannelID, File file) throws IOException {

		if (!file.exists()) {
			throw new IOException("External data source " + file + " does not exist");
		}

		FileBufferManagerEntry fbme;
		synchronized (this.dataSources) {

			fbme = this.dataSources.get(sourceChannelID);
			if (fbme == null) {
				fbme = new FileBufferManagerEntry(false);
				this.dataSources.put(sourceChannelID, fbme);
			}
		}
		fbme.addFileForReading(file);
	}

	public FileChannel getFileChannelForReading(ChannelID sourceChannelID) throws IOException {

		FileBufferManagerEntry fbme;
		synchronized (this.dataSources) {

			fbme = this.dataSources.get(sourceChannelID);
			if (fbme == null) {
				final IOException ioe = new IOException("Cannot find data source for channel " + sourceChannelID);
				LOG.error(ioe);
				throw ioe;
			}

		}
		return fbme.getFileChannelForReading();
	}

	public FileChannel getFileChannelForWriting(ChannelID sourceChannelID) throws IOException {

		FileBufferManagerEntry fbme;
		synchronized (this.dataSources) {

			fbme = this.dataSources.get(sourceChannelID);
			if (fbme == null) {
				fbme = new FileBufferManagerEntry(true);
				this.dataSources.put(sourceChannelID, fbme);
			}
		}

		return fbme.getFileChannelForWriting();
	}

	public void reportFileBufferAsConsumed(ChannelID sourceChannelID) {

		synchronized (this.dataSources) {

			FileBufferManagerEntry fbme = this.dataSources.get(sourceChannelID);
			if (fbme == null) {
				LOG.error("Cannot find data source for channel " + sourceChannelID + " to mark buffer as consumed");
				return;
			}

			try {
				fbme.checkForEndOfFile();
			} catch (IOException ioe) {
				LOG.error(ioe);
			}

			// Clean up
			// TODO: Fix this
			/*
			 * if(fbme.cleanUpPossible()) {
			 * this.dataSources.remove(sourceChannelID);
			 * }
			 */
		}
	}

	public void reportEndOfWritePhase(ChannelID sourceChannelID) throws IOException {

		FileBufferManagerEntry fbme;
		synchronized (this.dataSources) {
			fbme = this.dataSources.get(sourceChannelID);
		}

		if (fbme == null) {
			throw new IOException("Cannot find file buffer manager entry for source channel " + sourceChannelID);
		}

		fbme.reportEndOfWritePhase();
	}
}
