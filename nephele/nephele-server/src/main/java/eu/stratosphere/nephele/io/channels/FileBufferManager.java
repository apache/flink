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
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
<<<<<<< HEAD
import eu.stratosphere.nephele.io.InputGate;
=======
>>>>>>> experimental
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.FileUtils;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The file buffer manager manages the physical files which may be used to store the output or input of
 * {@link AbstractByteBufferedOutputChannel} or {@link AbstractByteBufferedInputChannel} objects, respectively. It is
 * designed as a singleton object.
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
	 * The singleton instance of the file buffer manager.
	 */
	private static FileBufferManager fileBufferManager = null;
	
	/**
	 * The directory to store temporary files.
	 */
	private final String tmpDir;

	private static enum FileEntryStatus {
		CLOSED, WRITING, WRITING_BUT_READ_REQUESTED
	};

	/**
	 * Stores the location of the directory for temporary files.
	 */
	private final String tmpDir;

	private final Map<ChannelID, Object> channelGroupMap = new ConcurrentHashMap<ChannelID, Object>();

	private final Map<Object, WritableSpillingFile> writableSpillingFileMap = new HashMap<Object, WritableSpillingFile>();

	private final Map<Object, Queue<ReadableSpillingFile>> readableSpillingFileMap = new HashMap<Object, Queue<ReadableSpillingFile>>();

	private final Set<ChannelID> canceledChannels;

	public FileBufferManager(final Set<ChannelID> canceledChannels) {

		this.tmpDir = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);

		this.canceledChannels = canceledChannels;
	}

	public FileChannel getFileChannelForReading(final ChannelID sourceChannelID) throws IOException,
			InterruptedException {

		final Object groupObject = this.channelGroupMap.get(sourceChannelID);
		if (groupObject == null) {
			throw new IOException("Cannot find input gate for source channel ID " + sourceChannelID);
		}

		Queue<ReadableSpillingFile> queue = null;
		synchronized (this.readableSpillingFileMap) {
			queue = this.readableSpillingFileMap.get(groupObject);
			if (queue == null) {
				queue = new ArrayDeque<ReadableSpillingFile>(1);
				this.readableSpillingFileMap.put(groupObject, queue);
			}
		}

		ReadableSpillingFile readableSpillingFile = null;
		synchronized (queue) {

			while (queue.isEmpty()) {

				synchronized (this.writableSpillingFileMap) {
					WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(groupObject);
					if (writableSpillingFile != null) {
						writableSpillingFile.requestReadAccess();

						if (writableSpillingFile.isSafeToClose()) {
							writableSpillingFile.close();
							this.writableSpillingFileMap.remove(groupObject);
							queue.add(new ReadableSpillingFile(writableSpillingFile.getPhysicalFile()));
						}
					}
				}

				if (queue.isEmpty()) {
					queue.wait(WritableSpillingFile.MAXIMUM_TIME_WITHOUT_WRITE_ACCESS);
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
		 */
		private synchronized FileChannel getFileChannelForWriting() throws IOException {

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

		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}
	}

	private Map<ChannelID, FileBufferManagerEntry> dataSources = new HashMap<ChannelID, FileBufferManagerEntry>();

	private FileBufferManager() {
		this.tmpDir = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
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

		FileBufferManagerEntry fbme = null;
		synchronized (this.dataSources) {

			fbme = this.dataSources.get(sourceChannelID);

			// Clean up
			// TODO: Fix this
			/*
			 * if(fbme.cleanUpPossible()) {
			 * this.dataSources.remove(sourceChannelID);
			 * }
			 */
		}

		if (fbme == null) {
			LOG.error("Cannot find data source for channel " + sourceChannelID + " to mark buffer as consumed");
			return;
		}

		try {
			fbme.checkForEndOfFile();
		} catch (IOException ioe) {
			LOG.error(ioe);
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

	public static synchronized FileBufferManager getFileBufferManager() {

		if(fileBufferManager == null) {
			fileBufferManager = new FileBufferManager();
		}
		
		return fileBufferManager;
	}

	public void shutDown() {
		// TODO: Implement me
	}
}
