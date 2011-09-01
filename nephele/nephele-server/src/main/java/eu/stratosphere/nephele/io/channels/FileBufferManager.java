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
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.AbstractByteBufferedOutputChannel;
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

	public static final String FILE_BUFFER_PREFIX = "fb_";

	/**
	 * Stores the location of the directory for temporary files.
	 */
	private final String tmpDir;

	private final Map<AbstractID, WritableSpillingFile> writableSpillingFileMap = new HashMap<AbstractID, WritableSpillingFile>();

	private final Map<AbstractID, Map<FileID, ReadableSpillingFile>> readableSpillingFileMap = new HashMap<AbstractID, Map<FileID, ReadableSpillingFile>>();

	private static FileBufferManager instance = null;

	public static synchronized FileBufferManager getInstance() {

		if (instance == null) {
			instance = new FileBufferManager();
		}

		return instance;
	}

	private FileBufferManager() {

		this.tmpDir = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
	}

	private ReadableSpillingFile getReadableSpillingFile(final AbstractID ownerID, final FileID fileID)
			throws IOException, InterruptedException {

		if (ownerID == null) {
			throw new IllegalStateException("ownerID is null");
		}

		if (fileID == null) {
			throw new IllegalStateException("fileID is null");
		}

		Map<FileID, ReadableSpillingFile> map = null;
		synchronized (this.readableSpillingFileMap) {
			map = this.readableSpillingFileMap.get(ownerID);
			if (map == null) {
				map = new HashMap<FileID, ReadableSpillingFile>();
				this.readableSpillingFileMap.put(ownerID, map);
			}
		}

		synchronized (map) {

			while (!map.containsKey(fileID)) {

				synchronized (this.writableSpillingFileMap) {
					WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(ownerID);
					if (writableSpillingFile != null) {
						writableSpillingFile.requestReadAccess();

						if (writableSpillingFile.isSafeToClose()) {
							writableSpillingFile.close();
							this.writableSpillingFileMap.remove(ownerID);
							map.put(
								writableSpillingFile.getFileID(),
								writableSpillingFile.toReadableSpillingFile());
						}
					}
				}

				if (!map.containsKey(fileID)) {
					map.wait(WritableSpillingFile.MAXIMUM_TIME_WITHOUT_WRITE_ACCESS);
				}
			}

			return map.get(fileID);
		}
	}

	public FileChannel getFileChannelForReading(final AbstractID ownerID, final FileID fileID) throws IOException,
			InterruptedException {

		return getReadableSpillingFile(ownerID, fileID).lockReadableFileChannel();
	}

	public void increaseBufferCounter(final AbstractID ownerID, final FileID fileID) throws IOException,
			InterruptedException {

		getReadableSpillingFile(ownerID, fileID).increaseNumberOfBuffers();
	}

	public void decreaseBufferCounter(final AbstractID ownerID, final FileID fileID) {

		try {
			Map<FileID, ReadableSpillingFile> map = null;
			synchronized (this.readableSpillingFileMap) {
				map = this.readableSpillingFileMap.get(ownerID);
				if (map == null) {
					throw new IOException("Cannot find readable spilling file queue for owner ID " + ownerID);
				}

				ReadableSpillingFile readableSpillingFile = null;
				synchronized (map) {
					readableSpillingFile = map.get(fileID);
					if (readableSpillingFile == null) {
						throw new IOException("Cannot find readable spilling file for owner ID " + ownerID);
					}

					if (readableSpillingFile.checkForEndOfFile()) {
						map.remove(fileID);
						if (map.isEmpty()) {
							this.readableSpillingFileMap.remove(ownerID);
						}
					}
				}
			}
		} catch (IOException ioe) {
			LOG.error(StringUtils.stringifyException(ioe));
		}
	}

	public void releaseFileChannelForReading(final AbstractID ownerID, final FileID fileID) {

		try {
			getReadableSpillingFile(ownerID, fileID).unlockReadableFileChannel();
		} catch (Exception e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	/**
	 * Locks and returns a file channel from a {@link WritableSpillingFile}.
	 * 
	 * @param ownerID
	 *        the ID of the owner the file channel shall be locked for
	 * @return the file channel object if the lock could be acquired or <code>null</code> if the locking operation
	 *         failed
	 * @throws IOException
	 *         thrown if no spilling for the given channel ID could be allocated
	 */
	public FileChannel getFileChannelForWriting(final AbstractID ownerID) throws IOException {

		synchronized (this.writableSpillingFileMap) {

			WritableSpillingFile writableSpillingFile = this.writableSpillingFileMap.get(ownerID);
			if (writableSpillingFile == null) {
				final FileID fileID = new FileID();
				final String filename = this.tmpDir + File.separator + FILE_BUFFER_PREFIX + fileID;
				writableSpillingFile = new WritableSpillingFile(fileID, new File(filename));
				this.writableSpillingFileMap.put(ownerID, writableSpillingFile);
			}

			return writableSpillingFile.lockWritableFileChannel();
		}
	}

	/**
	 * Returns the lock for a file channel of a {@link WritableSpillingFile}.
	 * 
	 * @param ownerID
	 *        the ID of the owner the lock has been acquired for
	 * @param currentFileSize
	 *        the size of the file after the last write operation using the locked file channel
	 * @throws IOException
	 *         thrown if the lock could not be released
	 */
	public FileID reportEndOfWritePhase(final AbstractID ownerID, final long currentFileSize) throws IOException {

		WritableSpillingFile writableSpillingFile = null;
		boolean removed = false;
		synchronized (this.writableSpillingFileMap) {

			writableSpillingFile = this.writableSpillingFileMap.get(ownerID);
			if (writableSpillingFile == null) {
				throw new IOException("Cannot find writable spilling file for owner ID " + ownerID);
			}

			writableSpillingFile.unlockWritableFileChannel(currentFileSize);

			if (writableSpillingFile.isReadRequested() && writableSpillingFile.isSafeToClose()) {
				this.writableSpillingFileMap.remove(ownerID);
				removed = true;
			}
		}

		if (removed) {
			writableSpillingFile.close();
			Map<FileID, ReadableSpillingFile> map = null;
			synchronized (this.readableSpillingFileMap) {
				map = this.readableSpillingFileMap.get(ownerID);
				if (map == null) {
					map = new HashMap<FileID, ReadableSpillingFile>();
					this.readableSpillingFileMap.put(ownerID, map);
				}
			}

			synchronized (map) {
				map.put(writableSpillingFile.getFileID(),
					writableSpillingFile.toReadableSpillingFile());
				map.notify();
			}
		}

		return writableSpillingFile.getFileID();
	}
}