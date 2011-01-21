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

package eu.stratosphere.pact.compiler;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileStatus;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.fs.LineReader;
import eu.stratosphere.nephele.fs.Path;

/**
 * The collection of access methods that can be used to retrieve statistical
 * information about the data processed in a job.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DataStatistics {
	// ------------------------------------------------------------------------
	//                             Constants
	// ------------------------------------------------------------------------

	/**
	 * The log for the statistics.
	 */
	private static final Log LOG = LogFactory.getLog(DataStatistics.class);

	/**
	 * A constant indicating a value is unknown.
	 */
	public static final int UNKNOWN = -1;

	/**
	 * The default number of sample lines to consider when calculating the line width.
	 */
	private static final int DEFAULT_NUM_SAMPLES = 10;

	// ------------------------------------------------------------------------
	//                            Members
	// ------------------------------------------------------------------------

	private final Map<String, BasicFileStatistics> fileStatisticsCache;

	private int numLineSamples;

	// ------------------------------------------------------------------------
	//                      Constructor / Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new statistics object.
	 */
	public DataStatistics() {
		this.fileStatisticsCache = new HashMap<String, DataStatistics.BasicFileStatistics>();

		this.numLineSamples = DEFAULT_NUM_SAMPLES;
	}

	// ------------------------------------------------------------------------
	// Accessors
	// ------------------------------------------------------------------------

	/**
	 * Gets the size of the file that is identified by the given path.
	 * The size of the file is given in bytes.
	 * 
	 * @param filePath
	 *        The path to the file.
	 * @return The size of the file, in bytes, or -1, if unknown, or -2, if an error occurred.
	 */
	public BasicFileStatistics getFileStatistics(String filePath) {
		// check the cache
		BasicFileStatistics stats = fileStatisticsCache.get(filePath);
		if (stats == null) {
			stats = new BasicFileStatistics(-1, UNKNOWN, UNKNOWN);
		}

		try {
			URI uri = new Path(filePath).toUri();

			// check if the file is qualified enough
			if (uri.getHost() == null || uri.getPort() == -1) {
				return stats;
			}

			Path file = new Path(uri.getPath());

			// get the filesystem
			FileSystem fs = FileSystem.get(uri);
			List<FileStatus> files = null;

			// get the file info and check whether the cached statistics are still
			// valid.
			{
				FileStatus status = fs.getFileStatus(file);

				if (status.isDir()) {
					FileStatus[] fss = fs.listStatus(file);
					files = new ArrayList<FileStatus>(fss.length);
					boolean unmodified = true;

					for (FileStatus s : fss) {
						if (!s.isDir()) {
							files.add(s);
							if (s.getModificationTime() > stats.fileModTime) {
								stats.fileModTime = s.getModificationTime();
								unmodified = false;
							}
						}
					}

					if (unmodified) {
						return stats;
					}
				} else {
					// check if the statistics are up to date
					if (stats.getLastModificationTime() == status.getModificationTime()) {
						return stats;
					}

					files = new ArrayList<FileStatus>(1);
					files.add(status);
				}
			}

			// calculate the whole length
			stats.fileSize = 0;
			for (FileStatus s : files) {
				stats.fileSize += s.getLen();
			}
			if (stats.fileSize <= 0) {
				return stats;
			}

			// make the samples small for very small files
			int numSamples = Math.min(numLineSamples, (int) (stats.fileSize / 1024));
			if (numSamples < 2) {
				numSamples = 2;
			}

			long offset = 0;
			long bytes = 0; // one byte for the linebreak
			long stepSize = stats.fileSize / numSamples;

			int fileNum = 0;
			int samplesTaken = 0;

			// take the samples
			for (int sampleNum = 0; sampleNum < numSamples && fileNum < files.size(); sampleNum++) {
				FileStatus currentFile = files.get(fileNum);
				FSDataInputStream inStream = null;

				try {
					inStream = fs.open(currentFile.getPath());

					LineReader lineReader = new LineReader(inStream, offset, currentFile.getLen() - offset, 1024);
					byte[] line = lineReader.readLine();
					lineReader.close();

					if (line != null && line.length > 0) {
						samplesTaken++;
						bytes += line.length + 1; // one for the linebreak
					}
				} finally {
					// make a best effort to close
					if (inStream != null) {
						try {
							inStream.close();
						} catch (Throwable t) {
						}
					}
				}

				offset += stepSize;

				// skip to the next file, if necessary
				while (fileNum < files.size() && offset >= (currentFile = files.get(fileNum)).getLen()) {
					offset -= currentFile.getLen();
					fileNum++;
				}
			}

			stats.avgBytesPerRecord = bytes / (float) samplesTaken;
		} catch (IOException ioex) {
			LOG.warn("Could not determine complete statistics for file '" + filePath + "' due to an io error: "
				+ ioex.getMessage());
		} catch (Throwable t) {
			LOG.error("Unexpected problen while getting the file statistics for file '" + filePath + "': "
				+ t.getMessage(), t);
		}

		// cache the statistics
		fileStatisticsCache.put(filePath, stats);

		return stats;
	}

	// ------------------------------------------------------------------------
	//                           Internal classes
	// ------------------------------------------------------------------------

	/**
	 * Encapsulation of the basic statistics the optimizer obtains about a file. Contained are
	 * the size of the file and the average bytes of a single record. The statistics also
	 * have a time-stamp that records the modification time of the file and indicates as such
	 * for which time the statistics were valid.
	 */
	public static final class BasicFileStatistics
	{
		private long fileModTime; // timestamp of the last modification

		private long fileSize; // size of the file(s) in bytes

		private float avgBytesPerRecord; // the average number of bytes for a record

		/**
		 * Creates a new statistics object.
		 * 
		 * @param fileModTime
		 *        The timestamp of the latest modification of any of the involved files.
		 * @param fileSize
		 *        The size of the file, in bytes. <code>-1</code>, if unknown.
		 * @param avgBytesPerRecord
		 *        The average number of byte in a record, or <code>-1.0f</code>, if unknown.
		 */
		public BasicFileStatistics(long fileModTime, long fileSize, float avgBytesPerRecord) {
			this.fileModTime = fileModTime;
			this.fileSize = fileSize;
			this.avgBytesPerRecord = avgBytesPerRecord;
		}

		/**
		 * Gets the file size.
		 * 
		 * @return The fileSize.
		 */
		public long getFileSize() {
			return fileSize;
		}

		/**
		 * Gets the average number of bytes per record.
		 * 
		 * @return The average number of bytes per record.
		 */
		public float getAvgBytesPerRecord() {
			return avgBytesPerRecord;
		}

		/**
		 * Gets the timestamp of the last modification.
		 * 
		 * @return The timestamp of the last modification.
		 */
		public long getLastModificationTime() {
			return fileModTime;
		}
	}

}
