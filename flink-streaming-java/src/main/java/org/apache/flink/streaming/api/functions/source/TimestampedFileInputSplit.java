/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * An extended {@link FileInputSplit} that also includes information about:
 * <ul>
 *     <li>The modification time of the file this split belongs to.</li>
 *     <li>When checkpointing, the state of the split at the moment of the checkpoint.</li>
 * </ul>
 * This class is used by the {@link ContinuousFileMonitoringFunction} and the
 * {@link ContinuousFileReaderOperator} to perform continuous file processing.
 * */
public class TimestampedFileInputSplit extends FileInputSplit implements Comparable<TimestampedFileInputSplit>{

	/** The modification time of the file this split belongs to. */
	private final long modificationTime;

	/**
	 * The state of the split. This information is used when
	 * restoring from a checkpoint and allows to resume reading the
	 * underlying file from the point we left off.
	 * */
	private Serializable splitState;

	/**
	 * Creates a {@link TimestampedFileInputSplit} based on the file modification time and
	 * the rest of the information of the {@link FileInputSplit}, as returned by the
	 * underlying filesystem.
	 *
	 * @param modificationTime the modification file of the file this split belongs to
	 * @param num    the number of this input split
	 * @param file   the file name
	 * @param start  the position of the first byte in the file to process
	 * @param length the number of bytes in the file to process (-1 is flag for "read whole file")
	 * @param hosts  the list of hosts containing the block, possibly {@code null}
	 */
	public TimestampedFileInputSplit(long modificationTime, int num, Path file, long start, long length, String[] hosts) {
		super(num, file, start, length, hosts);

		Preconditions.checkArgument(modificationTime >= 0 || modificationTime == Long.MIN_VALUE,
			"Invalid File Split Modification Time: " + modificationTime + ".");

		this.modificationTime = modificationTime;
	}

	/**
	 * Sets the state of the split. This information is used when restoring from a checkpoint and
	 * allows to resume reading the underlying file from the point we left off.
	 *
	 * <p>* This is applicable to
	 * {@link org.apache.flink.api.common.io.FileInputFormat FileInputFormats} that implement the
	 * {@link org.apache.flink.api.common.io.CheckpointableInputFormat} interface.
	 * */
	public void setSplitState(Serializable state) {
		this.splitState = state;
	}

	/**
	 * Sets the state of the split to {@code null}.
	 */
	public void resetSplitState() {
		this.setSplitState(null);
	}

	/** @return the state of the split. */
	public Serializable getSplitState() {
		return this.splitState;
	}

	/** @return The modification time of the file this split belongs to. */
	public long getModificationTime() {
		return this.modificationTime;
	}

	@Override
	public int compareTo(TimestampedFileInputSplit o) {
		int modTimeComp = Long.compare(this.modificationTime, o.modificationTime);
		if (modTimeComp != 0L) {
			return modTimeComp;
		}

		// the file input split does not prevent null paths.
		if (this.getPath() == null && o.getPath() != null) {
			return 1;
		} else if (this.getPath() != null && o.getPath() == null) {
			return -1;
		}

		int pathComp = this.getPath() == o.getPath() ? 0 :
			this.getPath().compareTo(o.getPath());

		return pathComp != 0 ? pathComp :
			this.getSplitNumber() - o.getSplitNumber();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o instanceof TimestampedFileInputSplit && super.equals(o)) {
			TimestampedFileInputSplit that = (TimestampedFileInputSplit) o;
			return this.modificationTime == that.modificationTime;
		}
		return false;
	}

	@Override
	public int hashCode() {
		int res = 37 * (int) (this.modificationTime ^ (this.modificationTime >>> 32));
		return 37 * res + super.hashCode();
	}

	@Override
	public String toString() {
		return "[" + getSplitNumber() + "] " + getPath() + " mod@ " +
			modificationTime + " : " + getStart() + " + " + getLength();
	}
}
