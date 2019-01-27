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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Maintains set of files and open files to be removed later.
 */
public class ChannelDeleteRegistry<T> {
	/** Collection of all currently open files, to be closed and deleted during cleanup. */
	private final HashSet<FileIOChannel> openChannels = new HashSet<>(64);

	/** Collection of all temporary files created and to be removed later. */
	private final HashSet<FileIOChannel.ID> channelsToBeDelete = new HashSet<>(64);

	public void registerOpenChannel(FileIOChannel channel) {
		openChannels.add(channel);
	}

	public void unregisterOpenChannel(FileIOChannel channel) {
		openChannels.remove(channel);
	}

	public void registerChannelToBeDelete(FileIOChannel.ID file) {
		channelsToBeDelete.add(file);
	}

	public void unregisterChannelToBeDelete(FileIOChannel.ID file) {
		channelsToBeDelete.remove(file);
	}

	void clearOpenFiles() {
		// we have to loop this, because it may fail with a concurrent modification exception
		while (!this.openChannels.isEmpty()) {
			try {
				for (Iterator<FileIOChannel> channels = this.openChannels.iterator(); channels.hasNext(); ) {
					final FileIOChannel channel = channels.next();
					channels.remove();
					channel.closeAndDelete();
				}
			}
			catch (Throwable t) {}
		}
	}

	void clearFiles() {
		// we have to loop this, because it may fail with a concurrent modification exception
		while (!this.channelsToBeDelete.isEmpty()) {
			try {
				for (Iterator<FileIOChannel.ID> channels = this.channelsToBeDelete.iterator(); channels.hasNext(); ) {
					final FileIOChannel.ID channel = channels.next();
					channels.remove();
					try {
						final File f = new File(channel.getPath());
						if (f.exists()) {
							f.delete();
						}
					} catch (Throwable t) {}
				}
			}
			catch (Throwable t) {}
		}
	}
}
