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
import java.io.IOException;
import java.nio.channels.FileChannel;

public final class ReadableSpillingFile {

	private boolean readableChannelLocked = false;

	private final File physicalFile;

	private final FileChannel readableFileChannel;

	private int numberOfBuffers;
	
	public ReadableSpillingFile(final File physicalFile, int numberOfBuffers) throws IOException {
		this.physicalFile = physicalFile;
		this.numberOfBuffers = numberOfBuffers;
		this.readableFileChannel = new FileInputStream(this.physicalFile).getChannel();
	}

	public synchronized boolean isReadableChannelLocked() {

		return this.readableChannelLocked;
	}

	public File getPhysicalFile() {
		return this.physicalFile;
	}

	public synchronized FileChannel lockReadableFileChannel() {

		if (this.readableChannelLocked) {
			return null;
		}

		this.readableChannelLocked = true;

		return this.readableFileChannel;
	}

	public synchronized void unlockReadableFileChannel() throws IOException {

		if (!this.readableChannelLocked) {
			return;
		}
		
		this.readableChannelLocked = false;
		this.notify();
	}

	public synchronized boolean checkForEndOfFile() throws IOException {

		--this.numberOfBuffers;
		
		if (this.numberOfBuffers == 0) {
			// Close the file
			this.readableFileChannel.close();

			this.physicalFile.delete();
			return true;
		}
		
		return false;
	}

	public synchronized void increaseNumberOfBuffers() {
		
		++this.numberOfBuffers;
	}
}
