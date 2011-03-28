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

	private final long fileSize;

	private long lastPosition = 0;
	
	private final FileChannel readableFileChannel;

	public ReadableSpillingFile(final File physicalFile) throws IOException {
		this.physicalFile = physicalFile;
		this.fileSize = physicalFile.length();
		this.readableFileChannel = new FileInputStream(this.physicalFile).getChannel();
	}

	public synchronized boolean isReadableChannelLocked() {

		return this.readableChannelLocked;
	}

	public File getPhysicalFile() {
		return this.physicalFile;
	}

	public synchronized FileChannel lockReadableFileChannel(final ChannelID sourceChannelID) throws InterruptedException {

		while (this.readableChannelLocked) {

			System.out.println("Waiting for lock on " + this.readableFileChannel);
			this.wait();
		}

		this.readableChannelLocked = true;
		
		/*try {
			System.out.println("---- Locking read channel at position " + this.readableFileChannel.position() + " for " + sourceChannelID);
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}*/
		
		return this.readableFileChannel;
	}

	public synchronized void unlockReadableFileChannel(final ChannelID sourceChannelID) {

		this.readableChannelLocked = false;
		this.notify();
		
		try {
			if(this.readableFileChannel.position() < this.lastPosition) {
				System.out.println("READ Invalid position " + this.readableFileChannel.position() + ", last was" + this.lastPosition);
			}
			this.lastPosition = this.readableFileChannel.position();
			//System.out.println("---- Unlocking read channel at position " + this.lastPosition  + " for " + sourceChannelID);
			} catch(IOException e) {
				e.printStackTrace();
			}
			
			
	}

	public synchronized boolean checkForEndOfFile() throws IOException {

		if (this.readableFileChannel.position() >= this.fileSize) {
			// Close the file
			this.readableFileChannel.close();

			this.physicalFile.delete();
			return true;
		}

		return false;
	}

}
