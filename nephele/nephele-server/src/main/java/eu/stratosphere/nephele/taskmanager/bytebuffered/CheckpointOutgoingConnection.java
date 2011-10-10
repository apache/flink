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
package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;


/**
 * @author marrus
 *
 */
public class CheckpointOutgoingConnection extends OutgoingConnection {

	private static final int DEFAULT_BUFFER_SIZE_IN_BYTES = 64 * 1024; // 64k
	private int numberOfConnectionRetries;
	private int bytesread = 0;
	private FileChannel readableByteChannel;
	private static final Log LOG = LogFactory.getLog(CheckpointOutgoingConnection.class);

	private boolean isSubscribedToWriteEvent= false;

	private long eof = 0;

	/**
	 * Initializes CheckpointOutgoingConnection.
	 *
	 * @param byteBufferedChannelManager
	 * @param connectionAddress
	 * @param connectionThread
	 * @param numberOfConnectionRetries
	 */
	public CheckpointOutgoingConnection(ByteBufferedChannelManager byteBufferedChannelManager,
			InetSocketAddress connectionAddress, OutgoingConnectionThread connectionThread,
			int numberOfConnectionRetries, FileChannel fileInputChannel) {
		super(byteBufferedChannelManager, connectionAddress, connectionThread, numberOfConnectionRetries);
		this.readableByteChannel = fileInputChannel;
		
	}
	/**
	 * @param l Size of Checkpoint-File
	 * @throws IOException
	 */
	public void write(final long l) throws IOException {

		if (!this.isConnected) {

			this.retriesLeft = this.numberOfConnectionRetries;
			this.timstampOfLastRetry = System.currentTimeMillis();
			this.connectionThread.triggerConnect(this);
			this.isConnected = true;
			this.isSubscribedToWriteEvent = true;
		} else {

			if (!this.isSubscribedToWriteEvent) {
				this.connectionThread.subscribeToWriteEvent(this.selectionKey);
				this.isSubscribedToWriteEvent = true;
			}
		}
		while(this.selectionKey == null){
			System.out.println("selektionkey null");
			this.connectionThread.triggerConnect(this);
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		WritableByteChannel writableByteChannel = null;
		writableByteChannel = (WritableByteChannel) super.selectionKey.channel();

		synchronized (writableByteChannel) {

			ByteBuffer tempBuffer = ByteBuffer.allocate(this.DEFAULT_BUFFER_SIZE_IN_BYTES);
			while(l != this.bytesread){

				try{
					this.eof = this.readableByteChannel.transferTo(this.bytesread,l-this.bytesread,writableByteChannel );

					this.bytesread += this.eof;
					LOG.debug(" transfered " + this.eof + " ALL " + this.bytesread + " of " +l );
				}catch(IOException e){
					//Catch "java.io.IOException: Resource temporarily unavailable"
					//See BUG http://bugs.sun.com/view_bug.do?bug_id=5103988

					e.printStackTrace();
					try {
						Thread.sleep(120000); //2 min
					} catch (InterruptedException ie) {
						ie.printStackTrace();
					}
				}
			}
		}
	}
	
	
	

}
