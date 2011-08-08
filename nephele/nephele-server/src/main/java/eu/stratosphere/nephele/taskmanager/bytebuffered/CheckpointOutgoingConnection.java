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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SelectionKey;
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
	private ByteBufferedChannelManager byteBufferedChannelManager;
	private InetSocketAddress connectionAddress;
	private OutgoingConnectionThread connectionThread;
	private int numberOfConnectionRetries;
	private SelectionKey selectionKey;
	private int bytesread = 0;
	private FileChannel readableByteChannel;
	private static final Log LOG = LogFactory.getLog(CheckpointOutgoingConnection.class);
	private final ByteBuffer tempBuffer = ByteBuffer.allocate(64);
	private boolean isConnected= false;
	private int retriesLeft = 0 ;
	private long timstampOfLastRetry;
	private boolean isSubscribedToWriteEvent= false;
	private final int bufferSizeInBytes;
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
		this.byteBufferedChannelManager = byteBufferedChannelManager;
		this.connectionAddress = connectionAddress;
		this.connectionThread = connectionThread;
		this.numberOfConnectionRetries = numberOfConnectionRetries;
		this.readableByteChannel = fileInputChannel;
		final Configuration configuration = GlobalConfiguration.getConfiguration();
		this.bufferSizeInBytes = configuration.getInteger("channel.network.bufferSizeInBytes",
			DEFAULT_BUFFER_SIZE_IN_BYTES);
		
		this.retriesLeft = this.numberOfConnectionRetries;
		this.timstampOfLastRetry = System.currentTimeMillis();
		this.connectionThread.triggerConnect(this);
		this.isConnected = true;
		this.isSubscribedToWriteEvent = true;
		this.connectionThread.run();
		
		
	}
	public boolean write() throws IOException {

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
		final WritableByteChannel writableByteChannel = (WritableByteChannel) this.selectionKey.channel();
		
		LOG.info("position" + this.readableByteChannel.position() + " transfered " + this.eof);
		LOG.info(this.readableByteChannel.size());
		//FileLock lock = this.readableByteChannel.lock();
	//	this.eof = this.readableByteChannel.read(this.tempBuffer,this.bytesread);
		this.eof = this.readableByteChannel.transferTo(this.bytesread,this.readableByteChannel.size(),writableByteChannel );
		//lock.release();
//		try{
//		int written = writableByteChannel.write(this.tempBuffer);
//		this.tempBuffer.clear();
//		
//		
//		}catch(NonWritableChannelException e){
//			
//		}
		LOG.info("position" + this.readableByteChannel.position() + " transfered " + this.eof + " ALL " + this.bytesread);
		this.bytesread += this.eof;
		
		return true;
	}
	
	
	/**
	 * Sets the selection key representing the interest set of the underlying TCP NIO connection.
	 * 
	 * @param selectionKey
	 *        the selection of the underlying TCP connection
	 */
	public void setSelectionKey(SelectionKey selectionKey) {
		this.selectionKey = selectionKey;
	}
}
