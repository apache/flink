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


package org.apache.flink.runtime.io.network.bufferprovider;

/**
 * A local buffer pool owner is an object which initially retrieves its buffers from the {@link GlobalBufferPool} and
 * manages its fraction of the overall buffer pool locally by means of a {@link LocalBufferPool}.
 * 
 */
public interface LocalBufferPoolOwner {

	/**
	 * Returns the number of byte-buffered channels that will retrieve their buffers from the local buffer pool.
	 * 
	 * @return the number of byte-buffered channels that will retrieve their buffers from the local buffer pool
	 */
	int getNumberOfChannels();

	/**
	 * Sets the designated number of buffers the local buffer pool owner is allowed to fetch from the global buffer pool
	 * and manage locally by means of the {@link LocalBufferPool}.
	 * 
	 * @param numBuffers
	 *        the numBuffers the local buffer pool owner is allowed to fetch from the global buffer pool
	 */
	void setDesignatedNumberOfBuffers(int numBuffers);

	/**
	 * Clears the local buffer pool and returns all buffers to the global buffer pool.
	 */
	void clearLocalBufferPool();

	void registerGlobalBufferPool(GlobalBufferPool globalBufferPool);

	/**
	 * Logs the current status of the local buffer pool. This method is intended mainly for debugging purposes.
	 */
	void logBufferUtilization();

	/**
	 * Reports an asynchronous event. Calling this method interrupts each blocking method of the buffer pool owner and
	 * allows the blocked thread to respond to the event.
	 */
	void reportAsynchronousEvent();
}
