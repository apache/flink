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

package org.apache.flink.runtime.io.disk.iomanager;

import java.io.IOException;

/**
 * Basic interface that I/O requests that are sent to the threads of the I/O manager need to implement.
 */
interface IORequest {
	
	/**
	 * Method that is called by the target I/O thread after the request has been processed.
	 * 
	 * @param ioex The exception that occurred while processing the I/O request. Is <tt>null</tt> if everything was fine.
	 */
	public void requestDone(IOException ioex);
}

/**
 * Interface for I/O requests that are handled by the IOManager's reading thread. 
 */
interface ReadRequest extends IORequest {
	
	/**
	 * Called by the target I/O thread to perform the actual reading operation.
	 * 
	 * @throws IOException My be thrown by the method to indicate an I/O problem.
	 */
	public void read() throws IOException;
}

/**
 * Interface for I/O requests that are handled by the IOManager's writing thread.
 */
interface WriteRequest extends IORequest {
	
	/**
	 * Called by the target I/O thread to perform the actual writing operation.
	 * 
	 * @throws IOException My be thrown by the method to indicate an I/O problem.
	 */
	public void write() throws IOException;
}
