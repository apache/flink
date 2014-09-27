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

import org.apache.flink.runtime.io.network.Buffer;

/**
 * This interface must be implemented to receive an asynchronous callback from
 * a {@link BufferProvider} as soon as a buffer has become available again.
 */
public interface BufferAvailabilityListener {

	/**
	 * Returns a Buffer to the listener.
	 * <p/>
	 * Note: the listener has to adjust the size of the returned Buffer to the
	 * requested size manually via {@link Buffer#limitSize(int)}.
	 */
	void bufferAvailable(Buffer buffer) throws Exception;
}
