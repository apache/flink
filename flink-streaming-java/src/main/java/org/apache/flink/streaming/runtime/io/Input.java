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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Basic interface for inputs of stream operators.
 */
@Internal
public interface Input extends AutoCloseable {

	int getInputIndex();

	/**
	 * Poll the next {@link StreamElement}.
	 *
	 * @return null if there is no data to return or if {@link #isFinished()} returns true.
	 *
	 * @throws IOException  Thrown if the network or local disk I/O fails.
	 * @throws InterruptedException Thrown if the thread is interrupted.
	 */
	StreamElement pollNextElement() throws IOException, InterruptedException;

	/**
	 * Tests if it reaches the end of the input.
	 *
	 * @return true if was reached, false otherwise.
	 */
	boolean isFinished();

	/**
	 * Register a listener that forward input notifications to.
	 */
	CompletableFuture<?> listen();
}
