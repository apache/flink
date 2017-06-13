/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.util.NonClosingInputStreamDecorator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class provides access to input streams that contain data of one state partition of a partitionable state.
 *
 * TODO use bounded stream that fail fast if the limit is exceeded on corrupted reads.
 */
@PublicEvolving
public class StatePartitionStreamProvider {

	/** A ready-made stream that contains data for one state partition */
	private final InputStream stream;

	/** Holds potential exception that happened when actually trying to create the stream */
	private final IOException creationException;

	public StatePartitionStreamProvider(IOException creationException) {
		this.creationException = Preconditions.checkNotNull(creationException);
		this.stream = null;
	}

	public StatePartitionStreamProvider(InputStream stream) {
		this.stream = new NonClosingInputStreamDecorator(Preconditions.checkNotNull(stream));
		this.creationException = null;
	}


	/**
	 * Returns a stream with the data of one state partition.
	 */
	public InputStream getStream() throws IOException {
		if (creationException != null) {
			throw new IOException(creationException);
		}
		return stream;
	}
}
