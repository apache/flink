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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

public abstract class CheckpointMetadataOutputStream extends FSDataOutputStream {

	public abstract CompletedCheckpointStorageLocation closeAndFinalizeCheckpoint() throws IOException;

	/**
	 * This method should close the stream, if has not been closed before.
	 * If this method actually closes the stream, it should delete/release the
	 * resource behind the stream, such as the file that the stream writes to.
	 *
	 * <p>The above implies that this method is intended to be the "unsuccessful close",
	 * such as when cancelling the stream writing, or when an exception occurs.
	 * Closing the stream for the successful case must go through {@link #closeAndFinalizeCheckpoint()}.
	 *
	 * @throws IOException Thrown, if the stream cannot be closed.
	 */
	@Override
	public abstract void close() throws IOException;
}
