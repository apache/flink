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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class describes the property of the {@link BucketWriter}.
 */
@Internal
public class WriterProperties {

	private final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverableSerializer;

	private final SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableSerializer;

	private final boolean supportsResume;

	WriterProperties(
			SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverableSerializer,
			SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableSerializer,
			boolean supportsResume) {
		this.inProgressFileRecoverableSerializer = checkNotNull(inProgressFileRecoverableSerializer);
		this.pendingFileRecoverableSerializer = checkNotNull(pendingFileRecoverableSerializer);
		this.supportsResume = supportsResume;
	}

	/**
	 * @return Whether the {@link BucketWriter} support appending data to the restored the in-progress file or not.
	 */
	boolean supportsResume() {
		return supportsResume;
	}

	/**
	 * @return the serializer for the {@link InProgressFileWriter.PendingFileRecoverable}.
	 */
	SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> getPendingFileRecoverableSerializer() {
		return pendingFileRecoverableSerializer;
	}

	/**
	 * @return the serializer for the {@link InProgressFileWriter.InProgressFileRecoverable}.
	 */
	SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> getInProgressFileRecoverableSerializer() {
		return inProgressFileRecoverableSerializer;
	}
}
