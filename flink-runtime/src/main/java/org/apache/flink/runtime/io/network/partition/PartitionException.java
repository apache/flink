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

package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;

/**
 * Exception for covering all the scenarios of consuming partition failure
 * which causes the consumer task failed, and the job master would decide
 * whether to restart the producer based on this exception.
 */
public abstract class PartitionException extends IOException {

	private static final long serialVersionUID = 0L;

	private final ResultPartitionID partitionId;

	public PartitionException(String message, ResultPartitionID partitionId) {
		this(message, partitionId, null);
	}

	public PartitionException(String message, ResultPartitionID partitionId, Throwable throwable) {
		super(message, throwable);

		this.partitionId = partitionId;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}
}
