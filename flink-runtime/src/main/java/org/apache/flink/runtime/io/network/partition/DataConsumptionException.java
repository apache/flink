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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This exception is used for the process of network shuffle in blocking data exchange mode.
 * The exception indicates an error which cannot be recovered by the effort of consumers.
 * The consumer should send this exception to job master to restart the producer for re-producing the data.
 */
public class DataConsumptionException extends IOException {

	private static final long serialVersionUID = -1555492656299526396L;

	/** The result partition id to indicate the consumed result partition. **/
	private final ResultPartitionID resultPartitionId;

	public DataConsumptionException(ResultPartitionID resultPartitionId, Throwable cause) {
		super(cause);

		this.resultPartitionId = checkNotNull(resultPartitionId);
	}

	public ResultPartitionID getResultPartitionId() {
		return resultPartitionId;
	}
}
