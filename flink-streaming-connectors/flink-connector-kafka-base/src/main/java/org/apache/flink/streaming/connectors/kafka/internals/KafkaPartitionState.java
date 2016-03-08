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

package org.apache.flink.streaming.connectors.kafka.internals;

import java.io.Serializable;

public class KafkaPartitionState implements Serializable {

	private static final long serialVersionUID = 722083576322742328L;

	private final int partitionID;
	private long offset;

	private long maxTimestamp = Long.MIN_VALUE;
	private boolean isActive = false;

	public KafkaPartitionState(int id, long offset) {
		this.partitionID = id;
		this.offset = offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public void setActive(boolean isActive) {
		this.isActive = isActive;
	}

	public void setMaxTimestamp(long timestamp) {
		maxTimestamp = timestamp;
	}

	public int getPartition() {
		return partitionID;
	}

	public boolean isActive() {
		return isActive;
	}

	public long getMaxTimestamp() {
		return maxTimestamp;
	}

	public long getOffset() {
		return offset;
	}

}
