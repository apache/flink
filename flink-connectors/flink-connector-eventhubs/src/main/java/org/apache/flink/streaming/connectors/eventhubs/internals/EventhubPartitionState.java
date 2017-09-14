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

package org.apache.flink.streaming.connectors.eventhubs.internals;

/**
 * Created by jozh on 5/23/2017.
 * Flink eventhub connnector has implemented with same design of flink kafka connector
 */

public class EventhubPartitionState {
	private final EventhubPartition partition;
	private volatile String offset;

	public EventhubPartitionState(EventhubPartition partition, String offset){
		this.partition = partition;
		this.offset = offset;
	}

	public final String getOffset() {
		return  this.offset;
	}

	public final void  setOffset(String offset) {
		this.offset = offset;
	}

	public EventhubPartition getPartition() {
		return this.partition;
	}
}

