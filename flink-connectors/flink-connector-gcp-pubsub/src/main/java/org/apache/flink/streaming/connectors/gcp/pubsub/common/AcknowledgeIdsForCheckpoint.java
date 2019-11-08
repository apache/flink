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

package org.apache.flink.streaming.connectors.gcp.pubsub.common;

import java.io.Serializable;
import java.util.List;

/**
 * This class contains a checkpointId and a List of AcknowledgementIds.
 * This class is used by {@link AcknowledgeOnCheckpoint} to keep track of acknowledgementIds
 * @param <AcknowledgeId> Type of the Ids used for acknowledging.
 */
public class AcknowledgeIdsForCheckpoint<AcknowledgeId> implements Serializable {
	private long checkpointId;
	private List<AcknowledgeId> acknowledgeIds;

	AcknowledgeIdsForCheckpoint(long checkpointId, List<AcknowledgeId> acknowledgeIds) {
		this.checkpointId = checkpointId;
		this.acknowledgeIds = acknowledgeIds;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public void setCheckpointId(long checkpointId) {
		this.checkpointId = checkpointId;
	}

	public List<AcknowledgeId> getAcknowledgeIds() {
		return acknowledgeIds;
	}

	public void setAcknowledgeIds(List<AcknowledgeId> acknowledgeIds) {
		this.acknowledgeIds = acknowledgeIds;
	}
}
