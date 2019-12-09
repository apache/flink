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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.util.event.EventListener;

/**
 * The task event publisher is used for publishing the event to the registered
 * {@link EventListener} instances.
 */
public interface TaskEventPublisher {

	/**
	 * Publishes the event to the registered {@link EventListener} instances.
	 *
	 * @param partitionId the partition ID to get registered handlers
	 * @param event the task event to be published to the handlers
	 * @return whether the event was published to a registered event handler or not
	 */
	boolean publish(ResultPartitionID partitionId, TaskEvent event);
}
