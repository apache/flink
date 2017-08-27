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

package org.apache.flink.streaming.connectors.fs.consistent;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;

/**
 * The eventual consistency bucketer is used with a {@code EventuallyConsistentBucketingSink }
 * to generate a unique path for each checkpoint based on the timestamp that the
 * checkpoint was initiated by the job manager.
 *
 * <p>It is important that if {@code EventuallyConsistentBucketer#getEventualConsistencyPath}
 * is called twice with the same checkpoint id but different timestamps that different
 * final paths are returned. Otherwise, exactly once cannot be guaranteed.
 */
public interface EventuallyConsistentBucketer extends Serializable {
	Path getEventualConsistencyPath(Path basePath, long checkpointId, long timestamp);
}
