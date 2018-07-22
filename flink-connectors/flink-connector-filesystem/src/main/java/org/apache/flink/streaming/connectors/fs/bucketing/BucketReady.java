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

package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;
import java.util.Set;

/**
 * A mechanism that determine whether the bucket is ready for user use. Bucket ready means all the part
 * files suffix name under a bucket neither .pending nor .in-progress.
 */
public interface BucketReady extends Serializable {

	/**
	 * To determine if the bucket is ready at the moment.
	 *
	 * @param path The path for a specific bucket.
	 *
	 * @return True if the bucket is ready for use.
	 */
	boolean isBucketReady(Set<Path> path);

}
