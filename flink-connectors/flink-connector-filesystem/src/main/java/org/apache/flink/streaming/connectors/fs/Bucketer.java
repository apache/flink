/**
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

package org.apache.flink.streaming.connectors.fs;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;

/**
 * A bucketer is used with a {@link RollingSink}
 * to put emitted elements into rolling files.
 *
 *
 * <p>The {@code RollingSink} has one active bucket that it is writing to at a time. Whenever
 * a new element arrives it will ask the {@code Bucketer} if a new bucket should be started and
 * the old one closed. The {@code Bucketer} can, for example, decide to start new buckets
 * based on system time.
 *
 * @deprecated use {@link org.apache.flink.streaming.connectors.fs.bucketing.Bucketer} instead.
 */
@Deprecated
public interface Bucketer extends Serializable {

	/**
	 * Returns {@code true} when a new bucket should be started.
	 *
	 * @param currentBucketPath The bucket {@code Path} that is currently being used.
	 */
	boolean shouldStartNewBucket(Path basePath, Path currentBucketPath);

	/**
	 * Returns the {@link Path} of a new bucket file.
	 *
	 * @param basePath The base path containing all the buckets.
	 *
	 * @return The complete new {@code Path} of the new bucket. This should include the {@code basePath}
	 *      and also the {@code subtaskIndex} tp avoid clashes with parallel sinks.
	 */
	Path getNextBucketPath(Path basePath);
}
