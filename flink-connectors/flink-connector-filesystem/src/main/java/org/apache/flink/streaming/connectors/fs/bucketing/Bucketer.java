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

import org.apache.flink.streaming.connectors.fs.Clock;

import org.apache.hadoop.fs.Path;

import java.io.Serializable;

/**
 * A bucketer is used with a {@link BucketingSink}
 * to put emitted elements into rolling files.
 *
 *
 * <p>The {@code BucketingSink} can be writing to many buckets at a time, and it is responsible for managing
 * a set of active buckets. Whenever a new element arrives it will ask the {@code Bucketer} for the bucket
 * path the element should fall in. The {@code Bucketer} can, for example, determine buckets based on
 * system time.
 */
public interface Bucketer<T> extends Serializable {
	/**
	 * Returns the {@link Path} of a bucket file.
	 *
	 * @param basePath The base path containing all the buckets.
	 * @param element The current element being processed.
	 *
	 * @return The complete {@code Path} of the bucket which the provided element should fall in. This
	 * should include the {@code basePath} and also the {@code subtaskIndex} to avoid clashes with
	 * parallel sinks.
	 */
	Path getBucketPath(Clock clock, Path basePath, T element);
}
