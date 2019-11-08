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

/**
 * A {@link Bucketer} that does not perform any
 * bucketing of files. All files are written to the base path.
 */
@Deprecated
public class BasePathBucketer<T> implements Bucketer<T> {
	private static final long serialVersionUID = 1L;

	@Override
	public Path getBucketPath(Clock clock, Path basePath, T element) {
		return basePath;
	}

	@Override
	public String toString() {
		return "BasePathBucketer";
	}
}
