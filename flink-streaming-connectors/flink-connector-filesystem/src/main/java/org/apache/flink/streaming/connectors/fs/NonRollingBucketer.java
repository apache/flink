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

/**
 * A {@link org.apache.flink.streaming.connectors.fs.Bucketer} that does not perform any
 * rolling of files. All files are written to the base path.
 */
public class NonRollingBucketer implements Bucketer {
	private static final long serialVersionUID = 1L;

	@Override
	public boolean shouldStartNewBucket(Path basePath, Path currentBucketPath) {
		return false;
	}

	@Override
	public Path getNextBucketPath(Path basePath) {
		return basePath;
	}

	@Override
	public String toString() {
		return "NonRollingBucketer";
	}
}
