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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.BlockLocation;

import java.io.IOException;

/**
 * Implementation of the {@link BlockLocation} interface for a local file system.
 */
@Internal
public class LocalBlockLocation implements BlockLocation {

	private final long length;

	private final String[] hosts;

	public LocalBlockLocation(final String host, final long length) {
		this.hosts = new String[] { host };
		this.length = length;
	}

	@Override
	public String[] getHosts() throws IOException {
		return this.hosts;
	}

	@Override
	public long getLength() {
		return this.length;
	}

	@Override
	public long getOffset() {
		return 0;
	}

	@Override
	public int compareTo(final BlockLocation o) {
		return 0;
	}
}
