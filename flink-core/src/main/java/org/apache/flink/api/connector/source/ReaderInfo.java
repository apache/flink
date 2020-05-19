/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.io.Serializable;
import java.util.Objects;

/**
 * A container class hosting the information of a {@link SourceReader}.
 */
@Public
public final class ReaderInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private final int subtaskId;
	private final String location;

	public ReaderInfo(int subtaskId, String location) {
		this.subtaskId = subtaskId;
		this.location = location;
	}

	/**
	 * @return the ID of the subtask that runs the source reader.
	 */
	public int getSubtaskId() {
		return subtaskId;
	}

	/**
	 * @return the location of the subtask that runs this source reader.
	 */
	public String getLocation() {
		return location;
	}

	@Override
	public int hashCode() {
		return Objects.hash(subtaskId, location);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof ReaderInfo)) {
			return false;
		}
		ReaderInfo other = (ReaderInfo) obj;
		return subtaskId == other.subtaskId && location.equals(other.location);
	}
}
