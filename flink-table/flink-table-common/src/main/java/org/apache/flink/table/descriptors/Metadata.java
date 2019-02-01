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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Map;

import static org.apache.flink.table.descriptors.MetadataValidator.METADATA_COMMENT;
import static org.apache.flink.table.descriptors.MetadataValidator.METADATA_CREATION_TIME;
import static org.apache.flink.table.descriptors.MetadataValidator.METADATA_LAST_ACCESS_TIME;

/**
 * Metadata descriptor for adding additional, useful information.
 */
@PublicEvolving
public class Metadata implements Descriptor {

	private String comment;
	private Long creationTime;
	private Long lastAccessTime;

	public Metadata() {}

	/**
	 * Sets a comment.
	 *
	 * @param comment the description
	 */
	public Metadata comment(String comment) {
		this.comment = comment;
		return this;
	}

	/**
	 * Sets a creation time.
	 *
	 * @param time UTC milliseconds timestamp
	 */
	public Metadata creationTime(Long time) {
		this.creationTime = time;
		return this;
	}

	/**
	 * Sets a last access time.
	 *
	 * @param time UTC milliseconds timestamp
	 */
	public Metadata lastAccessTime(Long time) {
		this.lastAccessTime = time;
		return this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public final Map<String, String> toProperties() {
		DescriptorProperties properties = new DescriptorProperties();
		if (comment != null) {
			properties.putString(METADATA_COMMENT, comment);
		}
		if (creationTime != null) {
			properties.putLong(METADATA_CREATION_TIME, creationTime);
		}
		if (lastAccessTime != null) {
			properties.putLong(METADATA_LAST_ACCESS_TIME, lastAccessTime);
		}
		return properties.asMap();
	}
}
