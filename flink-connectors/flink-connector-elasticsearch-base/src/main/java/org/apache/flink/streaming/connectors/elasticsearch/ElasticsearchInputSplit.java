/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.LocatableInputSplit;

import java.util.Objects;

/**
 * This class implements a input splits for Elasticsearch, which doesn't contain sclie info.
 * You can reference https://github.com/elastic/elasticsearch-hadoop/blob/master/docs/src/reference/asciidoc/appendix/breaking.adoc
 * and find this words "Disabling sliced scrolls by default, and switching them to be an explicitly opt-in feature."
 * Each elasticsearch input split corresponds to a shard.
 */
@Internal
public class ElasticsearchInputSplit extends LocatableInputSplit {

	private static final long serialVersionUID = 1L;

	/** The name of the index to retrieve data from. */
	private final String index;

	/** It is null in flink elasticsearch connector 7+. */
	private final String type;

	/** Index will split diffirent shards when index created. */
	private final int shard;

	public ElasticsearchInputSplit(int splitNumber, String[] hostnames, String index, String type, int shard) {
		super(splitNumber, hostnames);
		this.index = index;
		this.type = type;
		this.shard = shard;
	}

	public String getIndex() {
		return index;
	}

	public String getType() {
		return type;
	}

	public int getShard() {
		return shard;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof ElasticsearchInputSplit)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		ElasticsearchInputSplit split = (ElasticsearchInputSplit) o;
		return getShard() == split.getShard() &&
			Objects.equals(getIndex(), split.getIndex()) &&
			Objects.equals(getType(), split.getType());
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getIndex(), getType(), getShard());
	}

	@Override
	public String toString() {
		return "ElasticsearchInputSplit{" +
			"index='" + index + '\'' +
			", type='" + type + '\'' +
			", shard=" + shard +
			'}';
	}
}
