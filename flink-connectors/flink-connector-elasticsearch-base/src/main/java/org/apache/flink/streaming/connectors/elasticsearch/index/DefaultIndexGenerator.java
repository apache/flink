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

package org.apache.flink.streaming.connectors.elasticsearch.index;

import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * Default IndexGenerator implements.
 */
public class DefaultIndexGenerator implements IndexGenerator {

	private static final long serialVersionUID = 1L;
	private final String index;

	public DefaultIndexGenerator(String index) {
		this.index = index;
	}

	/**
	 * Generate index for each row dynamically, return the index value by default.
	 */
	public String generate(Row row) {
		return index;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof DefaultIndexGenerator)) {
			return false;
		}
		DefaultIndexGenerator that = (DefaultIndexGenerator) o;
		return Objects.equals(index, that.index);
	}

	@Override
	public int hashCode() {
		return Objects.hash(index);
	}
}
