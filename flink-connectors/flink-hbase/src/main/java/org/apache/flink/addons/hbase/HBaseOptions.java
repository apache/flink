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

package org.apache.flink.addons.hbase;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common Options for HBase.
 */
public class HBaseOptions {

	private final String tableName;
	private final String zkQuorum;
	@Nullable private final String zkNodeParent;

	private HBaseOptions(String tableName, String zkQuorum, @Nullable String zkNodeParent) {
		this.tableName = tableName;
		this.zkQuorum = zkQuorum;
		this.zkNodeParent = zkNodeParent;
	}

	String getTableName() {
		return tableName;
	}

	String getZkQuorum() {
		return zkQuorum;
	}

	Optional<String> getZkNodeParent() {
		return Optional.ofNullable(zkNodeParent);
	}

	@Override
	public String toString() {
		return "HBaseOptions{" +
			"tableName='" + tableName + '\'' +
			", zkQuorum='" + zkQuorum + '\'' +
			", zkNodeParent='" + zkNodeParent + '\'' +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HBaseOptions that = (HBaseOptions) o;
		return Objects.equals(tableName, that.tableName) &&
			Objects.equals(zkQuorum, that.zkQuorum) &&
			Objects.equals(zkNodeParent, that.zkNodeParent);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableName, zkQuorum, zkNodeParent);
	}

	/**
	 * Creates a builder of {@link HBaseOptions}.
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link HBaseOptions}.
	 */
	public static class Builder {

		private String tableName;
		private String zkQuorum;
		private String zkNodeParent;

		/**
		 * Required. Sets the HBase table name.
		 */
		public Builder setTableName(String tableName) {
			checkNotNull(tableName);
			this.tableName = tableName;
			return this;
		}

		/**
		 * Required. Sets the HBase ZooKeeper quorum configuration.
		 */
		public Builder setZkQuorum(String zkQuorum) {
			checkNotNull(zkQuorum);
			this.zkQuorum = zkQuorum;
			return this;
		}

		/**
		 * Optional. Sets the root dir in ZK for the HBase cluster. Default is "/hbase".
		 */
		public Builder setZkNodeParent(String zkNodeParent) {
			checkNotNull(zkNodeParent);
			this.zkNodeParent = zkNodeParent;
			return this;
		}

		/**
		 * Creates an instance of {@link HBaseOptions}.
		 */
		public HBaseOptions build() {
			checkNotNull(zkQuorum, "Zookeeper quorum is not set.");
			checkNotNull(tableName, "TableName is not set.");
			return new HBaseOptions(tableName, zkQuorum, zkNodeParent);
		}
	}
}
