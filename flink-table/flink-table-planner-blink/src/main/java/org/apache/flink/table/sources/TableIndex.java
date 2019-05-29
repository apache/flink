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

package org.apache.flink.table.sources;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An Index meta information of a Table.
 */
public class TableIndex {

	/**
	 * Index type, currently only support NORMAL INDEX, and UNIQUE INDEX.
	 */
	public enum IndexType {
		NORMAL,
		UNIQUE
	}

	private final String indexName;
	private final IndexType indexType;
	private final List<String> indexedColumns;
	private final String indexComment;

	private TableIndex(String indexName, IndexType indexType, List<String> indexedColumns, String indexComment) {
		this.indexName = indexName;
		this.indexType = indexType;
		this.indexedColumns = indexedColumns;
		this.indexComment = indexComment;
	}

	/**
	 * Returns name of the Index.
	 *
	 * @return an optional name of the index.
	 */
	public Optional<String> getIndexName() {
		return Optional.ofNullable(indexName);
	}

	/**
	 * Returns the column names of the index.
	 */
	public List<String> getIndexedColumns() {
		return indexedColumns;
	}

	/**
	 * Returns the type of the index.
	 */
	public IndexType getIndexType() {
		return indexType;
	}

	/**
	 * Returns comment of the index.
	 * @return an optional comment of the index.
	 */
	public Optional<String> getIndexComment() {
		return Optional.ofNullable(indexComment);
	}

	/**
	 * Returns a new builder that builds a {@link TableIndex}.
	 *
	 * <p>For example:
	 * <pre>
	 *     TableIndex.builder()
	 *       .uniqueIndex()
	 *       .indexedColumns("user_id", "user_name")
	 *       .name("idx_1")
	 *       .build();
	 * </pre>
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * A builder used to construct a {@link TableIndex}.
	 */
	public static class Builder {
		private String indexName;
		private IndexType indexType;
		private List<String> indexedColumns;
		private String indexComment;

		public Builder normalIndex() {
			checkState(indexType == null, "IndexType has been set.");
			this.indexType = IndexType.NORMAL;
			return this;
		}

		public Builder uniqueIndex() {
			checkState(indexType == null, "IndexType has been set.");
			this.indexType = IndexType.UNIQUE;
			return this;
		}

		public Builder name(String name) {
			this.indexName = name;
			return this;
		}

		public Builder indexedColumns(List<String> indexedColumns) {
			this.indexedColumns = indexedColumns;
			return this;
		}

		public Builder indexedColumns(String... indexedColumns) {
			this.indexedColumns = Arrays.asList(indexedColumns);
			return this;
		}

		public Builder comment(String comment) {
			this.indexComment = comment;
			return this;
		}

		public TableIndex build() {
			checkNotNull(indexedColumns);
			checkNotNull(indexType);
			return new TableIndex(indexName, indexType, indexedColumns, indexComment);
		}

	}

}
