/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.batch.connectors.cassandra;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 * Example of Cassandra Annotated POJO class for use with {@link CassandraPojoInputFormat}.
 */
@Table(name = CustomCassandraAnnotatedPojo.TABLE_NAME, keyspace = "flink")
public class CustomCassandraAnnotatedPojo {

	public static final String TABLE_NAME = "batches";

	@Column(name = "id")
	private String id;
	@Column(name = "counter")
	private Integer counter;
	@Column(name = "batch_id")
	private Integer batchId;

	/**
	 * Necessary for the driver's mapper instanciation.
	 */
	public CustomCassandraAnnotatedPojo(){}

	public CustomCassandraAnnotatedPojo(String id, Integer counter, Integer batchId) {
		this.id = id;
		this.counter = counter;
		this.batchId = batchId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Integer getCounter() {
		return counter;
	}

	public void setCounter(Integer counter) {
		this.counter = counter;
	}

	public Integer getBatchId() {
		return batchId;
	}

	public void setBatchId(Integer batchId) {
		this.batchId = batchId;
	}
}
