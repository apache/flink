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

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;

/**
 * InputFormat to read data from Apache Cassandra and generate a custom Cassandra annotated object.
 *
 * @param <OUT> type of inputClass
 */
public class CassandraPojoInputFormat<OUT> extends CassandraInputFormatBase<OUT> {

	private static final long serialVersionUID = 1992091320180905115L;

	private transient Result<OUT> resultSet;
	private final Class<OUT> inputClass;

	public CassandraPojoInputFormat(String query, ClusterBuilder builder, Class<OUT> inputClass) {
		super(query, builder);

		Preconditions.checkArgument(inputClass != null, "InputClass cannot be null");

		this.inputClass = inputClass;
	}

	@Override
	public void open(InputSplit split) {
		this.session = cluster.connect();
		MappingManager manager = new MappingManager(session);

		Mapper<OUT> mapper = manager.mapper(inputClass);

		this.resultSet = mapper.map(session.execute(query));
	}

	@Override
	public boolean reachedEnd() {
		return resultSet.isExhausted();
	}

	@Override
	public OUT nextRecord(OUT reuse) {
		return resultSet.one();
	}
}
