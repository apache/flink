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

package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.configuration.Configuration;

/**
 * Flink Sink to save data into a Cassandra cluster using {@link Mapper}, which
 * it uses annotations from {@link com.datastax.driver.mapping}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public class CassandraPojoAtLeastOnceSink<IN> extends CassandraAtLeastOnceSink<IN, Void> {
	protected Class<IN> clazz;
	protected transient Mapper<IN> mapper;
	protected transient MappingManager mappingManager;

	/**
	 * The main constructor for creating CassandraPojoAtLeastOnceSink
	 *
	 * @param clazz Class<IN> instance
	 */
	public CassandraPojoAtLeastOnceSink(Class<IN> clazz, ClusterBuilder builder) {
		super(builder);
		this.clazz = clazz;
	}

	@Override
	public void open(Configuration configuration) {
		super.open(configuration);
		try {
			this.mappingManager = new MappingManager(session);
			this.mapper = mappingManager.mapper(clazz);
		} catch (Exception e) {
			throw new RuntimeException("Cannot create CassandraPojoAtLeastOnceSink with input: " + clazz.getSimpleName(), e);
		}
	}

	@Override
	public ListenableFuture<Void> send(IN value) {
		return mapper.saveAsync(value);
	}
}
