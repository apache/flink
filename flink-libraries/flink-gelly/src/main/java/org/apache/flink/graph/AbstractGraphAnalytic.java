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

package org.apache.flink.graph;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;

/**
 * Base class for {@link GraphAnalytic}.
 *
 * @param <K> key type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <T> the return type
 */
public abstract class AbstractGraphAnalytic<K, VV, EV, T>
implements GraphAnalytic<K, VV, EV, T> {

	protected ExecutionEnvironment env;

	@Override
	public GraphAnalytic<K, VV, EV, T> run(Graph<K, VV, EV> input)
			throws Exception {
		env = input.getContext();
		return this;
	}

	@Override
	public T execute()
			throws Exception {
		env.execute();
		return getResult();
	}

	@Override
	public T execute(String jobName)
			throws Exception {
		Preconditions.checkNotNull(jobName);

		env.execute(jobName);
		return getResult();
	}
}
