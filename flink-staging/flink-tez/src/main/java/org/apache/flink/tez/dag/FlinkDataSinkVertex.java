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

package org.apache.flink.tez.dag;


import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.tez.runtime.DataSinkProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;

import java.io.IOException;

public class FlinkDataSinkVertex extends FlinkVertex {

	public FlinkDataSinkVertex(String taskName, int parallelism, TezTaskConfig taskConfig) {
		super(taskName, parallelism, taskConfig);
	}

	@Override
	public Vertex createVertex(TezConfiguration conf) {
		try {
			this.writeInputPositionsToConfig();
			this.writeSubTasksInOutputToConfig();

			conf.set(TezTaskConfig.TEZ_TASK_CONFIG, EncodingUtils.encodeObjectToString(taskConfig));

			ProcessorDescriptor descriptor = ProcessorDescriptor.create(
					DataSinkProcessor.class.getName());

			descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

			cached = Vertex.create(this.getUniqueName(), descriptor, getParallelism());

			return cached;
		}
		catch (IOException e) {
			throw new CompilerException(
					"An error occurred while creating a Tez Vertex: " + e.getMessage(), e);
		}
	}
}
