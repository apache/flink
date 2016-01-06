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
import org.apache.flink.tez.runtime.DataSourceProcessor;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.tez.runtime.input.FlinkInput;
import org.apache.flink.tez.runtime.input.FlinkInputSplitGenerator;
import org.apache.flink.tez.util.EncodingUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;

import java.io.IOException;

public class FlinkDataSourceVertex extends FlinkVertex {

	public FlinkDataSourceVertex(String taskName, int parallelism, TezTaskConfig taskConfig) {
		super(taskName, parallelism, taskConfig);
	}


	@Override
	public Vertex createVertex (TezConfiguration conf) {
		try {
			this.writeInputPositionsToConfig();
			this.writeSubTasksInOutputToConfig();

			taskConfig.setDatasourceProcessorName(this.getUniqueName());
			conf.set(TezTaskConfig.TEZ_TASK_CONFIG, EncodingUtils.encodeObjectToString(taskConfig));

			ProcessorDescriptor descriptor = ProcessorDescriptor.create(
					DataSourceProcessor.class.getName());

			descriptor.setUserPayload(TezUtils.createUserPayloadFromConf(conf));

			InputDescriptor inputDescriptor = InputDescriptor.create(FlinkInput.class.getName());

			InputInitializerDescriptor inputInitializerDescriptor =
					InputInitializerDescriptor.create(FlinkInputSplitGenerator.class.getName()).setUserPayload(TezUtils.createUserPayloadFromConf(conf));

			DataSourceDescriptor dataSourceDescriptor = DataSourceDescriptor.create(
					inputDescriptor,
					inputInitializerDescriptor,
					new Credentials()
			);

			cached = Vertex.create(this.getUniqueName(), descriptor, getParallelism());

			cached.addDataSource("Input " + this.getUniqueName(), dataSourceDescriptor);

			return cached;
		}
		catch (IOException e) {
			throw new CompilerException(
					"An error occurred while creating a Tez Vertex: " + e.getMessage(), e);
		}
	}
}
