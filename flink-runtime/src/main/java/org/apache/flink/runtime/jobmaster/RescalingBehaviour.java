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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.BiConsumerWithException;

/**
 * Definition of the rescaling behaviour.
 */
public enum RescalingBehaviour implements BiConsumerWithException<JobVertex, Integer, FlinkException> {
	// rescaling is only executed if the operator can be set to the given parallelism
	STRICT {
		@Override
		public void acceptWithException(JobVertex jobVertex, Integer newParallelism) throws FlinkException {
			if (jobVertex.getMaxParallelism() < newParallelism) {
				throw new FlinkException("Cannot rescale vertex " + jobVertex.getName() +
					" because its maximum parallelism " + jobVertex.getMaxParallelism() +
					" is smaller than the new parallelism " + newParallelism + '.');
			} else {
				jobVertex.setParallelism(newParallelism);
			}
		}
	},
	// the new parallelism will be the minimum of the given parallelism and the maximum parallelism
	RELAXED {
		@Override
		public void acceptWithException(JobVertex jobVertex, Integer newParallelism) {
			jobVertex.setParallelism(Math.min(jobVertex.getMaxParallelism(), newParallelism));
		}
	}
}
