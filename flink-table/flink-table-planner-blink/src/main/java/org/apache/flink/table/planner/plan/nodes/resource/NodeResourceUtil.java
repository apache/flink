/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.resource;

import org.apache.flink.api.common.operators.ResourceSpec;

/**
 * Deal with resource config for {@link org.apache.flink.table.planner.plan.nodes.exec.ExecNode}.
 */
public class NodeResourceUtil {

	/**
	 * How many Bytes per MB.
	 */
	public static final long SIZE_IN_MB =  1024L * 1024;

	/**
	 * Build resourceSpec from managedMem.
	 */
	public static ResourceSpec fromManagedMem(int managedMem) {
		ResourceSpec.Builder builder = ResourceSpec.newBuilder();
		builder.setManagedMemoryInMB(managedMem);
		return builder.build();
	}
}
