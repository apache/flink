/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.rpc;

import java.util.List;

import eu.stratosphere.nephele.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;

/**
 * This utility class provides a list of types frequently used by the RPC protocols included in this package.
 * 
 * @author warneke
 */
public class ProfilingTypeUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ProfilingTypeUtils() {
	}

	/**
	 * Returns a list of types frequently used by the RPC protocols of this package and its parent packages.
	 * 
	 * @return a list of types frequently used by the RPC protocols of this package
	 */
	public static List<Class<?>> getRPCTypesToRegister() {

		final List<Class<?>> types = ServerTypeUtils.getRPCTypesToRegister();

		types.add(InternalExecutionVertexThreadProfilingData.class);
		types.add(InternalInstanceProfilingData.class);

		return types;
	}
}
