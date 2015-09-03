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

package org.apache.flink.parameterserver.impl.ignite.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.parameterserver.impl.ignite.ParameterServerIgniteImpl;
import org.apache.flink.parameterserver.model.ParameterElement;
import org.apache.flink.parameterserver.model.ParameterServerClient;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines a map function with access to a parameter server.
 * The parameter server has two cache levels:
 * The partitioned parameter cache: stores parameter subject to writes at each iteration.
 * 			Furthermore, the new parameter is written only if associated with
 * 			a greater convergence degree.
 * The shared parameter cache: suitable for parameters more often read than written.
 *
 */
public abstract class RichMapFunctionWithParameterServer<IN, OUT>
		extends RichMapFunction<IN, OUT>
		implements ParameterServerClient {

	private static final Logger log =
			LoggerFactory.getLogger(RichMapFunctionWithParameterServer.class);

	private IgniteCache<String, ParameterElement> partitionedParameterCache = null;
	private IgniteCache<String, ParameterElement> sharedParameterCache = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		if (partitionedParameterCache == null || sharedParameterCache == null ) {
			ParameterServerIgniteImpl.prepareInstance(ParameterServerIgniteImpl.GRID_NAME);
			Ignite ignite = Ignition.ignite(ParameterServerIgniteImpl.GRID_NAME);
			partitionedParameterCache =
					ignite.getOrCreateCache(
							ParameterServerIgniteImpl.getParameterCacheConfiguration());
			partitionedParameterCache.clear();
			sharedParameterCache =
					ignite.getOrCreateCache(
							ParameterServerIgniteImpl.getSharedCacheConfiguration());
			sharedParameterCache.clear();
		}
	}

	@Override
	public void updateParameter(String id, ParameterElement el, ParameterElement opt) {
		ParameterElement dg = partitionedParameterCache.get("dg");
		if (dg == null || ((Double) opt.getValue() <= (Double) dg.getValue())) {
			partitionedParameterCache.put("dg", opt);
			partitionedParameterCache.withAsync().put(id, el);
		}
	}

	@Override
	public void updateShared(String id, ParameterElement el) {
		sharedParameterCache.withAsync().put(id, el);
	}

	@Override
	public ParameterElement getParameter(String id) {
		return partitionedParameterCache.get(id);
	}


	@Override
	public ParameterElement getSharedParameter(String id) {
		return sharedParameterCache.localPeek(id);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}

