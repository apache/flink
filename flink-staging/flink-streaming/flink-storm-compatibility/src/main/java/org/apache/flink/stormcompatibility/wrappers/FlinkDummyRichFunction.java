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

package org.apache.flink.stormcompatibility.wrappers;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * {@link FlinkDummyRichFunction} has the only purpose to retrieve the {@link RuntimeContext} for
 * {@link StormBoltWrapper} provided by Flink.
 */
class FlinkDummyRichFunction implements RichFunction, Serializable {
	private static final long serialVersionUID = 7992273349877302520L;

	// The runtime context of a Storm bolt
	private RuntimeContext context;

	@Override
	public void open(final Configuration parameters) throws Exception {/* nothing to do */}

	@Override
	public void close() throws Exception {/* nothing to do */}

	@Override
	public RuntimeContext getRuntimeContext() {
		return this.context;
	}

	@Override
	public void setRuntimeContext(final RuntimeContext t) {
		this.context = t;
	}

}
