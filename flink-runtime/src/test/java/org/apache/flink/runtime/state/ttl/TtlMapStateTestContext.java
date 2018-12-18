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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

abstract class TtlMapStateTestContext<UV, GV>
	extends TtlStateTestContextBase<TtlMapState<?, String, Integer, String>, UV, GV> {
	@SuppressWarnings("unchecked")
	@Override
	<US extends State, SV> StateDescriptor<US, SV> createStateDescriptor() {
		return (StateDescriptor<US, SV>) new MapStateDescriptor<>(
			"TtlTestMapState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
	}
}
