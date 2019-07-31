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

package org.apache.flink.table.planner.catalog;

import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Base class for flink {@link Schema}, which provides some default implementations.
 */
public abstract class FlinkSchema implements Schema {

	@Override
	public RelProtoDataType getType(String name) {
		return null;
	}

	@Override
	public Set<String> getTypeNames() {
		return Collections.emptySet();
	}

	@Override
	public Collection<Function> getFunctions(String name) {
		return Collections.emptyList();
	}

	@Override
	public Set<String> getFunctionNames() {
		return Collections.emptySet();
	}

	@Override
	public Schema snapshot(SchemaVersion version) {
		return this;
	}
}
