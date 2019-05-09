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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;

/**
 * An interface for descriptors that allow to register table source and/or sinks.
 */
@PublicEvolving
public interface RegistrableDescriptor extends Descriptor {

	/**
	 * Searches for the specified table source, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	void registerTableSource(String name);

	/**
	 * Searches for the specified table sink, configures it accordingly, and registers it as
	 * a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	void registerTableSink(String name);

	/**
	 * Searches for the specified table source and sink, configures them accordingly, and registers
	 * them as a table under the given name.
	 *
	 * @param name table name to be registered in the table environment
	 */
	void registerTableSourceAndSink(String name);
}
