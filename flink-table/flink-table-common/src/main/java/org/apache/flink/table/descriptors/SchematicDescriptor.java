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

/**
 * An interface for descriptors that allow to define a schema.
 *
 * @deprecated This class can be dropped once we remove external catalog support.
 */
@Deprecated
public interface SchematicDescriptor<D extends SchematicDescriptor<D>> extends Descriptor {

	/**
	 * Specifies the resulting table schema.
	 */
	D withSchema(Schema schema);
}
