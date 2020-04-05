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

package org.apache.flink.table.data;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.MapType;

/**
 * {@link MapData} is an internal data structure representing data of {@link MapType}
 * in Flink Table/SQL.
 */
@PublicEvolving
public interface MapData {

	/**
	 * Returns the number of key-value mappings in this map.
	 */
	int size();

	/**
	 * Returns an array view of the keys contained in this map.
	 * A key-value pair has the same index in the {@code keyArray} and {@code valueArray}.
	 */
	ArrayData keyArray();

	/**
	 * Returns an array view of the values contained in this map.
	 * A key-value pair has the same index in the {@code keyArray} and {@code valueArray}.
	 */
	ArrayData valueArray();
}
