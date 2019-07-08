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

package org.apache.flink.addons.hbase.parser;

import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;
import java.io.Serializable;

/**
 * RowParser.
 */
public interface RowParser<T> extends Serializable {
	/**
	 * init a RowParser.
	 */
	default void init() {}

	/**
	 * create Get from input parameter.
	 * NOTICE: now only support key.
	 */
	Get createGet(Object in) throws IOException;

	/**
	 * parse HBase result to a Row.
	 */
	Row parseToRow(T result, Object rowKey) throws IOException;
}
