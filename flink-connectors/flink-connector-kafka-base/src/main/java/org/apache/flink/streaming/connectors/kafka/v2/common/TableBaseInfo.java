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

package org.apache.flink.streaming.connectors.kafka.v2.common;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.TypeInfoWrappedDataType;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Tool interface for table source/sink which needs extra config info.
 *  We expect to remove this later on.
 */
public class TableBaseInfo implements Serializable {
	private static final long serialVersionUID = 1L;

	// use defined kv pairs with in sql with, all the keys are lowercase
	protected Map<String, String> userParamsMap;
	protected List<String> primaryKeys; // use defined PKs
	protected List<String> headerFields; // header field list
	protected RowTypeInfo rowTypeInfo; // field name and type

	public TableBaseInfo setUserParamsMap(Map<String, String> userParamsMap) {
		this.userParamsMap = userParamsMap;
		return this;
	}

	public TableBaseInfo setPrimaryKeys(List<String> primaryKeys) {
		this.primaryKeys = primaryKeys;
		return this;
	}

	public TableBaseInfo setHeaderFields(List<String> headerFields) {
		this.headerFields = headerFields;
		return this;
	}

	public TableBaseInfo setRowTypeInfo(RowTypeInfo rowTypeInfo) {
		this.rowTypeInfo = rowTypeInfo;
		return this;
	}

	public TableBaseInfo setRowTypeInfo(DataType dataType) {
		this.rowTypeInfo = (RowTypeInfo) ((TypeInfoWrappedDataType) dataType).getTypeInfo();
		return this;
	}
}
