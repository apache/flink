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

package org.apache.flink.table.temptable;

import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.util.TableProperties;

/**
 * Describe a TableFactory for FlinkTableServiceSource and FlinkTableServiceSink.
 */
public class FlinkTableServiceFactoryDescriptor {
	private TableFactory tableFactory;

	/**
	 * Properties for FlinkTableServiceSource and FlinkTableServiceSink.
	 */
	private TableProperties tableProperties;

	public FlinkTableServiceFactoryDescriptor(TableFactory tableFactory, TableProperties tableProperties) {
		this.tableFactory = tableFactory;
		this.tableProperties = tableProperties;
	}

	public TableFactory getTableFactory() {
		return tableFactory;
	}

	public void setTableFactory(TableFactory tableFactory) {
		this.tableFactory = tableFactory;
	}

	public TableProperties getTableProperties() {
		return tableProperties;
	}

	public void setTableProperties(TableProperties tableProperties) {
		this.tableProperties = tableProperties;
	}

}
