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

package org.apache.flink.table.operations;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operation to describe a USE [catalogName.]dataBaseName statement.
 */
public class UseDatabaseOperation implements UseOperation {

	private String[] fullDatabaseName;

	public UseDatabaseOperation(String[] fullDatabaseName) {
		checkNotNull(fullDatabaseName);
		checkArgument(fullDatabaseName.length > 0 && fullDatabaseName.length <= 2,
					"database full path length can not be zero or greater than 2");
		this.fullDatabaseName = fullDatabaseName;
	}

	public String[] getFullDatabaseName() {
		return fullDatabaseName;
	}

	@Override
	public String asSummaryString() {
		if (fullDatabaseName.length == 1) {
			return String.format("USE %s", fullDatabaseName[0]);
		} else {
			return String.format("USE %s.%s", fullDatabaseName[0], fullDatabaseName[1]);
		}
	}
}
