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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.util.NoSuchElementException;

/**
 * A {@link CloseableIterator} for insert operation result.
 */
@Internal
class InsertResultIterator implements CloseableIterator<Row> {
	private final JobClient jobClient;
	private final Row affectedRowCountsRow;
	private final ClassLoader classLoader;
	@Nullable
	private Boolean hasNext = null;

	InsertResultIterator(JobClient jobClient, Row affectedRowCountsRow, ClassLoader classLoader) {
		this.jobClient = jobClient;
		this.affectedRowCountsRow = affectedRowCountsRow;
		this.classLoader = classLoader;
	}

	@Override
	public void close() throws Exception {
		jobClient.cancel();
	}

	@Override
	public boolean hasNext() {
		if (hasNext == null) {
			try {
				jobClient.getJobExecutionResult().get();
			} catch (Exception e) {
				throw new TableException("Failed to wait job finish", e);
			}
			hasNext = true;
		}
		return hasNext;
	}

	@Override
	public Row next() {
		if (hasNext()) {
			hasNext = false;
			return affectedRowCountsRow;
		} else {
			throw new NoSuchElementException();
		}
	}
}
