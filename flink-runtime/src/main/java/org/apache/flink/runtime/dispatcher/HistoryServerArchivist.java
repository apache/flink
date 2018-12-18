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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;

import java.util.concurrent.CompletableFuture;

/**
 * Writer for an {@link AccessExecutionGraph}.
 */
public interface HistoryServerArchivist {

	/**
	 * Archives the given {@link AccessExecutionGraph} on the history server.
	 *
	 * @param executionGraph to store on the history server
	 * @return Future which is completed once the archiving has been completed.
	 */
	CompletableFuture<Acknowledge> archiveExecutionGraph(AccessExecutionGraph executionGraph);

	static HistoryServerArchivist createHistoryServerArchivist(Configuration configuration, JsonArchivist jsonArchivist) {
		final String configuredArchivePath = configuration.getString(JobManagerOptions.ARCHIVE_DIR);

		if (configuredArchivePath != null) {
			final Path archivePath = WebMonitorUtils.validateAndNormalizeUri(new Path(configuredArchivePath).toUri());

			return new JsonResponseHistoryServerArchivist(jsonArchivist, archivePath);
		} else {
			return VoidHistoryServerArchivist.INSTANCE;
		}
	}
}
