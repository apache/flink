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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for a {@link DispatcherBootstrap}.
 *
 * <p>NOTE TO IMPLEMENTERS: This is meant as a bridge between the package-private
 * {@link Dispatcher#runRecoveredJob(JobGraph)} method, and dispatcher bootstrap
 * implementations in other packages.
 */
@Internal
public abstract class AbstractDispatcherBootstrap implements DispatcherBootstrap {

	protected void launchRecoveredJobGraphs(final Dispatcher dispatcher, final Collection<JobGraph> recoveredJobGraphs) {
		checkNotNull(dispatcher);
		checkNotNull(recoveredJobGraphs);

		for (JobGraph recoveredJob : recoveredJobGraphs) {
			dispatcher.runRecoveredJob(recoveredJob);
		}
	}
}
