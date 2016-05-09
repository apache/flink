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
package org.apache.flink.metrics.reservoir.dropwizard;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.reservoir.Reservoir;

/**
 * Wrapper that allows Dropwizard reporters to use {@link org.apache.flink.metrics.reservoir.Reservoir}.
 */
@Internal
public class FlinkToDropwizardReservoirWrapper implements com.codahale.metrics.Reservoir {
	private final Reservoir reservoir;

	public FlinkToDropwizardReservoirWrapper(Reservoir reservoir) {
		this.reservoir = reservoir;
	}

	@Override
	public int size() {
		return this.reservoir.size();
	}

	@Override
	public void update(long l) {
		this.reservoir.update(l);
	}

	@Override
	public com.codahale.metrics.Snapshot getSnapshot() {
		return new FlinkToDropwizardSnapshotWrapper(this.reservoir.createSnapshot());
	}
}
