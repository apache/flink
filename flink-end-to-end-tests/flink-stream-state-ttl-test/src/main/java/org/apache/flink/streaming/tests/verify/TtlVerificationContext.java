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

package org.apache.flink.streaming.tests.verify;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Data to verify state update with TTL. */
public class TtlVerificationContext<UV, GV> implements Serializable {
	private final List<TtlValue<UV>> prevUpdates;
	private final TtlUpdateContext<UV, GV> updateContext;

	@SuppressWarnings("unchecked")
	public TtlVerificationContext(
		List<TtlValue<?>> prevUpdates,
		@Nonnull TtlUpdateContext<?, ?> updateContext) {
		prevUpdates = prevUpdates == null ? Collections.emptyList() : prevUpdates;
		this.prevUpdates = new ArrayList<>();
		prevUpdates.forEach(pu -> this.prevUpdates.add((TtlValue<UV>) pu));
		this.updateContext = (TtlUpdateContext<UV, GV>) updateContext;
	}

	@Nonnull
	List<TtlValue<UV>> getPrevUpdates() {
		return prevUpdates;
	}

	@Nonnull
	TtlUpdateContext<UV, GV> getUpdateContext() {
		return updateContext;
	}

	@Override
	public String toString() {
		return "TtlVerificationContext{" +
			"prevUpdates=" + prevUpdates +
			", updateContext=" + updateContext +
			'}';
	}
}
