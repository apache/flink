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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

abstract class AbstractTtlStateVerifier<D extends StateDescriptor<S, SV>, S extends State, SV, UV, GV>
	implements TtlStateVerifier<UV, GV> {
	static final Random RANDOM = new Random();

	@Nonnull
	final D stateDesc;

	AbstractTtlStateVerifier(@Nonnull D stateDesc) {
		this.stateDesc = stateDesc;
	}

	@Nonnull
	static String randomString() {
		StringBuilder sb = new StringBuilder();
		IntStream.range(0, RANDOM.nextInt(14) + 2).forEach(i -> sb.append(randomChar()));
		return sb.toString();
	}

	private static char randomChar() {
		char d = (char) ('0' + RANDOM.nextInt(9));
		char l = (char) ('a' + RANDOM.nextInt(25));
		return RANDOM.nextBoolean() ? d : l;
	}

	@SuppressWarnings("unchecked")
	@Override
	@Nonnull
	public State createState(@Nonnull FunctionInitializationContext context, @Nonnull StateTtlConfiguration ttlConfig) {
		stateDesc.enableTimeToLive(ttlConfig);
		return createState(context);
	}

	abstract State createState(FunctionInitializationContext context);

	@SuppressWarnings("unchecked")
	@Override
	@Nonnull
	public TypeSerializer<UV> getUpdateSerializer() {
		return (TypeSerializer<UV>) stateDesc.getSerializer();
	}

	@SuppressWarnings("unchecked")
	@Override
	public GV get(@Nonnull State state) throws Exception {
		return getInternal((S) state);
	}

	abstract GV getInternal(@Nonnull S state) throws Exception;

	@SuppressWarnings("unchecked")
	@Override
	public void update(@Nonnull State state, Object update) throws Exception {
		updateInternal((S) state, (UV) update);
	}

	abstract void updateInternal(@Nonnull S state, UV update) throws Exception;

	@SuppressWarnings("unchecked")
	@Override
	public boolean verify(@Nonnull TtlVerificationContext<?, ?> verificationContextRaw, @Nonnull Time precision) {
		TtlVerificationContext<UV, GV> verificationContext = (TtlVerificationContext<UV, GV>) verificationContextRaw;
		if (!isWithinPrecision(verificationContext, precision)) {
			return true;
		}
		List<TtlValue<UV>> updates = new ArrayList<>(verificationContext.getPrevUpdates());
		long currentTimestamp = verificationContext.getUpdateContext().getTimestamp();
		GV prevValue = expected(updates, currentTimestamp);
		GV valueBeforeUpdate = verificationContext.getUpdateContext().getValueBeforeUpdate();
		TtlValue<UV> update = verificationContext.getUpdateContext().getUpdateWithTs();
		GV updatedValue = verificationContext.getUpdateContext().getUpdatedValue();
		updates.add(update);
		GV expectedValue = expected(updates, currentTimestamp);
		return valuesEqual(valueBeforeUpdate, prevValue) && valuesEqual(updatedValue, expectedValue);
	}

	private boolean isWithinPrecision(TtlVerificationContext<UV, GV> verificationContext, Time precision) {
		List<TtlValue<UV>> prevUpdates = verificationContext.getPrevUpdates();
		if (prevUpdates.isEmpty()) {
			return true;
		}
		long ts = verificationContext.getUpdateContext().getTimestamp();
		long ttl = stateDesc.getTtlConfig().getTtl().toMilliseconds();
		return prevUpdates.stream().allMatch(u -> {
			long delta = ts - u.getUpdateTimestamp() - ttl;
			return precision.toMilliseconds() < delta;
		});
	}

	private boolean valuesEqual(GV v1, GV v2) {
		return (v1 == null && v2 == null) || (v1 != null && v1.equals(v2));
	}

	abstract GV expected(@Nonnull List<TtlValue<UV>> updates, long currentTimestamp);

	boolean expired(long lastTimestamp, long currentTimestamp) {
		return lastTimestamp + stateDesc.getTtlConfig().getTtl().toMilliseconds() <= currentTimestamp;
	}
}
