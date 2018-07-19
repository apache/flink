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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/** Base class for State TTL verifiers. */
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
		return StringUtils.getRandomString(RANDOM, 2, 20);
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
	public boolean verify(@Nonnull TtlVerificationContext<?, ?> verificationContextRaw) {
		TtlVerificationContext<UV, GV> verificationContext = (TtlVerificationContext<UV, GV>) verificationContextRaw;
		List<ValueWithTs<UV>> updates = new ArrayList<>(verificationContext.getPrevUpdates());
		long currentTimestamp = verificationContext.getUpdateContext().getTimestampBeforeUpdate();
		GV prevValue = expected(updates, currentTimestamp);
		GV valueBeforeUpdate = verificationContext.getUpdateContext().getValueBeforeUpdate();
		ValueWithTs<UV> update = verificationContext.getUpdateContext().getUpdateWithTs();
		GV updatedValue = verificationContext.getUpdateContext().getUpdatedValue();
		updates.add(update);
		GV expectedValue = expected(updates, currentTimestamp);
		return Objects.equals(valueBeforeUpdate, prevValue) && Objects.equals(updatedValue, expectedValue);
	}

	abstract GV expected(@Nonnull List<ValueWithTs<UV>> updates, long currentTimestamp);

	boolean expired(long lastTimestamp, long currentTimestamp) {
		return lastTimestamp + stateDesc.getTtlConfig().getTtl().toMilliseconds() <= currentTimestamp;
	}
}
