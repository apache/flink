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

package org.apache.flink.cep.nfa.aftermatch;

import org.apache.flink.cep.nfa.sharedbuffer.EventId;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Every possible match will be emitted.
 */
public class NoSkipStrategy extends AfterMatchSkipStrategy {

	private static final long serialVersionUID = -5843740153729531775L;

	static final NoSkipStrategy INSTANCE = new NoSkipStrategy();

	private NoSkipStrategy() {
	}

	@Override
	public boolean isSkipStrategy() {
		return false;
	}

	@Override
	protected boolean shouldPrune(EventId startEventID, EventId pruningId) {
		throw new IllegalStateException("This should never happen. Please file a bug.");
	}

	@Override
	protected EventId getPruningId(Collection<Map<String, List<EventId>>> match) {
		throw new IllegalStateException("This should never happen. Please file a bug.");
	}

	@Override
	public String toString() {
		return "NoSkipStrategy{}";
	}
}
