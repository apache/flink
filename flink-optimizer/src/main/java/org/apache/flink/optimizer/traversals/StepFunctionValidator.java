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

package org.apache.flink.optimizer.traversals;

import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.util.Visitor;

import java.util.HashSet;
import java.util.Set;

/**
 * A traversal that checks if the Workset of a delta iteration is used in the data flow
 * of its step function.
 */
public class StepFunctionValidator implements Visitor<Operator<?>> {

	private final Set<Operator<?>> seenBefore = new HashSet<Operator<?>>();

	private boolean foundWorkset;

	@Override
	public boolean preVisit(Operator<?> visitable) {
		if (visitable instanceof DeltaIterationBase.WorksetPlaceHolder) {
			foundWorkset = true;
		}

		return (!foundWorkset) && seenBefore.add(visitable);
	}

	@Override
	public void postVisit(Operator<?> visitable) {}

	public boolean hasFoundWorkset() {
		return foundWorkset;
	}
}
