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

package org.apache.flink.cep.time;

import org.apache.flink.annotation.Internal;

/**
 * Enables to provide time characteristic to {@link org.apache.flink.cep.nfa.NFA} for use in
 * {@link org.apache.flink.cep.pattern.conditions.IterativeCondition}.
 */
@Internal
public interface TimerService {

	/**
	 * Current processing time as returned from {@link org.apache.flink.streaming.api.TimerService}.
	 */
	long currentProcessingTime();

}
