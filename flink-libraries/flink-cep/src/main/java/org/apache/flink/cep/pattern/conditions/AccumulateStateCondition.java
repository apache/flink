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

package org.apache.flink.cep.pattern.conditions;

/**
 * AccmulateStateCondition. Here is the backgroundThe accmulator state belongs to a specific
 * ComputationState instance, which meansit only needs to be accumulated once per record. And ithas
 * nothing to do with the conditions either the edges, even thought the interfaceis defined in
 * IterativeCondition.
 */
public interface AccumulateStateCondition<T> {
    /** Accumulate the state and make sure filter() function is stateless). */
    void accumulate(T value, IterativeCondition.Context<T> ctx) throws Exception;
}
