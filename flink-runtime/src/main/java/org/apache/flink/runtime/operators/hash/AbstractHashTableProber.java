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

package org.apache.flink.runtime.operators.hash;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;

/**
 * @param <PT> probe side type
 * @param <BT> build side type
 */
public abstract class AbstractHashTableProber<PT, BT> {
	
	protected final TypeComparator<PT> probeTypeComparator;
	
	protected final TypePairComparator<PT, BT> pairComparator;
	
	public AbstractHashTableProber(TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, BT> pairComparator) {
		this.probeTypeComparator = probeTypeComparator;
		this.pairComparator = pairComparator;
	}
	
	public abstract BT getMatchFor(PT probeSideRecord, BT targetForMatch);

	public abstract BT getMatchFor(PT probeSideRecord);
	
	public abstract void updateMatch(BT record) throws IOException;
}
