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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.api.common.functions.Comparator;

/**
 * Compares subpartition views based on the amount of their remaining credits.
 */
public class CreditBasedSubpartitionViewComparator implements Comparator {
	@Override
	public int compare(Object o1, Object o2) {
		if (o1 instanceof ExternalBlockSubpartitionView && o2 instanceof ExternalBlockSubpartitionView) {
			ExternalBlockSubpartitionView v1 = (ExternalBlockSubpartitionView) o1;
			ExternalBlockSubpartitionView v2 = (ExternalBlockSubpartitionView) o2;
			int deltaCredit = v2.getCreditUnsafe() - v1.getCreditUnsafe();
			if (deltaCredit != 0) {
				return deltaCredit;
			}
			if (!v1.getResultPartitionDir().equals(v2.getResultPartitionDir())) {
				return v1.getResultPartitionDir().compareTo(v2.getResultPartitionDir());
			}
			return v1.getSubpartitionIndex() - v2.getSubpartitionIndex();
		}
		return 0;
	}
}
