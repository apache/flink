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

package org.apache.flink.util;

import java.io.Serializable;
import java.util.Iterator;


public abstract class SplittableIterator<T> implements Iterator<T>, Serializable {

	private static final long serialVersionUID = 200377674313072307L;

	public abstract Iterator<T>[] split(int numPartitions);

	public Iterator<T> getSplit(int num, int numPartitions) {
		if (numPartitions < 1 || num < 0 || num >= numPartitions) {
			throw new IllegalArgumentException();
		}

		return split(numPartitions)[num];
	}

	public abstract int getMaximumNumberOfSplits();
}
