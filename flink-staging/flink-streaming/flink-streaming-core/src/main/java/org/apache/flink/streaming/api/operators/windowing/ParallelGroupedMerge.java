/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.windowing;

import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * The version of the ParallelMerge CoFlatMap that does not reduce the incoming
 * elements only appends them to the current window. This is necessary for
 * grouped reduces.
 */
public class ParallelGroupedMerge<OUT> extends ParallelMerge<OUT> {

	private static final long serialVersionUID = 1L;

	public ParallelGroupedMerge() {
		super(null);
	}

	@Override
	protected void updateCurrent(StreamWindow<OUT> current, StreamWindow<OUT> nextWindow)
			throws Exception {
		current.addAll(nextWindow);
	}

}
