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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * This operator flattens the results of the window transformations by
 * outputing the elements of the {@link StreamWindow} one-by-one
 */
public class WindowFlattener<T> extends AbstractStreamOperator<T>
		implements OneInputStreamOperator<StreamWindow<T>, T> {

	private static final long serialVersionUID = 1L;

	public WindowFlattener() {
		chainingStrategy = ChainingStrategy.FORCE_ALWAYS;
	}

	@Override
	public void processElement(StreamWindow<T> window) throws Exception {
		for (T element : window) {
			output.collect(element);
		}
	}
}
