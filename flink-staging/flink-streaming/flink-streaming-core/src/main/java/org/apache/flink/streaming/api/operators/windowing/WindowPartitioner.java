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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.ChainableStreamOperator;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * This operator applies either split or key partitioning depending on the
 * transformation.
 */
public class WindowPartitioner<T> extends ChainableStreamOperator<StreamWindow<T>, StreamWindow<T>> {

	private KeySelector<T, ?> keySelector;
	private int numberOfSplits;

	public WindowPartitioner(KeySelector<T, ?> keySelector) {
		super(null);
		this.keySelector = keySelector;
		withoutInputCopy();
	}

	public WindowPartitioner(int numberOfSplits) {
		super(null);
		this.numberOfSplits = numberOfSplits;
		withoutInputCopy();
	}

	private static final long serialVersionUID = 1L;

	@Override
	public void run() throws Exception {
		while (isRunning && readNext() != null) {
			callUserFunctionAndLogException();
		}
	}

	@Override
	protected void callUserFunction() throws Exception {
		StreamWindow<T> currentWindow = nextObject;
		if (keySelector == null) {
			if (numberOfSplits <= 1) {
				collector.collect(currentWindow);
			} else {
				for (StreamWindow<T> window : StreamWindow.split(currentWindow, numberOfSplits)) {
					collector.collect(window);
				}
			}
		} else {

			for (StreamWindow<T> window : StreamWindow
					.partitionBy(currentWindow, keySelector, true)) {
				collector.collect(window);
			}

		}
	}
}
