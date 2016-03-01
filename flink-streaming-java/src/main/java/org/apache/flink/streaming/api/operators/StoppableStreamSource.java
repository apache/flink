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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * {@link StoppableStreamSource} takes a {@link SourceFunction} that implements {@link StoppableFunction}.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function which has to be stoppable
 */
public class StoppableStreamSource<OUT, SRC extends SourceFunction<OUT> & StoppableFunction>
	extends StreamSource<OUT, SRC> {

	private static final long serialVersionUID = -4365670858793587337L;

	/**
	 * Takes a {@link SourceFunction} that implements {@link StoppableFunction}.
	 *
	 * @param sourceFunction
	 *            A {@link SourceFunction} that implements {@link StoppableFunction}.
	 */
	public StoppableStreamSource(SRC sourceFunction) {
		super(sourceFunction);
	}

	/**
	 * Marks the source a stopped and calls {@link StoppableFunction#stop()} on the user function.
	 */
	public void stop() {
		// important: marking the source as stopped has to happen before the function is stopped.
		// the flag that tracks this status is volatile, so the memory model also guarantees
		// the happens-before relationship
		markCanceledOrStopped();
		userFunction.stop();
	}
}
