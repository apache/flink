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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import java.io.Serializable;

import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Class for defining specialized buffers to store/emit window data.
 * Pre-aggregators should be implemented using this interface.
 */
public abstract class WindowBuffer<T> implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	protected Integer nextID = 1;
	protected boolean sequentialID = false;
	protected boolean emitEmpty = false;
	protected boolean emitPerGroup = false;

	public abstract void store(T element) throws Exception;

	public abstract void evict(int n);

	public abstract void emitWindow(Collector<StreamRecord<StreamWindow<T>>> collector);

	public abstract WindowBuffer<T> clone();

	public WindowBuffer<T> emitEmpty() {
		emitEmpty = true;
		return this;
	}

	public WindowBuffer<T> sequentialID() {
		sequentialID = true;
		return this;
	}

	protected StreamWindow<T> createEmptyWindow() {
		return sequentialID ? new StreamWindow<T>(nextID++) : new StreamWindow<T>();
	}

}
