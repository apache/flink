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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.util.Collector;

/**
 * Abstract class for defining rich mapWindow transformation to be applied on
 * {@link WindowedDataStream}s. The mapWindow function will be called on each
 * {@link StreamWindow}.</p> In addition the user can access the functionality
 * provided by the {@link RichFunction} interface.
 */
public abstract class RichWindowMapFunction<IN, OUT> extends AbstractRichFunction implements
		WindowMapFunction<IN, OUT> {

	private static final long serialVersionUID = 9052714915997374185L;

	@Override
	public abstract void mapWindow(Iterable<IN> values, Collector<OUT> out) throws Exception;

}
