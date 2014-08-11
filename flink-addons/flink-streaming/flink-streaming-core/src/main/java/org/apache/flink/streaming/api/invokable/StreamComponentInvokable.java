/**
 *
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
 *
 */

package org.apache.flink.streaming.api.invokable;

import java.io.Serializable;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public abstract class StreamComponentInvokable<OUT> extends AbstractRichFunction implements
		Serializable {

	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unused")
	private String componentName;
	@SuppressWarnings("unused")
	private int channelID;
	protected Collector<OUT> collector;
	protected Function userFunction;

	public StreamComponentInvokable(Function userFunction) {
		this.userFunction = userFunction;
	}

	public void setCollector(Collector<OUT> collector) {
		this.collector = collector;
	}

	public void setAttributes(String componentName, int channelID) {
		this.componentName = componentName;
		this.channelID = channelID;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		if (userFunction instanceof RichFunction) {
			((RichFunction) userFunction).open(parameters);
		}
	}

	@Override
	public void close() throws Exception {
		if (userFunction instanceof RichFunction) {
			((RichFunction) userFunction).close();
		}
	}
	
	public abstract void invoke() throws Exception;
}
