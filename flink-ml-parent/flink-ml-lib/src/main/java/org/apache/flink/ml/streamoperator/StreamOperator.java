/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.streamoperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.ml.streamoperator.source.TableSourceStreamOp;
import org.apache.flink.table.api.Table;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of streaming algorithm operators.
 */
public abstract class StreamOperator<T extends StreamOperator <T>> extends AlgoOperator<T> {

	public StreamOperator() {
		super();
	}

	public StreamOperator(Params params) {
		super(params);
	}

	public static StreamOperator sourceFrom(Table table) {
		return new TableSourceStreamOp(table);
	}

	@Override
	public String toString() {
		return getOutput().toString();
	}

	public <S extends StreamOperator> S link(S next) {
		return linkTo(next);
	}

	public <S extends StreamOperator> S linkTo(S next) {
		next.linkFrom(this);
		return (S) next;
	}

	public abstract T linkFrom(StreamOperator in);

	public T linkFrom(StreamOperator in1, StreamOperator in2) {
		List <StreamOperator> ls = new ArrayList();
		ls.add(in1);
		ls.add(in2);
		return linkFrom(ls);
	}

	public T linkFrom(StreamOperator in1, StreamOperator in2, StreamOperator in3) {
		List <StreamOperator> ls = new ArrayList();
		ls.add(in1);
		ls.add(in2);
		ls.add(in3);
		return linkFrom(ls);
	}

	public T linkFrom(List <StreamOperator> ins) {
		if (null != ins && ins.size() == 1) {
			return linkFrom(ins.get(0));
		} else {
			throw new RuntimeException("Not support more than 1 inputs!");
		}
	}

}
