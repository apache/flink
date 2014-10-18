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

package org.apache.flink.streaming.util.serialization;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class CombineTypeWrapper<OUT1, OUT2> extends
		TypeWrapper<Tuple2<OUT1, OUT2>> {

	private static final long serialVersionUID = 1L;
	// Info about OUT
	private TypeWrapper<OUT1> outTypeWrapper1;
	private TypeWrapper<OUT2> outTypeWrapper2;

	public CombineTypeWrapper(TypeWrapper<OUT1> outTypeWrapper1,
			TypeWrapper<OUT2> outTypeWrapper2) {
		this.outTypeWrapper1 = outTypeWrapper1;
		this.outTypeWrapper2 = outTypeWrapper2;
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}

	@Override
	protected void setTypeInfo() {
		typeInfo = new TupleTypeInfo<Tuple2<OUT1, OUT2>>(
				outTypeWrapper1.getTypeInfo(), outTypeWrapper2.getTypeInfo());
	}
}