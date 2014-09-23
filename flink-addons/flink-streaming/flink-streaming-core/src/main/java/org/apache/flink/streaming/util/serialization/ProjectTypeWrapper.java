/**
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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

public class ProjectTypeWrapper<IN,OUT extends Tuple> extends
		TypeWrapper<OUT> {
	private static final long serialVersionUID = 1L;


	private TypeWrapper<IN> inType;
	Class<?>[] givenTypes;
	int[] fields;

	public ProjectTypeWrapper(TypeWrapper<IN> inType,int[] fields,Class<?>[] givenTypes) {
		this.inType = inType;
		this.givenTypes = givenTypes;
		this.fields = fields;
		setTypeInfo();
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException,
			ClassNotFoundException {
		in.defaultReadObject();
		setTypeInfo();
	}

	@Override
	protected void setTypeInfo() {
		TypeInformation<?>[] outTypes = extractFieldTypes();
		this.typeInfo = new TupleTypeInfo<OUT>(outTypes);
	}
	
	private TypeInformation<?>[] extractFieldTypes() {
		
		TupleTypeInfo<?> inTupleType = (TupleTypeInfo<?>) inType.getTypeInfo();
		TypeInformation<?>[] fieldTypes = new TypeInformation[fields.length];
				
		for(int i=0; i<fields.length; i++) {
			
			if(inTupleType.getTypeAt(fields[i]).getTypeClass() != givenTypes[i]) {
				throw new IllegalArgumentException("Given types do not match types of input data set.");
			}
				
			fieldTypes[i] = inTupleType.getTypeAt(fields[i]);
		}
		
		return fieldTypes;
	}
}