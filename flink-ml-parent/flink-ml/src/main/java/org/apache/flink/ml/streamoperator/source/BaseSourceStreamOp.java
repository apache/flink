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

package org.apache.flink.ml.streamoperator.source;

import org.apache.flink.ml.io.AnnotationUtils;
import org.apache.flink.ml.io.BaseDB;
import org.apache.flink.ml.io.Constants;
import org.apache.flink.ml.io.IOType;
import org.apache.flink.ml.io.IOTypeAnnotation;
import org.apache.flink.ml.params.Params;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.table.api.Table;

/**
 * Base class for the streaming sources.
 *
 * @param <T> the type of actual streaming source.
 */
@IOTypeAnnotation(type = IOType.SourceStream)
public abstract class BaseSourceStreamOp<T extends BaseSourceStreamOp <T>> extends StreamOperator <T> {

	static final IOType IO_TYPE = IOType.SourceStream;

	protected BaseSourceStreamOp(String nameSrcSnk, Params params) {
		super(params);
		this.params.set(Constants.IO_TYPE, IO_TYPE, IOType.class)
			.set(Constants.IO_NAME, nameSrcSnk);
	}

	public static BaseSourceStreamOp of(Params params) throws Exception {
		if (params.contains(Constants.IO_TYPE)
			&& params.get(Constants.IO_TYPE, IOType.class).equals(IO_TYPE)
			&& params.contains(Constants.IO_NAME)) {
			if (BaseDB.isDB(params)) {
				return new DBSourceStreamOp(BaseDB.of(params), params);
			} else if (params.contains(Constants.IO_NAME)) {
				String name = params.getString(Constants.IO_NAME);
				return (BaseSourceStreamOp) AnnotationUtils.createOp(name, IO_TYPE, params);
			}
		}
		throw new RuntimeException("Parameter Error.");
	}

	@Override
	public T linkFrom(StreamOperator in) {
		throw new UnsupportedOperationException("Source operator does not support linkFrom()");
	}

	@Override
	public Table getTable() {
		if (super.table == null) {
			super.table = initializeDataSource();
		}
		return super.table;
	}

	protected abstract Table initializeDataSource();
}
