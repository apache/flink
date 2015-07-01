/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Date;


public final class DateSerializer extends TypeSerializerSingleton<Date> {

	private static final long serialVersionUID = 1L;
	
	public static final DateSerializer INSTANCE = new DateSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Date createInstance() {
		return new Date();
	}

	@Override
	public Date copy(Date from) {
		if(from == null) {
			return null;
		}
		return new Date(from.getTime());
	}

	
	@Override
	public Date copy(Date from, Date reuse) {
		if(from == null) {
			return null;
		}
		reuse.setTime(from.getTime());
		return reuse;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Date record, DataOutputView target) throws IOException {
		if(record == null) {
			target.writeLong(-1L);
		} else {
			target.writeLong(record.getTime());
		}
	}

	@Override
	public Date deserialize(DataInputView source) throws IOException {
		long v = source.readLong();
		if(v == -1L) {
			return null;
		} else {
			return new Date(v);
		}
	}
	
	@Override
	public Date deserialize(Date reuse, DataInputView source) throws IOException {
		long v = source.readLong();
		if(v == -1L) {
			return null;
		}
		reuse.setTime(v);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeLong(source.readLong());
	}
}
