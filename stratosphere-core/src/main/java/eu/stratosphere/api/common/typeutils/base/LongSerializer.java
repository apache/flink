/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.common.typeutils.base;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


public class LongSerializer extends TypeSerializer<Long> {

	private static final long serialVersionUID = 1L;
	
	public static final LongSerializer INSTANCE = new LongSerializer();
	
	private static final Long ZERO = Long.valueOf(0);


	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public boolean isStateful() {
		return false;
	}
	
	@Override
	public Long createInstance() {
		return ZERO;
	}

	@Override
	public Long copy(Long from, Long reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Long record, DataOutputView target) throws IOException {
		target.writeLong(record.longValue());
	}

	@Override
	public Long deserialize(Long reuse, DataInputView source) throws IOException {
		return Long.valueOf(source.readLong());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeLong(source.readLong());
	}
}
