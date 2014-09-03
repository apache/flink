/**
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

package org.apache.flink.test.iterative.nephele.customdanglingpagerank.types;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class VertexWithRankSerializer extends TypeSerializer<VertexWithRank> {

	private static final long serialVersionUID = 1L;
	
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}
	
	@Override
	public VertexWithRank createInstance() {
		return new VertexWithRank();
	}

	@Override
	public VertexWithRank copy(VertexWithRank from, VertexWithRank reuse) {
		reuse.setVertexID(from.getVertexID());
		reuse.setRank(from.getRank());
		return reuse;
	}

	@Override
	public int getLength() {
		return 16;
	}

	@Override
	public void serialize(VertexWithRank record, DataOutputView target) throws IOException {
		target.writeLong(record.getVertexID());
		target.writeDouble(record.getRank());
	}

	@Override
	public VertexWithRank deserialize(VertexWithRank target, DataInputView source) throws IOException {
		target.setVertexID(source.readLong());
		target.setRank(source.readDouble());
		return target;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 16);
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public int hashCode() {
		return 1;
	}
	
	@Override
	public boolean equals(Object obj) {
		return obj != null && obj.getClass() == VertexWithRankSerializer.class;
	}
}
