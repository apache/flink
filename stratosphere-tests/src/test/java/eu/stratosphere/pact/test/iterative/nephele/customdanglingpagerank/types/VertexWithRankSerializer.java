/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.test.iterative.nephele.customdanglingpagerank.types;

import java.io.IOException;

import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


/**
 *
 */
public final class VertexWithRankSerializer extends TypeSerializer<VertexWithRank> {

	@Override
	public VertexWithRank createInstance() {
		return new VertexWithRank();
	}

	@Override
	public VertexWithRank createCopy(VertexWithRank from) {
		VertexWithRank n = new VertexWithRank();
		copyTo(from, n);
		return n;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.generic.types.TypeSerializer#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(VertexWithRank from, VertexWithRank to) {
		to.setVertexID(from.getVertexID());
		to.setRank(from.getRank());
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
	public void deserialize(VertexWithRank target, DataInputView source) throws IOException {
		target.setVertexID(source.readLong());
		target.setRank(source.readDouble());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 16);
	}
}
