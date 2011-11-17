/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class AbstractTaggableRecord implements Record {

	private byte[] tag = null;

	public void setTag(final byte[] tag) {
		this.tag = tag;
	}

	public byte[] getTag() {

		return this.tag;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		if (this.tag == null) {
			out.writeBoolean(false);
		} else {
			out.writeBoolean(true);
			out.writeShort((short) this.tag.length);
			out.write(this.tag);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		if (in.readBoolean()) {
			final short length = in.readShort();
			this.tag = new byte[length];
			in.readFully(this.tag);
		}
	}
}
