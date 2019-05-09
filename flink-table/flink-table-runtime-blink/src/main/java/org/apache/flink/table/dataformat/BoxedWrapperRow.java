/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat;

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;

/**
 * A {@link ObjectArrayRow} that wrap primitive type to boxed object to reuse.
 */
public final class BoxedWrapperRow extends ObjectArrayRow {

	public BoxedWrapperRow(int arity) {
		super(arity);
	}

	@Override
	public boolean getBoolean(int i) {
		return ((BooleanValue) fields[i]).getValue();
	}

	@Override
	public byte getByte(int i) {
		return ((ByteValue) fields[i]).getValue();
	}

	@Override
	public short getShort(int i) {
		return ((ShortValue) fields[i]).getValue();
	}

	@Override
	public int getInt(int i) {
		return ((IntValue) fields[i]).getValue();
	}

	@Override
	public long getLong(int i) {
		return ((LongValue) fields[i]).getValue();
	}

	@Override
	public float getFloat(int i) {
		return ((FloatValue) fields[i]).getValue();
	}

	@Override
	public double getDouble(int i) {
		return ((DoubleValue) fields[i]).getValue();
	}

	@Override
	public void setBoolean(int i, boolean value) {
		BooleanValue wrap;
		if ((wrap = (BooleanValue) fields[i]) == null) {
			wrap = new BooleanValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setByte(int i, byte value) {
		ByteValue wrap;
		if ((wrap = (ByteValue) fields[i]) == null) {
			wrap = new ByteValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setShort(int i, short value) {
		ShortValue wrap;
		if ((wrap = (ShortValue) fields[i]) == null) {
			wrap = new ShortValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setInt(int i, int value) {
		IntValue wrap;
		if ((wrap = (IntValue) fields[i]) == null) {
			wrap = new IntValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setLong(int i, long value) {
		LongValue wrap;
		if ((wrap = (LongValue) fields[i]) == null) {
			wrap = new LongValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setFloat(int i, float value) {
		FloatValue wrap;
		if ((wrap = (FloatValue) fields[i]) == null) {
			wrap = new FloatValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setDouble(int i, double value) {
		DoubleValue wrap;
		if ((wrap = (DoubleValue) fields[i]) == null) {
			wrap = new DoubleValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	public void setNonPrimitiveValue(int i, Object value) {
		fields[i] = value;
	}
}
