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

package eu.stratosphere.nephele.io.compression.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;

public class ThroughputAnalyzerResult implements IOReadableWritable {

	private long noCompressionTime = -1;

	private long lightCompressionTime = -1;

	private long mediumCompressionTime = -1;

	private long mediumHeavyCompressionTime = -1;

	private long heavyCompressionTime = -1;

	private long byteThroughputTime = -1;

	private int noCompressionAge = 0;

	private int lightCompressionAge = 0;

	private int mediumCompressionAge = 0;

	private int mediumHeavyCompressionAge = 0;

	private int heavyCompressionAge = 0;

	@Override
	public void read(DataInput in) throws IOException {
		this.noCompressionTime = in.readLong();
		this.lightCompressionTime = in.readLong();
		this.mediumCompressionTime = in.readLong();
		this.mediumHeavyCompressionTime = in.readLong();
		this.heavyCompressionTime = in.readLong();

		this.byteThroughputTime = in.readLong();

		this.noCompressionAge = in.readInt();
		this.lightCompressionAge = in.readInt();
		this.mediumCompressionAge = in.readInt();
		this.mediumHeavyCompressionAge = in.readInt();
		this.heavyCompressionAge = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(noCompressionTime);
		out.writeLong(lightCompressionTime);
		out.writeLong(mediumCompressionTime);
		out.writeLong(mediumHeavyCompressionTime);
		out.writeLong(heavyCompressionTime);

		out.writeLong(byteThroughputTime);

		out.writeInt(noCompressionAge);
		out.writeInt(lightCompressionAge);
		out.writeInt(mediumCompressionAge);
		out.writeInt(mediumHeavyCompressionAge);
		out.writeInt(heavyCompressionAge);

	}

	public long getNoCompressionTime() {
		return noCompressionTime;
	}

	public void setNoCompressionTime(long noCompressionTime) {
		this.noCompressionTime = noCompressionTime;
	}

	public long getLightCompressionTime() {
		return lightCompressionTime;
	}

	public void setLightCompressionTime(long lightCompressionTime) {
		this.lightCompressionTime = lightCompressionTime;
	}

	public long getMediumCompressionTime() {
		return mediumCompressionTime;
	}

	public void setMediumCompressionTime(long mediumCompressionTime) {
		this.mediumCompressionTime = mediumCompressionTime;
	}

	public long getMediumHeavyCompressionTime() {
		return mediumHeavyCompressionTime;
	}

	public void setMediumHeavyCompressionTime(long mediumHeavyCompressionTime) {
		this.mediumHeavyCompressionTime = mediumHeavyCompressionTime;
	}

	public long getHeavyCompressionTime() {
		return heavyCompressionTime;
	}

	public void setHeavyCompressionTime(long heavyCompressionTime) {
		this.heavyCompressionTime = heavyCompressionTime;
	}

	public long getByteThroughputTime() {
		return byteThroughputTime;
	}

	public void setByteThroughputTime(long byteThroughputTime) {
		this.byteThroughputTime = byteThroughputTime;
	}

	public int getNoCompressionAge() {
		return noCompressionAge;
	}

	public void setNoCompressionAge(int noCompressionAge) {
		this.noCompressionAge = noCompressionAge;
	}

	public int getLightCompressionAge() {
		return lightCompressionAge;
	}

	public void setLightCompressionAge(int lightCompressionAge) {
		this.lightCompressionAge = lightCompressionAge;
	}

	public int getMediumCompressionAge() {
		return mediumCompressionAge;
	}

	public void setMediumCompressionAge(int mediumCompressionAge) {
		this.mediumCompressionAge = mediumCompressionAge;
	}

	public int getMediumHeavyCompressionAge() {
		return mediumHeavyCompressionAge;
	}

	public void setMediumHeavyCompressionAge(int mediumHeavyCompressionAge) {
		this.mediumHeavyCompressionAge = mediumHeavyCompressionAge;
	}

	public int getHeavyCompressionAge() {
		return heavyCompressionAge;
	}

	public void setHeavyCompressionAge(int heavyCompressionAge) {
		this.heavyCompressionAge = heavyCompressionAge;
	}

}
