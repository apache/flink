/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler.config;

import eu.stratosphere.nephele.configuration.Configuration;

public class ResourceConfig {

	public static final String NUM_SORT_BUFFER_DEFAULT = "pact.compiler.num.sort.buffer.default";

	public static final String NUM_SORT_BUFFER_REDUCE = "pact.compiler.num.sort.buffer.reduce";

	public static final String NUM_SORT_BUFFER_COMBINE = "pact.compiler.num.sort.buffer.combine";

	public static final String NUM_SORT_BUFFER_MATCH = "pact.compiler.num.sort.buffer.match";

	public static final String NUM_SORT_BUFFER_COGROUP = "pact.compiler.num.sort.buffer.cogroup";

	public static final String SIZE_SORT_BUFFER_DEFAULT = "pact.compiler.size.sort.buffer.default";

	public static final String SIZE_SORT_BUFFER_REDUCE = "pact.compiler.size.sort.buffer.reduce";

	public static final String SIZE_SORT_BUFFER_COMBINE = "pact.compiler.size.sort.buffer.combine";

	public static final String SIZE_SORT_BUFFER_MATCH = "pact.compiler.size.sort.buffer.match";

	public static final String SIZE_SORT_BUFFER_COGROUP = "pact.compiler.size.sort.buffer.cogroup";

	public static final String SIZE_IO_BUFFER_DEFAULT = "pact.compiler.size.io.buffer.default";

	public static final String SIZE_IO_BUFFER_REDUCE = "pact.compiler.size.io.buffer.reduce";

	public static final String SIZE_IO_BUFFER_COMBINE = "pact.compiler.size.io.buffer.combine";

	public static final String SIZE_IO_BUFFER_MATCH = "pact.compiler.size.io.buffer.match";

	public static final String SIZE_IO_BUFFER_COGROUP = "pact.compiler.size.io.buffer.cogroup";

	public static final String SIZE_IO_BUFFER_CROSS = "pact.compiler.size.io.buffer.cross";

	public static final String SIZE_IO_BUFFER_TEMP = "pact.compiler.size.io.buffer.temp";

	public static final String MERGE_FACTOR_DEFAULT = "pact.compiler.merge.factor.default";

	public static final String MERGE_FACTOR_REDUCE = "pact.compiler.merge.factor.reduce";

	public static final String MERGE_FACTOR_COMBINE = "pact.compiler.merge.factor.combine";

	public static final String MERGE_FACTOR_MATCH = "pact.compiler.merge.factor.match";

	public static final String MERGE_FACTOR_COGROUP = "pact.compiler.merge.factor.cogroup";

	private int numSortBuffer_Reduce;

	private int sortBufferSize_Reduce;

	private int ioBufferSize_Reduce;

	private int mergeFactor_Reduce;

	private int numSortBuffer_Combine;

	private int sortBufferSize_Combine;

	private int ioBufferSize_Combine;

	private int mergeFactor_Combine;

	private int numSortBuffer_Match;

	private int sortBufferSize_Match;

	private int ioBufferSize_Match;

	private int mergeFactor_Match;

	private int numSortBuffer_CoGroup;

	private int sortBufferSize_CoGroup;

	private int ioBufferSize_CoGroup;

	private int mergeFactor_CoGroup;

	private int ioBufferSize_Cross;

	private int ioBufferSize_Temp;

	public ResourceConfig(Configuration config) {
		this.setNumSortBuffer_Default(config.getInteger(NUM_SORT_BUFFER_DEFAULT, -1));
		this.setNumSortBuffer_Reduce(config.getInteger(NUM_SORT_BUFFER_REDUCE, -1));
		this.setNumSortBuffer_Combine(config.getInteger(NUM_SORT_BUFFER_COMBINE, -1));
		this.setNumSortBuffer_Match(config.getInteger(NUM_SORT_BUFFER_MATCH, -1));
		this.setNumSortBuffer_CoGroup(config.getInteger(NUM_SORT_BUFFER_COGROUP, -1));

		this.setSortBufferSize_Default(config.getInteger(SIZE_SORT_BUFFER_DEFAULT, -1));
		this.setSortBufferSize_Reduce(config.getInteger(SIZE_SORT_BUFFER_REDUCE, -1));
		this.setSortBufferSize_Combine(config.getInteger(SIZE_SORT_BUFFER_COMBINE, -1));
		this.setSortBufferSize_Match(config.getInteger(SIZE_SORT_BUFFER_MATCH, -1));
		this.setSortBufferSize_CoGroup(config.getInteger(SIZE_SORT_BUFFER_COGROUP, -1));

		this.setIOBufferSize_Default(config.getInteger(SIZE_IO_BUFFER_DEFAULT, -1));
		this.setIOBufferSize_Reduce(config.getInteger(SIZE_IO_BUFFER_REDUCE, -1));
		this.setIOBufferSize_Combine(config.getInteger(SIZE_IO_BUFFER_COMBINE, -1));
		this.setIOBufferSize_Match(config.getInteger(SIZE_IO_BUFFER_MATCH, -1));
		this.setIOBufferSize_CoGroup(config.getInteger(SIZE_IO_BUFFER_COGROUP, -1));
		this.setIOBufferSize_Cross(config.getInteger(SIZE_IO_BUFFER_CROSS, -1));
		this.setIOBufferSize_Temp(config.getInteger(SIZE_IO_BUFFER_TEMP, -1));

		this.setMergeFactor_Default(config.getInteger(MERGE_FACTOR_DEFAULT, -1));
		this.setMergeFactor_Reduce(config.getInteger(MERGE_FACTOR_REDUCE, -1));
		this.setMergeFactor_Combine(config.getInteger(MERGE_FACTOR_COMBINE, -1));
		this.setMergeFactor_Match(config.getInteger(MERGE_FACTOR_MATCH, -1));
		this.setMergeFactor_CoGroup(config.getInteger(MERGE_FACTOR_COGROUP, -1));
	}

	public ResourceConfig() {
		this.setNumSortBuffer_Default(-1);
		this.setSortBufferSize_Default(-1);
		this.setIOBufferSize_Default(-1);
		this.setMergeFactor_Default(-1);
	}

	public void setNumSortBuffer_Default(int numSortBuffer) {
		this.numSortBuffer_Reduce = numSortBuffer;
		this.numSortBuffer_Combine = numSortBuffer;
		this.numSortBuffer_Match = numSortBuffer;
		this.numSortBuffer_CoGroup = numSortBuffer;
	}

	public void setSortBufferSize_Default(int sortBufferSize) {
		this.sortBufferSize_Reduce = sortBufferSize;
		this.sortBufferSize_Combine = sortBufferSize;
		this.sortBufferSize_Match = sortBufferSize;
		this.sortBufferSize_CoGroup = sortBufferSize;
	}

	public void setIOBufferSize_Default(int ioBufferSize) {
		this.ioBufferSize_Reduce = ioBufferSize;
		this.ioBufferSize_Combine = ioBufferSize;
		this.ioBufferSize_Match = ioBufferSize;
		this.ioBufferSize_CoGroup = ioBufferSize;
		this.ioBufferSize_Cross = ioBufferSize;
		this.ioBufferSize_Temp = ioBufferSize;
	}

	public void setMergeFactor_Default(int mergeFactor) {
		this.mergeFactor_Reduce = mergeFactor;
		this.mergeFactor_Combine = mergeFactor;
		this.mergeFactor_Match = mergeFactor;
		this.mergeFactor_CoGroup = mergeFactor;
	}

	public int getNumSortBuffer_Reduce() {
		return numSortBuffer_Reduce;
	}

	public void setNumSortBuffer_Reduce(int numSortBufferReduce) {
		numSortBuffer_Reduce = numSortBufferReduce;
	}

	public int getSortBufferSize_Reduce() {
		return sortBufferSize_Reduce;
	}

	public void setSortBufferSize_Reduce(int sortBufferSizeReduce) {
		sortBufferSize_Reduce = sortBufferSizeReduce;
	}

	public int getIOBufferSize_Reduce() {
		return ioBufferSize_Reduce;
	}

	public void setIOBufferSize_Reduce(int ioBufferSizeReduce) {
		ioBufferSize_Reduce = ioBufferSizeReduce;
	}

	public int getMergeFactor_Reduce() {
		return mergeFactor_Reduce;
	}

	public void setMergeFactor_Reduce(int mergeFactorReduce) {
		mergeFactor_Reduce = mergeFactorReduce;
	}

	public int getNumSortBuffer_Combine() {
		return numSortBuffer_Combine;
	}

	public void setNumSortBuffer_Combine(int numSortBufferCombine) {
		numSortBuffer_Combine = numSortBufferCombine;
	}

	public int getSortBufferSize_Combine() {
		return sortBufferSize_Combine;
	}

	public void setSortBufferSize_Combine(int sortBufferSizeCombine) {
		sortBufferSize_Combine = sortBufferSizeCombine;
	}

	public int getIOBufferSize_Combine() {
		return ioBufferSize_Combine;
	}

	public void setIOBufferSize_Combine(int ioBufferSizeCombine) {
		ioBufferSize_Combine = ioBufferSizeCombine;
	}

	public int getMergeFactor_Combine() {
		return mergeFactor_Combine;
	}

	public void setMergeFactor_Combine(int mergeFactorCombine) {
		mergeFactor_Combine = mergeFactorCombine;
	}

	public int getNumSortBuffer_Match() {
		return numSortBuffer_Match;
	}

	public void setNumSortBuffer_Match(int numSortBufferMatch) {
		numSortBuffer_Match = numSortBufferMatch;
	}

	public int getSortBufferSize_Match() {
		return sortBufferSize_Match;
	}

	public void setSortBufferSize_Match(int sortBufferSizeMatch) {
		sortBufferSize_Match = sortBufferSizeMatch;
	}

	public int getIOBufferSize_Match() {
		return ioBufferSize_Match;
	}

	public void setIOBufferSize_Match(int ioBufferSizeMatch) {
		ioBufferSize_Match = ioBufferSizeMatch;
	}

	public int getMergeFactor_Match() {
		return mergeFactor_Match;
	}

	public void setMergeFactor_Match(int mergeFactorMatch) {
		mergeFactor_Match = mergeFactorMatch;
	}

	public int getNumSortBuffer_CoGroup() {
		return numSortBuffer_CoGroup;
	}

	public void setNumSortBuffer_CoGroup(int numSortBufferCoGroup) {
		numSortBuffer_CoGroup = numSortBufferCoGroup;
	}

	public int getSortBufferSize_CoGroup() {
		return sortBufferSize_CoGroup;
	}

	public void setSortBufferSize_CoGroup(int sortBufferSizeCoGroup) {
		sortBufferSize_CoGroup = sortBufferSizeCoGroup;
	}

	public int getIOBufferSize_CoGroup() {
		return ioBufferSize_CoGroup;
	}

	public void setIOBufferSize_CoGroup(int ioBufferSizeCoGroup) {
		ioBufferSize_CoGroup = ioBufferSizeCoGroup;
	}

	public int getMergeFactor_CoGroup() {
		return mergeFactor_CoGroup;
	}

	public void setMergeFactor_CoGroup(int mergeFactorCoGroup) {
		mergeFactor_CoGroup = mergeFactorCoGroup;
	}

	public int getIOBufferSize_Cross() {
		return ioBufferSize_Cross;
	}

	public void setIOBufferSize_Cross(int ioBufferSizeCross) {
		ioBufferSize_Cross = ioBufferSizeCross;
	}

	public int getIOBufferSize_Temp() {
		return ioBufferSize_Temp;
	}

	public void setIOBufferSize_Temp(int ioBufferSizeTemp) {
		ioBufferSize_Temp = ioBufferSizeTemp;
	}

}
