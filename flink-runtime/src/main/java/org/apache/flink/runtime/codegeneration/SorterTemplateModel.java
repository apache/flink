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

package org.apache.flink.runtime.codegeneration;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SorterTemplateModel} is a class that implements code generation logic for a given
 * {@link TypeComparator}.
 *
 * <p>The swap and compare methods in {@link NormalizedKeySorter} work on a sequence of bytes.
 * We speed up these operations by splitting this sequence of bytes into chunks that can
 * be handled by primitive operations such as Integer and Long operations.</p>
 */
class SorterTemplateModel {

	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------

	static final String TEMPLATE_NAME = "sorter.ftl";

	/** We don't split to chunks above this size. */
	private static final int SPLITTING_THRESHOLD = 32;

	/**
	 * POSSIBLE_CHUNK_SIZES must be in descending order,
	 * because methods that using it are using greedy approach.
	 */
	private static final Integer[] POSSIBLE_CHUNK_SIZES = {8, 4, 2, 1};

	/** Mapping from chunk sizes to primitive operators. */
	private static final HashMap<Integer, String> byteOperatorMapping = new HashMap<Integer, String>(){
		{
			put(8, "Long");
			put(4, "Int");
			put(2, "Short");
			put(1, "Byte");
		}
	};

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------

	private final TypeComparator typeComparator;

	/**
	 * Sizes of the chunks. Empty, if we are not splitting to chunks. (See calculateChunks())
	 */
	private final ArrayList<Integer> primitiveChunks;

	private final String sorterName;

	/**
	 * Shows whether the order of records can be completely determined by the normalized
	 * sorting key, or the sorter has to also deserialize records if their keys are equal to
	 * really confirm the order.
	 */
	private final boolean normalizedKeyFullyDetermines;

	/**
	 * Constructor.
	 * @param typeComparator
	 * 		  The type information of underlying data
	 */
	SorterTemplateModel(TypeComparator typeComparator){
		this.typeComparator = typeComparator;

		// number of bytes of the sorting key
		int numKeyBytes;

		// compute no. bytes for sorting records and check whether these bytes are just a prefix or not.
		if (this.typeComparator.supportsNormalizedKey()) {
			// compute the max normalized key length
			int numPartialKeys;
			try {
				numPartialKeys = this.typeComparator.getFlatComparators().length;
			} catch (Throwable t) {
				numPartialKeys = 1;
			}

			int maxLen = Math.min(NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN, NormalizedKeySorter.MAX_NORMALIZED_KEY_LEN_PER_ELEMENT * numPartialKeys);

			numKeyBytes = Math.min(this.typeComparator.getNormalizeKeyLen(), maxLen);
			this.normalizedKeyFullyDetermines = !this.typeComparator.isNormalizedKeyPrefixOnly(numKeyBytes);
		}
		else {
			numKeyBytes = 0;
			this.normalizedKeyFullyDetermines = false;
		}

		this.primitiveChunks = calculateChunks(numKeyBytes);

		this.sorterName = generateCodeFilename(this.primitiveChunks, this.normalizedKeyFullyDetermines);
	}

	// ------------------------------------------------------------------------
	//                               Public Methods
	// ------------------------------------------------------------------------

	/**
	 * Generate suitable sequence of operators for creating custom NormalizedKeySorter.
	 * @return map of procedures and corresponding code
	 */
	Map<String, String> getTemplateVariables() {

		Map<String, String> templateVariables = new HashMap<>();

		templateVariables.put("name", this.sorterName);

		String swapProcedures  	 = generateSwapProcedures();
		String writeProcedures   = generateWriteProcedures();
		String compareProcedures = generateCompareProcedures();

		templateVariables.put("writeProcedures", writeProcedures);
		templateVariables.put("swapProcedures", swapProcedures);
		templateVariables.put("compareProcedures", compareProcedures);

		return templateVariables;
	}

	/**
	 * Getter for sorterName (generated in the constructor).
	 * @return name of the sorter
	 */
	String getSorterName(){
		return this.sorterName;
	}

	// ------------------------------------------------------------------------
	//                               Protected Methods
	// ------------------------------------------------------------------------

	/**
	 * Getter for primitiveChunks.
	 * this method is for testing purposes
	 */
	ArrayList<Integer> getPrimitiveChunks(){
		return primitiveChunks;
	}

	// ------------------------------------------------------------------------
	//                               Private Methods
	// ------------------------------------------------------------------------

	/**
	 *  Given no. of bytes, break it into chunks that can be handled by
	 *  primitive operations (e.g., integer or long operations)
	 *  @return ArrayList of chunk sizes
	 */
	private ArrayList<Integer> calculateChunks(int numKeyBytes){
		ArrayList<Integer> chunks = new ArrayList<>();

		// if no. of bytes is too large, we don't split
		if (numKeyBytes > SPLITTING_THRESHOLD) {
			return chunks;
		}

		// also include the offset because of the pointer
		numKeyBytes += NormalizedKeySorter.OFFSET_LEN;

		// greedy finding of chunk sizes
		int i = 0;
		while (numKeyBytes > 0) {
			int bytes = POSSIBLE_CHUNK_SIZES[i];
			if (bytes <= numKeyBytes) {
				chunks.add(bytes);
				numKeyBytes -= bytes;
			} else {
				i++;
			}
		}

		// generateCompareProcedures and generateWriteProcedures skip the
		// first 8 bytes, because it contains the pointer.
		// They do this by skipping the first entry of primitiveChunks, because that
		// should always be 8 in this case.
		if (!(NormalizedKeySorter.OFFSET_LEN == 8 && chunks.get(0).equals(8))) {
			throw new RuntimeException("Bug: Incorrect OFFSET_LEN or primitiveChunks");
		}

		return chunks;
	}

	/**
	 * Based on primitiveChunks variable, generate the most suitable operators
	 * for swapping function.
	 *
	 * @return code used in the swap method
	 */
	private String generateSwapProcedures(){
		/* Example generated code, for 20 bytes (8+8+4):

		long temp1 = segI.getLong(segmentOffsetI);
		long temp2 = segI.getLong(segmentOffsetI+8);
		int temp3 = segI.getInt(segmentOffsetI+16);

		segI.putLong(segmentOffsetI, segJ.getLong(segmentOffsetJ));
		segI.putLong(segmentOffsetI+8, segJ.getLong(segmentOffsetJ+8));
		segI.putInt(segmentOffsetI+16, segJ.getInt(segmentOffsetJ+16));

		segJ.putLong(segmentOffsetJ, temp1);
		segJ.putLong(segmentOffsetJ+8, temp2);
		segJ.putInt(segmentOffsetJ+16, temp3);
		 */

		String procedures = "";

		if (this.primitiveChunks.size() > 0) {
			StringBuilder temporaryString 	  = new StringBuilder();
			StringBuilder firstSegmentString  = new StringBuilder();
			StringBuilder secondSegmentString = new StringBuilder();

			int accOffset = 0;
			for (int i = 0; i  < primitiveChunks.size(); i++){
				int numberByte = primitiveChunks.get(i);
				int varIndex = i + 1;

				String primitiveClass = byteOperatorMapping.get(numberByte);
				String primitiveType  = primitiveClass.toLowerCase();

				String offsetString = "";
				if (i > 0) {
					accOffset += primitiveChunks.get(i - 1);
					offsetString = "+" + accOffset;
				}

				temporaryString.append(String.format("%s temp%d = segI.get%s(segmentOffsetI%s);\n",
					primitiveType, varIndex, primitiveClass, offsetString));

				firstSegmentString.append(String.format("segI.put%s(segmentOffsetI%s, segJ.get%s(segmentOffsetJ%s));\n",
					primitiveClass, offsetString, primitiveClass, offsetString));

				secondSegmentString.append(String.format("segJ.put%s(segmentOffsetJ%s, temp%d);\n",
					primitiveClass, offsetString, varIndex));

			}

			procedures = temporaryString.toString()
				+ "\n" + firstSegmentString.toString()
				+ "\n" + secondSegmentString.toString();
		} else {
			procedures = "segI.swapBytes(this.swapBuffer, segJ, segmentOffsetI, segmentOffsetJ, this.indexEntrySize);";
		}

		return procedures;
	}

	/**
	 * Based on primitiveChunks variable, generate reverse byte operators for little endian machine
	 * for writing a record to MemorySegment, such that later during comparison
	 * we can directly use native byte order to do unsigned comparison.
	 *
	 * @return code used in the write method
	 */
	private String generateWriteProcedures(){
		/* Example generated code, for 12 bytes (8+4):

		long temp1 = Long.reverseBytes(this.currentSortIndexSegment.getLong(this.currentSortIndexOffset+8));
		this.currentSortIndexSegment.putLong(this.currentSortIndexOffset + 8, temp1);
		int temp2 = Integer.reverseBytes(this.currentSortIndexSegment.getInt(this.currentSortIndexOffset+16));
		this.currentSortIndexSegment.putInt(this.currentSortIndexOffset + 16, temp2);
		 */

		StringBuilder procedures = new StringBuilder();
		// skip the first chunk, which is the pointer before the key
		if (primitiveChunks.size() > 1 && ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
			int offset = 0;
			// starts from 1 because of skipping the first chunk
			for (int i = 1; i < primitiveChunks.size(); i++){
				int noBytes = primitiveChunks.get(i);
				if (noBytes == 1){
					/* 1-byte chunk doesn't need to be reversed and it always comes last,
					 * so we can finish the loop early here
					 */
					break;
				}
				String primitiveClass = byteOperatorMapping.get(noBytes);
				String primitiveType  = primitiveClass.toLowerCase();

				offset += primitiveChunks.get(i - 1);

				String reverseBytesMethod = primitiveClass;
				if (primitiveClass.equals("Int")){
					reverseBytesMethod = "Integer";
				}

				procedures.append(String.format("%s temp%d = %s.reverseBytes(this.currentSortIndexSegment.get%s(this.currentSortIndexOffset+%d));\n",
					primitiveType, i, reverseBytesMethod, primitiveClass, offset
				));

				procedures.append(String.format("this.currentSortIndexSegment.put%s(this.currentSortIndexOffset + %d, temp%d);\n",
					primitiveClass, offset, i
				));

			}
		}
		return procedures.toString();
	}

	/**
	 * Based on primitiveChunks, generate the most suitable operators
	 * for comparing two records. Note that we generate procedures that use
	 * native byte-order here, regardless of the endianness of the
	 * machine, as we compensate the order already during writing the key.
	 *
	 * @return code used in the compare method
	 */
	private String generateCompareProcedures(){
		/* Example generated code for 12 bytes (8+4):

		long l_1_1  = segI.getLong(segmentOffsetI + 8);
		long l_1_2  = segJ.getLong(segmentOffsetJ + 8);
		if( l_1_1 != l_1_2 ) {
		  return ((l_1_1 < l_1_2) ^ ( l_1_1 < 0 ) ^ ( l_1_2 < 0 ) ? -1 : 1);
		}

		int l_2_1  = segI.getInt(segmentOffsetI + 16);
		int l_2_2  = segJ.getInt(segmentOffsetJ + 16);
		if( l_2_1 != l_2_2 ) {
		  return ((l_2_1 < l_2_2) ^ ( l_2_1 < 0 ) ^ ( l_2_2 < 0 ) ? -1 : 1);
		}

		return 0;
		 */

		StringBuilder procedures = new StringBuilder();

		// skip the first chunk, which is the pointer before the key
		if (primitiveChunks.size() > 1){
			String sortOrder = "";
			if (this.typeComparator.invertNormalizedKey()){
				sortOrder = "-";
			}

			int offset = 0;

			// starts from 1 because of skipping the first chunk
			for (int i = 1; i < primitiveChunks.size(); i++){

				offset += primitiveChunks.get(i - 1);
				String primitiveClass = byteOperatorMapping.get(primitiveChunks.get(i));
				String primitiveType  = primitiveClass.toLowerCase();

				String var1 = "l_" + i + "_1";
				String var2 = "l_" + i + "_2";
				procedures.append(String.format("%s %s  = segI.get%s(segmentOffsetI + %d);\n",
					primitiveType, var1, primitiveClass, offset));
				procedures.append(String.format("%s %s  = segJ.get%s(segmentOffsetJ + %d);\n",
					primitiveType, var2, primitiveClass, offset));

				procedures.append(String.format("if( %s != %s ) {\n", var1, var2));
				procedures.append(String.format("  return %s((%s < %s) ^ ( %s < 0 ) ^ ( %s < 0 ) ? -1 : 1);\n",
					sortOrder, var1, var2, var1, var2));
				procedures.append("}\n\n");

			}
		}

		if (this.normalizedKeyFullyDetermines){
			// don't need to compare records further for fully determined key
			procedures.append("return 0;\n");
		} else {
			procedures.append("final long pointerI = segI.getLong(segmentOffsetI) & POINTER_MASK;\n");
			procedures.append("final long pointerJ = segJ.getLong(segmentOffsetJ) & POINTER_MASK;\n");
			procedures.append("return compareRecords(pointerI, pointerJ);\n");
		}

		return procedures.toString();
	}

	/**
	 * Generate name of the sorter based on the chunk sizes and determinant of the key.
	 * @param chunks array of chunk sizes
	 * @return a suitable name of sorter for particular type-comparator
	 */
	private String generateCodeFilename(ArrayList<Integer> chunks, boolean normalizedKeyFullyDetermines) {

		StringBuilder name = new StringBuilder();

		for (Integer opt : chunks){
			name.append(byteOperatorMapping.get(opt));
		}

		if (typeComparator.invertNormalizedKey()){
			name.append("_Desc_");
		} else {
			name.append("_Asc_");
		}

		if (normalizedKeyFullyDetermines){
			name.append("FullyDetermining_");
		} else {
			name.append("NonFullyDetermining_");
		}

		name.append("NormalizedKeySorter");

		return name.toString();
	}
}
