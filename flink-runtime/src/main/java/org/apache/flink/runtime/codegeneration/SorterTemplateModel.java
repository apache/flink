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
 * {@link SorterTemplateModel} is a class that implements code generation logic for given {@link TypeComparator}
 */
public class SorterTemplateModel {

	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------

	public final static String TEMPLATE_NAME = "sorter.ftlh";

	/* POSSIBLE_FIXEDBYTE_OPERATORS must be in descending order,
	 * because methods that using it are using greedy approach.
	 */
	private final static Integer[] POSSIBLE_FIXEDBYTE_OPERATORS = {8,4,2,1};

	// mapping between fixed-byte chunk to fixed-byte operator
	private final static HashMap<Integer,String> byteOperatorMapping = new HashMap<Integer, String>(){
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
	private final ArrayList<Integer> fixedByteChunks;
	private final String sorterName;

	// no. bytes of a sorting key
	private final int numKeyBytes;

	// used to determine whether order of records can completely determined by normalized sorting key
	// or the sorter has to also deserialize records if their keys are equal to really confirm the order
	private final boolean isKeyFullyDetermined;

	/**
	 * Constructor
	 * @param typeComparator
	 * 		  The type information of underlying data
	 */
	public SorterTemplateModel(TypeComparator typeComparator){
		this.typeComparator = typeComparator;

		// compute no. bytes for sorting records and check whether this bytes is just prefix or not.
		if (this.typeComparator.supportsNormalizedKey()) {
			// compute the max normalized key length
			int numPartialKeys;
			try {
				numPartialKeys = this.typeComparator.getFlatComparators().length;
			} catch (Throwable t) {
				numPartialKeys = 1;
			}

			int maxLen = Math.min( NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN, NormalizedKeySorter.MAX_NORMALIZED_KEY_LEN_PER_ELEMENT * numPartialKeys);

			this.numKeyBytes = Math.min(this.typeComparator.getNormalizeKeyLen(), maxLen);
			this.isKeyFullyDetermined = !this.typeComparator.isNormalizedKeyPrefixOnly(this.numKeyBytes);
		}
		else {
			this.numKeyBytes = 0;
			this.isKeyFullyDetermined = false;
		}

		// split key into fixed-byte chunks
		this.fixedByteChunks = generatedSequenceFixedByteChunks(this.numKeyBytes);

		this.sorterName   = generateCodeFilename(this.fixedByteChunks, this.isKeyFullyDetermined);
	}

	// ------------------------------------------------------------------------
	//                               Public Methods
	// ------------------------------------------------------------------------

	/**
	 * Generate suitable sequence of operators for creating custom NormalizedKeySorter
	 * @return map of procedures and corresponding code
	 */
	public Map<String,String> getTemplateVariables() {

		Map<String,String> templateVariables = new HashMap();

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
	 * Getter for sorterName which instantiated from constructor
	 * @return name of the sorter
	 */
	public String getSorterName (){
		return this.sorterName;
	}

	// ------------------------------------------------------------------------
	//                               Protected Methods
	// ------------------------------------------------------------------------

	/**
	 * Getter for fixedByteChunks
	 * this method is made for testing proposed
	 */
	protected ArrayList<Integer> getBytesOperators() {
		return fixedByteChunks;
	}

	// ------------------------------------------------------------------------
	//                               Private Methods
	// ------------------------------------------------------------------------

	/**
	 *  Given no. of bytes, break it into fixed-byte chunks
	 *  @return array of fixed-byte chunks
	 */
	private ArrayList<Integer> generatedSequenceFixedByteChunks(int numKeyBytes){
		ArrayList<Integer> chunks = new ArrayList<>();

		// if no. of bytes is too large, we don't split
		if( numKeyBytes > NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN ) {
			return chunks;
		}

		// also include offset
		numKeyBytes += NormalizedKeySorter.OFFSET_LEN;

		// greedy finding fixed-byte operators
		int i = 0;
		while( numKeyBytes > 0 ) {
			int bytes = POSSIBLE_FIXEDBYTE_OPERATORS[i];
			if( bytes <= numKeyBytes ) {
				chunks.add(bytes);
				numKeyBytes -= bytes;
			} else {
				i++;
			}
		}
		return chunks;
	}

	/**
	 * Based on fixedByteChunks variable, generate the most suitable operators
	 * for swapping function
	 * @return string of a sequence operators used in swapping process
	 */
	private String generateSwapProcedures(){
		String procedures = "";

		if( this.fixedByteChunks.size() > 0 ) {
			StringBuilder temporaryString 	  = new StringBuilder();
			StringBuilder firstSegmentString  = new StringBuilder();
			StringBuilder secondSegmentString = new StringBuilder();

			int accOffset = 0;
			for( int i = 0; i  < fixedByteChunks.size(); i++ ){
				int numberByte = fixedByteChunks.get(i);
				int varIndex  = i+1;

				String primitiveClass = byteOperatorMapping.get(numberByte);
				String primitiveType  = primitiveClass.toLowerCase();

				String offsetString = "";
				if( i > 0 ) {
					accOffset += fixedByteChunks.get(i-1);
					offsetString = "+" + accOffset;
				}

				temporaryString.append(String.format("%s temp%d = segI.get%s(iBufferOffset%s);\n",
					primitiveType, varIndex, primitiveClass, offsetString ));

				firstSegmentString.append(String.format("segI.put%s(iBufferOffset%s, segJ.get%s(jBufferOffset%s));\n",
					primitiveClass, offsetString, primitiveClass, offsetString));

				secondSegmentString.append(String.format("segJ.put%s(jBufferOffset%s, temp%d);\n",
					primitiveClass, offsetString, varIndex));

			}

			procedures = temporaryString.toString()
				+ "\n" + firstSegmentString.toString()
				+ "\n" + secondSegmentString.toString();
		} else {
			procedures = "segI.swapBytes(this.swapBuffer, segJ, iBufferOffset, jBufferOffset, this.indexEntrySize);";
		}

		return procedures;
	}

	/**
	 * Based on fixedByteChunks variable, generate reverse byte operators for little endian machine
	 * for writing a record to MemorySegment, such that later during comparison
	 * we can directly use native byte order to do unsigned comparison
	 * @return string of a sequence operators used in writing process
	 */
	private String generateWriteProcedures(){
		StringBuilder procedures = new StringBuilder();
		// skip first operator for prefix
		if( fixedByteChunks.size() > 1 && ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ) {
			int offset = 0;
			for( int i = 1; i < fixedByteChunks.size(); i++ ){
				int noBytes = fixedByteChunks.get(i);
				if( noBytes == 1 ){
					/* 1-byte chunk doesn't need to be reversed and it always comes last,
					 * so we can finish the loop early here
					 */
					break;
				}
				String primitiveClass = byteOperatorMapping.get(noBytes);
				String primitiveType  = primitiveClass.toLowerCase();

				offset += fixedByteChunks.get(i-1);

				String reverseBytesMethod = primitiveClass;
				if( primitiveClass.equals("Int") ) {
					reverseBytesMethod = "Integer";
				}

				procedures.append(String.format("%s temp%d = %s.reverseBytes(this.currentSortIndexSegment.get%s(this.currentSortIndexOffset+%d));\n",
					primitiveType, i, reverseBytesMethod, primitiveClass, offset
				));

				procedures.append(String.format("this.currentSortIndexSegment.put%s( this.currentSortIndexOffset + %d, temp%d);\n",
					primitiveClass, offset, i
				));

			}
		}
		return procedures.toString();
	}

	/**
	 * Based on fixedByteChunks variable, generate the most suitable operators
	 * for comparing 2 records. Nothing that, we are generating procedures that use
	 * native-order byte methods here regardless endianness of the machine as we compensate
	 * the order already in writing process.
	 * @return string of a sequence operators used in comparing processes
	 */
	private String generateCompareProcedures(){
		StringBuilder procedures = new StringBuilder();

		// skip first operator for prefix
		if( fixedByteChunks.size() > 1 ) {
			String sortOrder = "";
			if(this.typeComparator.invertNormalizedKey()){
				sortOrder = "-";
			}

			int offset = 0;
			for (int i = 1; i < fixedByteChunks.size(); i++) {

				offset += fixedByteChunks.get(i-1);
				String primitiveClass = byteOperatorMapping.get(fixedByteChunks.get(i));
				String primitiveType  = primitiveClass.toLowerCase();

				String var1 = "l_"+ i + "_1";
				String var2 = "l_"+ i + "_2";
				procedures.append(String.format("%s %s  = segI.get%s(iBufferOffset + %d);\n",
					primitiveType, var1, primitiveClass, offset));
				procedures.append(String.format("%s %s  = segJ.get%s(jBufferOffset + %d);\n",
					primitiveType, var2, primitiveClass, offset));

				procedures.append(String.format("if( %s != %s ) {\n", var1, var2));
				procedures.append(String.format("return %s(%s < %s) ^ ( %s < 0 ) ^ ( %s < 0 ) ? -1 : 1;\n",
					sortOrder, var1, var2, var1, var2 ));
				procedures.append("}\n\n");

			}
		}

		if( this.isKeyFullyDetermined ){
			// don't need to compare records further for fully determined key
			procedures.append("return 0;\n");
		} else {
			procedures.append("final long pointerI = segI.getLong(iBufferOffset) & POINTER_MASK;\n");
			procedures.append("final long pointerJ = segJ.getLong(jBufferOffset) & POINTER_MASK;\n");
			procedures.append("return compareRecords(pointerI, pointerJ);\n");
		}

		return procedures.toString();
	}

	/**
	 * Generate name of the sorter based on fixed-size chunks and determinant of the key
	 * @param array of fixed-byte chunks
	 * @return a suitable name of sorter for particular type-comparator
	 */
	private String generateCodeFilename(ArrayList<Integer> chunks, boolean isKeyFullyDetermined) {

		StringBuilder name = new StringBuilder();

		for( Integer opt : chunks ) {
			name.append(byteOperatorMapping.get(opt));
		}

		if(isKeyFullyDetermined){
			name.append("FullyDeterminedKey");
		} else {
			name.append("NonFullyDeterminedKey");
		}

		name.append("Sorter");

		return name.toString();
	}
}
