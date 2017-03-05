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

public class SorterTemplateModel {
	public final static String TEMPLATE_NAME = "sorter.ftlh";

	private final static Integer[] POSSIBLE_FIXEDBYTE_OPERATORS = {8,4,2,1};

	private final HashMap<Integer,String> byteOperatorMapping;
	private final TypeComparator typeComparator;
	private final ArrayList<Integer> chunkSizes;
	private final String sorterName;
	private final int numBytes;
	private final boolean isKeyFullyDetermined;

	public SorterTemplateModel(TypeComparator typeComparator){
		this.typeComparator = typeComparator;


		if (this.typeComparator.supportsNormalizedKey()) {
			// compute the max normalized key length
			int numPartialKeys;
			try {
				numPartialKeys = this.typeComparator.getFlatComparators().length;
			} catch (Throwable t) {
				numPartialKeys = 1;
			}

			int maxLen = Math.min( NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN, NormalizedKeySorter.MAX_NORMALIZED_KEY_LEN_PER_ELEMENT * numPartialKeys);

			this.numBytes = Math.min(this.typeComparator.getNormalizeKeyLen(), maxLen);
			this.isKeyFullyDetermined = !this.typeComparator.isNormalizedKeyPrefixOnly(this.numBytes);
		}
		else {
			this.numBytes = 0;
			this.isKeyFullyDetermined = false;
		}

		this.chunkSizes = generatedSequenceFixedByteOperators(this.numBytes);

		this.byteOperatorMapping = new HashMap<>();

		this.byteOperatorMapping.put(8, "Long");
		this.byteOperatorMapping.put(4, "Int");
		this.byteOperatorMapping.put(2, "Short");
		this.byteOperatorMapping.put(1, "Byte");

		this.sorterName = generateCodeFilename();

	}

	public String generateCodeFilename() {

		StringBuilder name = new StringBuilder();

		for( Integer opt : chunkSizes ) {
			name.append(byteOperatorMapping.get(opt));
		}

		if(this.isKeyFullyDetermined){
			name.append("FullyDeterminedKey");
		} else {
			name.append("NonFullyDeterminedKey");
		}

		name.append("Sorter");

		return name.toString();
	}

	public Map<String,String> getTemplateVariables() {

		Map<String,String> templateVariables = new HashMap();

		templateVariables.put("name", this.sorterName);

		// generate swap function string
		String swapProcedures  = generateSwapProcedures();
		String writeProcedures = generateWriteProcedures();
		String compareProcedures = generateCompareProcedures();

		templateVariables.put("writeProcedures", writeProcedures);
		templateVariables.put("swapProcedures", swapProcedures);
		templateVariables.put("compareProcedures", compareProcedures);


		return templateVariables;
	}

	private ArrayList<Integer> generatedSequenceFixedByteOperators(int numberBytes){
		ArrayList<Integer> operators = new ArrayList<>();
		if( numberBytes > NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN ) {
			return operators;
		}

		// also include offset
		numberBytes += NormalizedKeySorter.OFFSET_LEN;

		// greedy checking index
		int i = 0;
		while( numberBytes > 0 ) {
			int bytes = POSSIBLE_FIXEDBYTE_OPERATORS[i];
			if( bytes <= numberBytes ) {
				operators.add(bytes);
				numberBytes -= bytes;
			} else {
				i++;
			}
		}
		return operators;
	}

	public ArrayList<Integer> getBytesOperators() {
		return chunkSizes;
	}

	public String getSorterName (){
		return this.sorterName;
	}

	private String generateSwapProcedures(){
		String procedures = "";

		if( this.chunkSizes.size() > 0 ) {
			StringBuilder temporaryString 	  = new StringBuilder();
			StringBuilder firstSegmentString  = new StringBuilder();
			StringBuilder secondSegmentString = new StringBuilder();

			int accOffset = 0;
			for( int i = 0; i  < chunkSizes.size(); i++ ){
				int numberByte = chunkSizes.get(i);
				int varIndex  = i+1;

				String primitiveClass = byteOperatorMapping.get(numberByte);
				String primitiveType  = primitiveClass.toLowerCase();

				String offsetString = "";
				if( i > 0 ) {
					accOffset += chunkSizes.get(i-1);
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

	private String generateWriteProcedures(){
		StringBuilder procedures = new StringBuilder();
		// skip first operator for prefix
		if( chunkSizes.size() > 1 && ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ) {
			int offset = 0;
			for( int i = 1; i < chunkSizes.size(); i++ ){
				int noBytes = chunkSizes.get(i);
				if( noBytes == 1 ){
					break;
				}
				String primitiveClass = byteOperatorMapping.get(noBytes);
				String primitiveType  = primitiveClass.toLowerCase();

				offset += chunkSizes.get(i-1);

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

	private String generateCompareProcedures(){
		StringBuilder procedures = new StringBuilder();

		// skip first operator for prefix
		if( chunkSizes.size() > 1 && ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ) {
			String sortOrder = "";
			if(this.typeComparator.invertNormalizedKey()){
				sortOrder = "-";
			}

			int offset = 0;
			for (int i = 1; i < chunkSizes.size(); i++) {

				offset += chunkSizes.get(i-1);
				String primitiveClass = byteOperatorMapping.get(chunkSizes.get(i));
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

		// order can be determined by key
		if( this.isKeyFullyDetermined ){
			procedures.append("return 0;\n");
		} else {
			procedures.append("final long pointerI = segI.getLong(iBufferOffset) & POINTER_MASK;\n");
			procedures.append("final long pointerJ = segJ.getLong(jBufferOffset) & POINTER_MASK;\n");
			procedures.append("return compareRecords(pointerI, pointerJ);\n");
		}

		return procedures.toString();
	}
}
