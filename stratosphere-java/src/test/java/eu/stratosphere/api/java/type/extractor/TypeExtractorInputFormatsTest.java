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
package eu.stratosphere.api.java.type.extractor;


import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.common.io.statistics.BaseStatistics;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.ResultTypeQueryable;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.InputSplit;
import eu.stratosphere.types.TypeInformation;

@SuppressWarnings("serial")
public class TypeExtractorInputFormatsTest {

	@Test
	public void testExtractInputFormatType() {
		try {
			InputFormat<?, ?> format = new DummyFloatInputFormat();
			TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
			assertEquals(BasicTypeInfo.FLOAT_TYPE_INFO, typeInfo);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testExtractDerivedInputFormatType() {
		try {
			// simple type
			{
				InputFormat<?, ?> format = new DerivedInputFormat();
				TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
				assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, typeInfo);
			}
			
			// composite type
			{
				InputFormat<?, ?> format = new DerivedTupleInputFormat();
				TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
				
				assertTrue(typeInfo.isTupleType());
				assertTrue(typeInfo instanceof TupleTypeInfo);
				
				@SuppressWarnings("unchecked")
				TupleTypeInfo<Tuple3<String, Short, Double>> tupleInfo = (TupleTypeInfo<Tuple3<String, Short, Double>>) typeInfo;
				
				assertEquals(3, tupleInfo.getArity());
				assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tupleInfo.getTypeAt(0));
				assertEquals(BasicTypeInfo.SHORT_TYPE_INFO, tupleInfo.getTypeAt(1));
				assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tupleInfo.getTypeAt(2));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testMultiLevelDerivedInputFormatType() {
		try {

			// composite type
			{
				InputFormat<?, ?> format = new FinalRelativeInputFormat();
				TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
				
				assertTrue(typeInfo.isTupleType());
				assertTrue(typeInfo instanceof TupleTypeInfo);
				
				@SuppressWarnings("unchecked")
				TupleTypeInfo<Tuple3<String, Integer, Double>> tupleInfo = (TupleTypeInfo<Tuple3<String, Integer, Double>>) typeInfo;
				
				assertEquals(3, tupleInfo.getArity());
				assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tupleInfo.getTypeAt(0));
				assertEquals(BasicTypeInfo.INT_TYPE_INFO, tupleInfo.getTypeAt(1));
				assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tupleInfo.getTypeAt(2));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testQueryableFormatType() {
		try {
			InputFormat<?, ?> format = new QueryableInputFormat();
			TypeInformation<?> typeInfo = TypeExtractor.getInputFormatTypes(format);
			assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, typeInfo);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Test formats
	// --------------------------------------------------------------------------------------------
	
	public static final class DummyFloatInputFormat implements InputFormat<Float, InputSplit> {

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) { return null; }

		@Override
		public InputSplit[] createInputSplits(int minNumSplits) { return null; }

		@Override
		public Class<? extends InputSplit> getInputSplitType() { return null; }

		@Override
		public void open(InputSplit split) {}

		@Override
		public boolean reachedEnd() { return false; }

		@Override
		public Float nextRecord(Float reuse) throws IOException { return null; }

		@Override
		public void close() {}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class DerivedInputFormat extends GenericInputFormat<Short> {

		@Override
		public boolean reachedEnd() { return false; }

		@Override
		public Short nextRecord(Short reuse) { return null; }
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class DerivedTupleInputFormat extends GenericInputFormat<Tuple3<String, Short, Double>> {

		@Override
		public boolean reachedEnd() { return false; }

		@Override
		public Tuple3<String, Short, Double> nextRecord(Tuple3<String, Short, Double> reuse) { return null; }
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class RelativeInputFormat<T> extends GenericInputFormat<Tuple3<String, T, Double>> {

		@Override
		public boolean reachedEnd() { return false; }

		@Override
		public Tuple3<String, T, Double> nextRecord(Tuple3<String, T, Double> reuse) { return null; }
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class FinalRelativeInputFormat extends RelativeInputFormat<Integer> {

		@Override
		public Tuple3<String, Integer, Double> nextRecord(Tuple3<String, Integer, Double> reuse) { return null; }
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static final class QueryableInputFormat implements InputFormat<Float, InputSplit>, ResultTypeQueryable<Double> {

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) { return null; }

		@Override
		public InputSplit[] createInputSplits(int minNumSplits) { return null; }

		@Override
		public Class<? extends InputSplit> getInputSplitType() { return null; }

		@Override
		public void open(InputSplit split) {}

		@Override
		public boolean reachedEnd() { return false; }

		@Override
		public Float nextRecord(Float reuse) throws IOException { return null; }

		@Override
		public void close() {}

		@Override
		public TypeInformation<Double> getProducedType() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}
	}
}
