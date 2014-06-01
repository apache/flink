/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.api.java.functions;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.SingleInputSemanticProperties;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.tuple.Tuple5;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.types.TypeInformation;
import junit.framework.Assert;

import org.junit.Test;

public class SemanticPropUtilTest {
	
	// --------------------------------------------------------------------------------------------
	// Constant Fields
	// --------------------------------------------------------------------------------------------

	@Test
	public void testConstantWithArrowIndividualStrings() {
		// no spaces
		{
			String[] constantFields = { "0->0,1", "1->2" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(0));
			Assert.assertTrue(fs.contains(1));
	
			fs = sp.getForwardedField(1);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
		}
		
		// with spaces
		{
			String[] constantFields = { "0 -> 0 ,   1 ", " 1     -> 2  " };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(0));
			Assert.assertTrue(fs.contains(1));
	
			fs = sp.getForwardedField(1);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
		}
	}
	
	@Test
	public void testConstantWithArrowIndividualStringsSpaces() {
		String[] constantFields = { "  1-> 1 , 2", "2 ->3" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple4<Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);

		FieldSet fs = sp.getForwardedField(1);
		Assert.assertTrue(fs.size() == 2);
		Assert.assertTrue(fs.contains(1));
		Assert.assertTrue(fs.contains(2));

		fs = sp.getForwardedField(2);
		Assert.assertTrue(fs.size() == 1);
		Assert.assertTrue(fs.contains(3));
	}
	
	@Test
	public void testConstantWithArrowOneString() {
		// narrow (no spaces)
		{
			String[] constantFields = { "0->0,1;1->2" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(0));
			Assert.assertTrue(fs.contains(1));
	
			fs = sp.getForwardedField(1);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
		}
		
		// wide (with spaces)
		{
			String[] constantFields = { "  0 ->  0  ,  1  ;   1  -> 2 " };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(0));
			Assert.assertTrue(fs.contains(1));
	
			fs = sp.getForwardedField(1);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
		}
	}
	
	@Test
	public void testConstantNoArrrowIndividualStrings() {
		// no spaces
		{
			String[] constantFields = {"2","3","0"};
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(3);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(3));
	
			Assert.assertNull(sp.getForwardedField(1));
			Assert.assertNull(sp.getForwardedField(4));
		}
		
		// with spaces
		{
			String[] constantFields = {" 2   ","3  ","  0"};
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(3);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(3));
	
			Assert.assertNull(sp.getForwardedField(1));
			Assert.assertNull(sp.getForwardedField(4));
		}
	}
	
	@Test
	public void testConstantNoArrrowOneString() {
		// no spaces
		{
			String[] constantFields = {"2;3;0"};
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(3);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(3));
	
			Assert.assertNull(sp.getForwardedField(1));
			Assert.assertNull(sp.getForwardedField(4));
		}
		
		// no spaces
		{
			String[] constantFields = {"  2  ;   3  ;  0   "};
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(3);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(3));
	
			Assert.assertNull(sp.getForwardedField(1));
			Assert.assertNull(sp.getForwardedField(4));
		}
	}
	
//	@Test
//	public void testConstantNoArrrowOneStringInvalidDelimiter() {
//		String[] constantFields = {"2,3,0"};
//
//		TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
//				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
//		try {
//			SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
//			Assert.fail("Accepted invalid input");
//		}
//		catch (InvalidProgramException e) {
//			// bueno!
//		}
//	}
	
	@Test
	public void testConstantMixedOneString() {
		// no spaces
		{
			String[] constantFields = {"2,3;0->1,4;4->0"};
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(1));
			Assert.assertTrue(fs.contains(4));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(3);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(3));
	
			fs = sp.getForwardedField(4);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
		}
		
		// with spaces
		{
			String[] constantFields = {" 2  ,  3   ;  0 -> 1  , 4 ; 4 ->  0"};
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple5<Integer, Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(1));
			Assert.assertTrue(fs.contains(4));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(3);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(3));
	
			fs = sp.getForwardedField(4);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
		}
	}

	@Test
	public void testConstantWildCard() {
		// no spaces
		{
			String[] constantFields = { "*" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(1);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(1));
	
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
	
			fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
		}
		
//		// with spaces
//		{
//			String[] constantFields = { "   *   " };
//	
//			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
//					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
//			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
//	
//			FieldSet fs = sp.getForwardedField(1);
//			Assert.assertTrue(fs.size() == 1);
//			Assert.assertTrue(fs.contains(1));
//	
//			fs = sp.getForwardedField(2);
//			Assert.assertTrue(fs.size() == 1);
//			Assert.assertTrue(fs.contains(2));
//	
//			fs = sp.getForwardedField(0);
//			Assert.assertTrue(fs.size() == 1);
//			Assert.assertTrue(fs.contains(0));
//		}
	}
		

	@Test
	public void testConstantWildCard2() {
		// no spaces
		{
			String[] constantFields = { "1->*" };
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
	
			FieldSet fs = sp.getForwardedField(1);
			Assert.assertTrue(fs.size() == 3);
			Assert.assertTrue(fs.contains(0));
			Assert.assertTrue(fs.contains(1));
			Assert.assertTrue(fs.contains(2));
			Assert.assertTrue(sp.getForwardedField(0) == null);
			Assert.assertTrue(sp.getForwardedField(2) == null);
		}
		
//		// with spaces
//		{
//			String[] constantFields = { "  1  -> * " };
//			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
//					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
//			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
//	
//			FieldSet fs = sp.getForwardedField(1);
//			Assert.assertTrue(fs.size() == 3);
//			Assert.assertTrue(fs.contains(0));
//			Assert.assertTrue(fs.contains(1));
//			Assert.assertTrue(fs.contains(2));
//			Assert.assertTrue(sp.getForwardedField(0) == null);
//			Assert.assertTrue(sp.getForwardedField(2) == null);
//		}
	}

	@Test
	public void testConstantInvalidString() {
		String[] constantFields = { "notValid" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		try {
			SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
			Assert.fail("accepted invalid input");
		} catch (InvalidProgramException e) {
			// good
		} catch (Exception e) {
			Assert.fail("Wrong type of exception thrown");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// Constant Fields Except
	// --------------------------------------------------------------------------------------------

	@Test
	public void testConstantExceptOneString() {
		// no spaces
		{
			String[] constantFieldsExcept = { "1" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			Assert.assertNull(sp.getForwardedField(1));
			
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
		}
		
		// with spaces
		{
			String[] constantFieldsExcept = { " 1  "};
			
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			Assert.assertNull(sp.getForwardedField(1));
			
			fs = sp.getForwardedField(2);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(2));
		}
	}
	
//	@Test
//	public void testConstantExceptIndividualStrings() {
//		// no spaces
//		{
//			String[] constantFieldsExcept = { "1", "2" };
//	
//			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
//					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
//			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
//	
//			FieldSet fs = sp.getForwardedField(0);
//			Assert.assertTrue(fs.size() == 1);
//			Assert.assertTrue(fs.contains(0));
//	
//			Assert.assertNull(sp.getForwardedField(1));
//			Assert.assertNull(sp.getForwardedField(2));
//		}
//		
//		// with spaces
//		{
//			String[] constantFieldsExcept = { " 1  ", " 2" };
//			
//			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
//					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
//			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
//	
//			FieldSet fs = sp.getForwardedField(0);
//			Assert.assertTrue(fs.size() == 1);
//			Assert.assertTrue(fs.contains(0));
//	
//			Assert.assertNull(sp.getForwardedField(1));
//			Assert.assertNull(sp.getForwardedField(2));
//		}
//	}
	
	@Test
	public void testConstantExceptSingleString() {
		// no spaces
		{
			String[] constantFieldsExcept = { "1,2" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			Assert.assertNull(sp.getForwardedField(1));
			Assert.assertNull(sp.getForwardedField(2));
		}
		
		// with spaces
		{
			String[] constantFieldsExcept = { " 1  , 2" };
			
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
	
			FieldSet fs = sp.getForwardedField(0);
			Assert.assertTrue(fs.size() == 1);
			Assert.assertTrue(fs.contains(0));
	
			Assert.assertNull(sp.getForwardedField(1));
			Assert.assertNull(sp.getForwardedField(2));
		}
	}
	
	@Test
	public void testConstantExceptString() {
		String[] constantFieldsExcept = { "notValid" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		try {
			SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);
			Assert.fail();
		} catch (Exception e) {
			// good
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	// Constant Fields Except
	// --------------------------------------------------------------------------------------------

	@Test
	public void testReadFieldsIndividualStrings() {
		// no spaces
		{
			String[] readFields = { "1", "2" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, null, readFields, type, type);
	
			FieldSet fs = sp.getReadFields();
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(2));
			Assert.assertTrue(fs.contains(1));
		}
		
		// with spaces
		{
			String[] readFields = { "   1    ", " 2   " };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, null, readFields, type, type);
	
			FieldSet fs = sp.getReadFields();
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(2));
			Assert.assertTrue(fs.contains(1));
		}
	}
	
	@Test
	public void testReadFieldsOneString() {
		// no spaces
		{
			String[] readFields = { "1,2" };
	
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, null, readFields, type, type);
	
			FieldSet fs = sp.getReadFields();
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(2));
			Assert.assertTrue(fs.contains(1));
		}
		
		// with spaces
		{
			String[] readFields = { "  1  , 2   " };
			
			TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, null, readFields, type, type);
	
			FieldSet fs = sp.getReadFields();
			Assert.assertTrue(fs.size() == 2);
			Assert.assertTrue(fs.contains(2));
			Assert.assertTrue(fs.contains(1));
		}
	}

	@Test
	public void testReadFieldsInvalidString() {
		String[] readFields = { "notValid" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		try {
			SemanticPropUtil.getSemanticPropsSingleFromString(null, null, readFields, type, type);
			Assert.fail("accepted invalid input");
		} catch (InvalidProgramException e) {
			// good
		} catch (Exception e) {
			Assert.fail("Wrong type of exception thrown");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// Two Inputs
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testSimpleCaseDual() {
		String[] constantFieldsFirst = { "1->1,2", "2->3" };
		String[] constantFieldsSecond = { "1->1,2", "2->3" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple4<Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		
		
		DualInputSemanticProperties dsp = new DualInputSemanticProperties();
		
		SemanticPropUtil.getSemanticPropsDualFromString(dsp, constantFieldsFirst, constantFieldsSecond, null,
				null, null, null, type, type, type);

		FieldSet fs = dsp.getForwardedField1(1);
		Assert.assertTrue(fs.size() == 2);
		Assert.assertTrue(fs.contains(1));
		Assert.assertTrue(fs.contains(2));

		fs = dsp.getForwardedField1(2);
		Assert.assertTrue(fs.size() == 1);
		Assert.assertTrue(fs.contains(3));
	}

	@Test
	public void testFieldsExceptDual() {
		String[] constantFieldsFirstExcept = { "1,2" };
		String[] constantFieldsSecond = { "0->1" };

		DualInputSemanticProperties dsp = new DualInputSemanticProperties();
		
		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		SemanticPropUtil.getSemanticPropsDualFromString(dsp, null, constantFieldsSecond,
				constantFieldsFirstExcept, null, null, null, type, type, type);

		FieldSet fs = dsp.getForwardedField1(0);
		Assert.assertTrue(fs.size() == 1);
		Assert.assertTrue(fs.contains(0));

		fs = dsp.getForwardedField1(1);
		Assert.assertTrue(fs == null);

		fs = dsp.getForwardedField1(2);
		Assert.assertTrue(fs == null);

		fs = dsp.getForwardedField2(0);
		Assert.assertTrue(fs.size() == 1);
		Assert.assertTrue(fs.contains(1));
	}
}
