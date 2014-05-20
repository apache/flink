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

	@Test
	public void testSimpleCase() {
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

	@Test
	public void testSimpleCaseWildCard() {
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

	@Test
	public void testSimpleCaseWildCard2() {
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

	@Test
	public void testConstantFieldList() {
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

	@Test
	public void testConstantFieldsExcept() {
		String[] constantFieldsExcept = { "1" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, constantFieldsExcept, null, type, type);

		FieldSet fs = sp.getForwardedField(0);
		Assert.assertTrue(fs.size() == 1);
		Assert.assertTrue(fs.contains(0));

		fs = sp.getForwardedField(1);
		Assert.assertTrue(fs == null);

		fs = sp.getForwardedField(2);
		Assert.assertTrue(fs.size() == 1);
		Assert.assertTrue(fs.contains(2));
	}

	@Test
	public void testReadFields() {
		String[] readFields = { "1, 2" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		SingleInputSemanticProperties sp = SemanticPropUtil.getSemanticPropsSingleFromString(null, null, readFields, type, type);

		FieldSet fs = sp.getReadFields();
		Assert.assertTrue(fs.size() == 2);
		Assert.assertTrue(fs.contains(2));
		Assert.assertTrue(fs.contains(1));
	}

	@Test
	public void testSimpleCaseDual() {
		String[] constantFieldsFirst = { "1->1,2", "2->3" };
		String[] constantFieldsSecond = { "1->1,2", "2->3" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple4<Integer, Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		DualInputSemanticProperties dsp = SemanticPropUtil.getSemanticPropsDualFromString(constantFieldsFirst, constantFieldsSecond, null,
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

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		DualInputSemanticProperties dsp = SemanticPropUtil.getSemanticPropsDualFromString(null, constantFieldsSecond,
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

	@Test
	public void testStringParse1() {
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
	public void testStringParse2() {
		String[] constantFields = { "notValid" };

		TypeInformation<?> type = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		try {
			SemanticPropUtil.getSemanticPropsSingleFromString(constantFields, null, null, type, type);
			Assert.fail();
		} catch (Exception e) {
			// good
		}
	}
}
