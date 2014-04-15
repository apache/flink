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

package eu.stratosphere.test.testPrograms.CsvReaderCustomTupleTest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.io.CsvInputFormat;
import eu.stratosphere.api.java.io.CsvReader;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

public class CsvReaderTupleTypeTest {

	public static class Item extends Tuple4<Integer, String, Double, String> {
		
		private static final long serialVersionUID = -7444437337392053502L;
		
		public Item() {
		}
		
		public Item(Integer f0, String f1, Double f2, String f3) {
			this.f0 = f0;
			this.f1 = f1;
			this.f2 = f2;
			this.f3 = f3;
		}
		
		public int getID() {
			return this.f0;
		}
		public void setID(int iD) {
			this.f0 = iD;
		}
		public String getValue1() {
			return this.f1;
		}
		public void setValue1(String value1) {
			this.f1 = value1;
		}
		public double getDouble1() {
			return this.f2;
		}
		public void setDouble1(double double1) {
			this.f2 = double1;
		}
		public String getValue3() {
			return this.f3;
		}
		public void setValue3(String value3) {
			this.f3 = value3;
		}
	}	
	
	public static class SubItem extends Item {
		
		public SubItem() {			
		}		

		public SubItem(Integer f0, String f1, Double f2, String f3) {
			this.setID(f0);
			this.setValue1(f1);
			this.setDouble1(f2);
			this.setValue3(f3);
		}
	}
	
	@Test
	public void testReturnType() throws Exception {
		CsvReader reader = new CsvReader("test/test", ExecutionEnvironment.getExecutionEnvironment());
		DataSource<Item> items = reader.tupleType(Item.class);
		Assert.assertTrue(items.getType().getTypeClass() == Item.class);
	}
	
	@Test
	public void testFieldTypes() throws Exception {
		CsvReader reader = new CsvReader("test/test", ExecutionEnvironment.getExecutionEnvironment());
		DataSource<Item> items = reader.tupleType(Item.class);
		TypeInformation<?> info = items.getType();
		if (!info.isTupleType()) {
			Assert.fail();
		} else {
			TupleTypeInfo<?> tinfo = (TupleTypeInfo<?>) info;
			Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tinfo.getTypeAt(0));
			Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(1));
			Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tinfo.getTypeAt(2));
			Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(3));

		}		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSubClass() throws Exception {
		CsvReader reader = new CsvReader("test/test", ExecutionEnvironment.getExecutionEnvironment());
		DataSource<SubItem> sitems = reader.tupleType(SubItem.class);
		TypeInformation<?> info = sitems.getType();
		
		Assert.assertEquals(true, info.isTupleType());
		Assert.assertEquals(SubItem.class, info.getTypeClass());
		
		TupleTypeInfo<SubItem> tinfo = (TupleTypeInfo<SubItem>) info;
		
		Assert.assertEquals(BasicTypeInfo.INT_TYPE_INFO, tinfo.getTypeAt(0));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(1));
		Assert.assertEquals(BasicTypeInfo.DOUBLE_TYPE_INFO, tinfo.getTypeAt(2));
		Assert.assertEquals(BasicTypeInfo.STRING_TYPE_INFO, tinfo.getTypeAt(3));
			
	}
}
