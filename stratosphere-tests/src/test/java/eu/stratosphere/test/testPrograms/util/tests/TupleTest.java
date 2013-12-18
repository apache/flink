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

package eu.stratosphere.test.testPrograms.util.tests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.test.testPrograms.util.Tuple;

public class TupleTest {

	@Test
	public void testTupleByteArrayShortArrayInt() {
		
		String tupleStr = "attr1|attr2|3|4|attr5|";
		int[] offsets = {0,6,12,14,16,22};
		Tuple t1 = new Tuple(tupleStr.getBytes(),offsets,5);
		
		Assert.assertTrue(t1.getBytes().length == tupleStr.getBytes().length);
		for(int i=0;i<t1.getBytes().length;i++) {
			Assert.assertTrue(t1.getBytes()[i] == tupleStr.getBytes()[i]);
		}
		Assert.assertTrue(t1.getNumberOfColumns() == 5);
		Assert.assertTrue(t1.getStringValueAt(0).equals("attr1"));
		Assert.assertTrue(t1.getStringValueAt(1).equals("attr2"));
		Assert.assertTrue(t1.getLongValueAt(2) == 3);
		Assert.assertTrue(t1.getLongValueAt(3) == 4);
		Assert.assertTrue(t1.getStringValueAt(4).equals("attr5"));
		
	}

	@Test
	public void testGetNumberOfColumns() {
		
		Tuple t = new Tuple();
		
		Assert.assertTrue(t.getNumberOfColumns() == 0);
		
		t.addAttribute("a");
		Assert.assertTrue(t.getNumberOfColumns() == 1);
		
		t.addAttribute("b");
		Assert.assertTrue(t.getNumberOfColumns() == 2);
		
		t.addAttribute("aasdfasd|fasdf");
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		
		String tupleStr = "attr1|attr2|3|4|attr5|";
		int[] offsets = {0,6,12,14,16,22};
		t = new Tuple(tupleStr.getBytes(),offsets,5);
		
		Assert.assertTrue(t.getNumberOfColumns() == 5);
	}

	@Test
	public void testGetBytes() {
		
		Tuple t1 = new Tuple();
		
		String[] values = {"a","b","cdefgh","2342432","Adasdfee324324D"};
		
		for(String val : values) {
			t1.addAttribute(val);
		}
		
		String exp1 = "";
		for(int i=0;i<values.length;i++) {
			exp1 += values[i]+"|";
		}
		
		byte[] ret1 = t1.getBytes();
		
		for(int i=0;i<exp1.getBytes().length;i++) {
			Assert.assertTrue(ret1[i] == exp1.getBytes()[i]);
		}
		Assert.assertTrue(ret1[exp1.getBytes().length+1] == 0);
		
		String tupleStr = "attr1|attr2|3|4|attr5|";
		int[] offsets = {0,6,12,14,16,22};
		Tuple t2 = new Tuple(tupleStr.getBytes(),offsets,5);
		
		byte[] ret2 = t2.getBytes();
		
		Assert.assertTrue(ret2.length == tupleStr.getBytes().length);
		for(int i=0;i<tupleStr.getBytes().length;i++) {
			Assert.assertTrue(ret2[i] == tupleStr.getBytes()[i]);
		}
	}

	@Test
	public void testGetColumnLength() {
		Tuple t = new Tuple();
		
		Assert.assertTrue(t.getColumnLength(1) == -1);
		Assert.assertTrue(t.getColumnLength(0) == -1);
		Assert.assertTrue(t.getColumnLength(-1) == -1);
		
		t.addAttribute("a");
		Assert.assertTrue(t.getColumnLength(0) == 1);
		
		t.addAttribute("b");
		Assert.assertTrue(t.getColumnLength(1) == 1);
		
		t.addAttribute("aasdfasd|fasdf");
		Assert.assertTrue(t.getColumnLength(2) == 14);
		
		String tupleStr = "attr1|attr2|3|4|attr5|";
		int[] offsets = {0,6,12,14,16,22};
		t = new Tuple(tupleStr.getBytes(),offsets,5);
		
		Assert.assertTrue(t.getColumnLength(0) == 5);
		Assert.assertTrue(t.getColumnLength(1) == 5);
		Assert.assertTrue(t.getColumnLength(2) == 1);
		Assert.assertTrue(t.getColumnLength(3) == 1);
		Assert.assertTrue(t.getColumnLength(4) == 5);
		
	}

	@Test
	public void testConcatenate() {
		Tuple t0 = new Tuple();
		Tuple t1 = new Tuple();
		Tuple t2 = new Tuple();
		
		// check handling of empty tuples
		t0.concatenate(t1);
		Assert.assertTrue(t0.getNumberOfColumns() == 0);
		
		t1.addAttribute("a");
		t1.concatenate(t0);
		Assert.assertTrue(t1.getNumberOfColumns() == 1);
		Assert.assertTrue(t1.getStringValueAt(0).equals("a"));
		
		t0.concatenate(t1);
		Assert.assertTrue(t0.getNumberOfColumns() == 1);
		Assert.assertTrue(t0.getStringValueAt(0).equals("a"));
		
		t1.addAttribute("b");
		t1.addAttribute("c");
		t2.addAttribute("z");
		t2.addAttribute("y");
		t2.addAttribute("x");
		t2.concatenate(t1);
		// check tuple t2
		Assert.assertTrue(t2.getNumberOfColumns() == 6);
		Assert.assertTrue(t2.getStringValueAt(0).equals("z"));
		Assert.assertTrue(t2.getStringValueAt(1).equals("y"));
		Assert.assertTrue(t2.getStringValueAt(2).equals("x"));
		Assert.assertTrue(t2.getStringValueAt(3).equals("a"));
		Assert.assertTrue(t2.getStringValueAt(4).equals("b"));
		Assert.assertTrue(t2.getStringValueAt(5).equals("c"));
		
		t1.concatenate(t2);
		// check tuple t1
		Assert.assertTrue(t1.getNumberOfColumns() == 9);
		Assert.assertTrue(t1.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t1.getStringValueAt(1).equals("b"));
		Assert.assertTrue(t1.getStringValueAt(2).equals("c"));
		Assert.assertTrue(t1.getStringValueAt(3).equals("z"));
		Assert.assertTrue(t1.getStringValueAt(4).equals("y"));
		Assert.assertTrue(t1.getStringValueAt(5).equals("x"));
		Assert.assertTrue(t1.getStringValueAt(6).equals("a"));
		Assert.assertTrue(t1.getStringValueAt(7).equals("b"));
		Assert.assertTrue(t1.getStringValueAt(8).equals("c"));
		// check tuple t2
		Assert.assertTrue(t2.getNumberOfColumns() == 6);
		Assert.assertTrue(t2.getStringValueAt(0).equals("z"));
		Assert.assertTrue(t2.getStringValueAt(1).equals("y"));
		Assert.assertTrue(t2.getStringValueAt(2).equals("x"));
		Assert.assertTrue(t2.getStringValueAt(3).equals("a"));
		Assert.assertTrue(t2.getStringValueAt(4).equals("b"));
		Assert.assertTrue(t2.getStringValueAt(5).equals("c"));
		
	}

	@Test
	public void testProject() {
		
		Tuple t = new Tuple();
		
		t.project(1);
		Assert.assertTrue(t.getNumberOfColumns() == 0);
		
		t.addAttribute("a");
		t.project(0);
		Assert.assertTrue(t.getNumberOfColumns() == 0);

		t.addAttribute("a");
		t.addAttribute("b");
		t.project(2);
		Assert.assertTrue(t.getNumberOfColumns() == 1);
		Assert.assertTrue(t.getStringValueAt(0).equals("b"));
		
		t.addAttribute("c");
		t.addAttribute("d");
		t.project(5);
		Assert.assertTrue(t.getNumberOfColumns() == 2);
		Assert.assertTrue(t.getStringValueAt(0).equals("b"));
		Assert.assertTrue(t.getStringValueAt(1).equals("d"));
		
		t.project(0);
		Assert.assertTrue(t.getNumberOfColumns() == 0);
		
		t.addAttribute("a");
		t.addAttribute("b");
		t.addAttribute("c");
		t.addAttribute("d");
		t.project(11);
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		Assert.assertTrue(t.getStringValueAt(2).equals("d"));
		
		t.project(23);
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		Assert.assertTrue(t.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t.getStringValueAt(1).equals("b"));
		Assert.assertTrue(t.getStringValueAt(2).equals("d"));
		
		t.project(-1);
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		Assert.assertTrue(t.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t.getStringValueAt(1).equals("b"));
		Assert.assertTrue(t.getStringValueAt(2).equals("d"));
	}

	@Test
	public void testCompareStringAttribute() {
		Tuple t1 = new Tuple();
		Tuple t2 = new Tuple();
		
		t1.addAttribute("a");
		t1.addAttribute("b");
		t1.addAttribute("ccccc");
		
		t2.addAttribute("a");
		t2.addAttribute("bb");
		t2.addAttribute("ccccc");
		t2.addAttribute("z");
		
		Assert.assertTrue(t1.compareStringAttribute(t2, 0, 0) == 0);
		Assert.assertTrue(t1.compareStringAttribute(t2, 0, 1) < 0);
		Assert.assertTrue(t1.compareStringAttribute(t2, 1, 1) < 0);
		Assert.assertTrue(t1.compareStringAttribute(t2, 2, 2) == 0);
		Assert.assertTrue(t1.compareStringAttribute(t2, 2, 3) < 0);
		
		Assert.assertTrue(t2.compareStringAttribute(t1, 0, 0) == 0);
		Assert.assertTrue(t2.compareStringAttribute(t1, 1, 0) > 0);
		Assert.assertTrue(t2.compareStringAttribute(t1, 1, 1) > 0);
		Assert.assertTrue(t2.compareStringAttribute(t1, 2, 2) == 0);
		Assert.assertTrue(t2.compareStringAttribute(t1, 3, 2) > 0);
		
		Assert.assertTrue(t1.compareStringAttribute(t1, 0, 0) == 0);
		Assert.assertTrue(t1.compareStringAttribute(t1, 1, 1) == 0);
		Assert.assertTrue(t1.compareStringAttribute(t1, 2, 2) == 0);
		Assert.assertTrue(t1.compareStringAttribute(t1, 0, 1) < 0);
		Assert.assertTrue(t1.compareStringAttribute(t1, 2, 1) > 0);
		
		// check for out-of-bounds values
		boolean exceptionThrown = false;
		try {
			t1.compareStringAttribute(t1, 0, 3);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t1.compareStringAttribute(t1, 4, 0);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
	}

	@Test
	public void testCompareIntAttribute() {
		Tuple t1 = new Tuple();
		
		t1.addAttribute("1");
		t1.addAttribute("2");
		t1.addAttribute("112315412");
		t1.addAttribute(Integer.MAX_VALUE+"");
		t1.addAttribute("-1");
		t1.addAttribute(Integer.MIN_VALUE+"");
		
		// check identical values
		Assert.assertTrue(t1.compareIntAttribute(t1, 0, 0) == 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 1, 1) == 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 2, 2) == 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 3, 3) == 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 4, 4) == 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 5, 5) == 0);
		
		// check unequal values
		Assert.assertTrue(t1.compareIntAttribute(t1, 0, 1) < 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 1, 0) > 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 1, 2) < 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 2, 1) > 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 2, 3) < 0);
		
		// check negative values
		Assert.assertTrue(t1.compareIntAttribute(t1, 0, 4) > 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 4, 0) < 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 4, 5) > 0);
		Assert.assertTrue(t1.compareIntAttribute(t1, 5, 4) < 0);
		
		// check for non-existing attributes
		boolean exceptionThrown = false;
		try {
			t1.compareIntAttribute(t1, 0, 6);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t1.compareIntAttribute(t1, 7, 0);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
	}

	@Test
	public void testGetStringValueAt() {
		
		Tuple t = new Tuple();
		
		String[] testStrings = {"a","b","123123","Hello world!","Test with p|pe","!§$%&/()=*'.:,;-_#+'`}][{"};
		
		for(String testString : testStrings) {
			t.addAttribute(testString);
		}
		
		// check for same value
		for(int i=0;i<testStrings.length;i++) {
			Assert.assertTrue(t.getStringValueAt(i).equals(testStrings[i]));
		}
		
		// check for out-of-bounds values
		boolean exceptionThrown = false;
		try {
			t.getStringValueAt(-1);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t.getStringValueAt(testStrings.length);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t.getStringValueAt(testStrings.length+1);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
	}

	@Test
	public void testGetLongValueAt() {
	
		Tuple t = new Tuple();
		
		long[] testVals = {0,1,2,1234123,-1,-1212312, Long.MIN_VALUE, Long.MAX_VALUE};
		
		for(long testVal : testVals) {
			t.addAttribute(testVal+"");
		}
		
		// check for same value
		for(int i=0;i<testVals.length;i++) {
			Assert.assertTrue(t.getLongValueAt(i) == testVals[i]);
		}
		
		// check for out-of-bounds values
		boolean exceptionThrown = false;
		try {
			t.getLongValueAt(-1);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t.getLongValueAt(testVals.length);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t.getLongValueAt(testVals.length+1);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		// check for invalid format exception
		t.addAttribute("abc");
		exceptionThrown = false;
		try {
			t.getLongValueAt(testVals.length);
		} catch(NumberFormatException nfe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
	}

	@Test
	public void testGetByteArrayValueAt() {
		
		Tuple t = new Tuple();
		
		String[] testStrings = {"a","b","123123","Hello world!","Test with p|pe"};
		
		for(String testString : testStrings) {
			t.addAttribute(testString);
		}
		
		// check for same value
		for(int i=0;i<testStrings.length;i++) {
			byte[] att = t.getByteArrayValueAt(i);
			Assert.assertTrue(att.length == (testStrings[i]).getBytes().length);
			for(int j=0;j<att.length;j++) {
				Assert.assertTrue(att[j] == testStrings[i].getBytes()[j]);
			}
		}
		
		// check for out-of-bounds values
		boolean exceptionThrown = false;
		try {
			t.getByteArrayValueAt(-1);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t.getByteArrayValueAt(testStrings.length);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
		exceptionThrown = false;
		try {
			t.getByteArrayValueAt(testStrings.length+1);
		} catch(IndexOutOfBoundsException ioobe) {
			exceptionThrown = true;
		}
		Assert.assertTrue(exceptionThrown);
		
	}

	@Test
	public void testReserveSpace() {
		Tuple t = new Tuple();
		
		t.addAttribute("a");
		t.addAttribute("b");
		t.addAttribute("cde");
		
		t.reserveSpace(512);
		
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		Assert.assertTrue(t.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t.getStringValueAt(1).equals("b"));
		Assert.assertTrue(t.getStringValueAt(2).equals("cde"));
		Assert.assertTrue(t.getBytes().length == 512);
		
		t.reserveSpace(20);
		
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		Assert.assertTrue(t.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t.getStringValueAt(1).equals("b"));
		Assert.assertTrue(t.getStringValueAt(2).equals("cde"));
		Assert.assertTrue(t.getBytes().length == 512);
		
	}

	@Test
	public void testCompact() {
		
		Tuple t = new Tuple();
		t.addAttribute("Hello world!");
		
		Assert.assertTrue(t.getBytes().length == 256);
		t.compact();
		Assert.assertTrue(t.getBytes().length == 13);
		
		byte[] ba = new byte[1024];
		int[] of = {0};
		t = new Tuple(ba, of, 0);
		
		Assert.assertTrue(t.getBytes().length == 1024);
		t.compact();
		Assert.assertTrue(t.getBytes().length == 0);
		
		ba = "attr1|attr2|3|4|attr5|thisdoesnotbelongtothetuple".getBytes();
		int[] of2 = {0,6,12,14,16,22};
		t = new Tuple(ba, of2, 5);
		
		Assert.assertTrue(t.getBytes().length == ba.length);
		t.compact();
		Assert.assertTrue(t.getBytes().length == 22);
		
	}

	@Test
	public void testAddAttributeByteArray() {
		Tuple t = new Tuple();
		
		Assert.assertTrue(t.getNumberOfColumns() == 0);
		
		t.addAttribute("a".getBytes());
		Assert.assertTrue(t.getNumberOfColumns() == 1);
		Assert.assertTrue(t.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t.getBytes().length == 256);
		
		t.compact();
		t.addAttribute("123345".getBytes());
		Assert.assertTrue(t.getNumberOfColumns() == 2);
		Assert.assertTrue(t.getLongValueAt(1) == 123345);
		Assert.assertTrue(t.getBytes().length == 9);
		
		t.addAttribute("adfasdfg".getBytes());
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		byte[] ret = t.getByteArrayValueAt(2);
		Assert.assertTrue(ret.length == "adfasdfg".getBytes().length);
		for(int i=0;i<ret.length;i++) {
			Assert.assertTrue(ret[i] == "adfasdfg".getBytes()[i]);
		}
		Assert.assertTrue(t.getBytes().length == 18);
		
	}

	@Test
	public void testAddAttributeFromKVRecord() {
		
		Tuple t1 = new Tuple();
		t1.addAttribute("a");
		t1.addAttribute("123345");
		t1.addAttribute("adfasdfg");
		
		Tuple t2 = new Tuple();
		
		Assert.assertTrue(t2.getNumberOfColumns() == 0);
		
		t2.addAttributeFromKVRecord(t1, 1);
		Assert.assertTrue(t2.getNumberOfColumns() == 1);
		Assert.assertTrue(t2.getLongValueAt(0) == 123345);
		Assert.assertTrue(t2.getBytes().length == 256);
		
		t2.compact();

		t2.addAttributeFromKVRecord(t1, 2);
		Assert.assertTrue(t2.getNumberOfColumns() == 2);
		byte[] ret = t2.getByteArrayValueAt(1);
		Assert.assertTrue(ret.length == "adfasdfg".getBytes().length);
		for(int i=0;i<ret.length;i++) {
			Assert.assertTrue(ret[i] == "adfasdfg".getBytes()[i]);
		}
		Assert.assertTrue(t2.getBytes().length == 16);
		
		t2.addAttributeFromKVRecord(t1, 0);
		Assert.assertTrue(t2.getNumberOfColumns() == 3);
		Assert.assertTrue(t2.getStringValueAt(2).equals("a"));
		Assert.assertTrue(t2.getBytes().length == 18);
		
	}

	@Test
	public void testAddAttributeString() {
		
		Tuple t = new Tuple();
		
		Assert.assertTrue(t.getNumberOfColumns() == 0);
		
		t.addAttribute("a");
		Assert.assertTrue(t.getNumberOfColumns() == 1);
		Assert.assertTrue(t.getStringValueAt(0).equals("a"));
		Assert.assertTrue(t.getBytes().length == 256);
		
		t.compact();
		t.addAttribute(123345+"");
		Assert.assertTrue(t.getNumberOfColumns() == 2);
		Assert.assertTrue(t.getLongValueAt(1) == 123345);
		Assert.assertTrue(t.getBytes().length == 9);
		
		t.addAttribute("adfasdfg");
		Assert.assertTrue(t.getNumberOfColumns() == 3);
		byte[] ret = t.getByteArrayValueAt(2);
		Assert.assertTrue(ret.length == "adfasdfg".getBytes().length);
		for(int i=0;i<ret.length;i++) {
			Assert.assertTrue(ret[i] == "adfasdfg".getBytes()[i]);
		}
		Assert.assertTrue(t.getBytes().length == 18);
		
	}

	@Test
	public void testSerialization() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		
		Tuple t = new Tuple();
		t.addAttribute("Hello world!");
		try {
			t.write(dos);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		t.addAttribute("2ndAttribute");
		try {
			t.write(dos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		byte[] ba = "attr1|attr2|3|4|attr5|thisdoesnotbelongtothetuple".getBytes();
		int[] of2 = {0,6,12,14,16,22};
		t = new Tuple(ba, of2, 5);
		
		try {
			t.write(dos);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			dos.flush();
			baos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		byte[] data = baos.toByteArray();
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		DataInputStream dis = new DataInputStream(bais);
		
		try {
			dos.close();
			baos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		t = new Tuple();
		try {
			t.read(dis);
			
			Assert.assertTrue(t.getNumberOfColumns() == 1);
			Assert.assertTrue(t.getStringValueAt(0).equals("Hello world!"));
			Assert.assertTrue(t.toString().equals("Hello world!|"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		t = new Tuple();
		try {
			t.read(dis);
			
			Assert.assertTrue(t.getNumberOfColumns() == 2);
			Assert.assertTrue(t.getStringValueAt(0).equals("Hello world!"));
			Assert.assertTrue(t.getStringValueAt(1).equals("2ndAttribute"));
			Assert.assertTrue(t.toString().equals("Hello world!|2ndAttribute|"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		t = new Tuple();
		try {
			t.read(dis);
			
			Assert.assertTrue(t.getNumberOfColumns() == 5);
			Assert.assertTrue(t.getStringValueAt(0).equals("attr1"));
			Assert.assertTrue(t.getStringValueAt(1).equals("attr2"));
			Assert.assertTrue(t.getLongValueAt(2) == 3);
			Assert.assertTrue(t.getLongValueAt(3) == 4);
			Assert.assertTrue(t.getStringValueAt(4).equals("attr5"));
			Assert.assertTrue(t.toString().equals("attr1|attr2|3|4|attr5|"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			dis.close();
			bais.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}
	
	@Test
	public void testToString() {
		Tuple t = new Tuple();

		t.addAttribute("Hello world!");
		Assert.assertTrue(t.toString().equals("Hello world!|"));
		t.addAttribute("2ndValue");
		Assert.assertTrue(t.toString().equals("Hello world!|2ndValue|"));
		
		byte[] ba = "attr1|attr2|3|4|attr5|thisdoesnotbelongtothetuple".getBytes();
		int[] of2 = {0,6,12,14,16,22};
		t = new Tuple(ba, of2, 5);
		
		Assert.assertTrue(t.toString().equals("attr1|attr2|3|4|attr5|"));
		
	}

}
