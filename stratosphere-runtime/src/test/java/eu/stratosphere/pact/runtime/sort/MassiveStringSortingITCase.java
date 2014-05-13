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

package eu.stratosphere.pact.runtime.sort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.junit.Assert;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.base.StringComparator;
import eu.stratosphere.api.common.typeutils.base.StringSerializer;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeStatelessSerializerFactory;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.util.MutableObjectIterator;

public class MassiveStringSortingITCase {

	private static final long SEED = 347569784659278346L;
	
	@SuppressWarnings("unused")
	private static final char LINE_BREAK = '\n';
	
	
	public void testStringSorting() {
		File input = null;
		File sorted = null;

		try {
			// the source file
			input = generateFileWithStrings(300000, "http://some-uri.com/that/is/a/common/prefix/to/all");
			
			// the sorted file
			sorted = File.createTempFile("sorted_strings", "txt");
			
			String[] command = {"/bin/bash","-c","export LC_ALL=\"C\" && cat \"" + input.getAbsolutePath() + "\" | sort > \"" + sorted.getAbsolutePath() + "\""};
			
			Process p = null;
			try {
				p = Runtime.getRuntime().exec(command);
				int retCode = p.waitFor();
				if (retCode != 0) {
					throw new Exception("Command failed with return code " + retCode);
				}
				p = null;
			} finally {
				if (p != null) {
					p.destroy();
				}
			}
			
			// sort the data
			UnilateralSortMerger<String> sorter = null;
			BufferedReader reader = null;
			BufferedReader verifyReader = null;
			
			try {
				MemoryManager mm = new DefaultMemoryManager(1024 * 1024);
				IOManager ioMan = new IOManager();
					
				TypeSerializer<String> serializer = StringSerializer.INSTANCE;
				TypeComparator<String> comparator = new StringComparator(true);
				
				reader = new BufferedReader(new FileReader(input));
				MutableObjectIterator<String> inputIterator = new StringReaderMutableObjectIterator(reader);
				
				sorter = new UnilateralSortMerger<String>(mm, ioMan, inputIterator, new DummyInvokable(),
						new RuntimeStatelessSerializerFactory<String>(serializer, String.class), comparator, 1024 * 1024, 4, 0.8f);

				MutableObjectIterator<String> sortedData = sorter.getIterator();
				
				reader.close();
				
				// verify
				verifyReader = new BufferedReader(new FileReader(sorted));
				String next;
				
				while ((next = verifyReader.readLine()) != null) {
					String nextFromStratoSort = sortedData.next("");
					
					Assert.assertNotNull(nextFromStratoSort);
					Assert.assertEquals(next, nextFromStratoSort);
				}
			}
			finally {
				if (reader != null) {
					reader.close();
				}
				if (verifyReader != null) {
					verifyReader.close();
				}
				if (sorter != null) {
					sorter.close();
				}
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		finally {
			if (input != null) {
				input.delete();
			}
			if (sorted != null) {
				sorted.delete();
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void testStringTuplesSorting() {
		final int NUM_STRINGS = 300000;
		
		File input = null;
		File sorted = null;

		try {
			// the source file
			input = generateFileWithStringTuples(NUM_STRINGS, "http://some-uri.com/that/is/a/common/prefix/to/all");
			
			// the sorted file
			sorted = File.createTempFile("sorted_strings", "txt");
			
			String[] command = {"/bin/bash","-c","export LC_ALL=\"C\" && cat \"" + input.getAbsolutePath() + "\" | sort > \"" + sorted.getAbsolutePath() + "\""};
			
			Process p = null;
			try {
				p = Runtime.getRuntime().exec(command);
				int retCode = p.waitFor();
				if (retCode != 0) {
					throw new Exception("Command failed with return code " + retCode);
				}
				p = null;
			} finally {
				if (p != null) {
					p.destroy();
				}
			}
			
			// sort the data
			UnilateralSortMerger<Tuple2<String, String[]>> sorter = null;
			BufferedReader reader = null;
			BufferedReader verifyReader = null;
			
			try {
				MemoryManager mm = new DefaultMemoryManager(1024 * 1024);
				IOManager ioMan = new IOManager();
					
				TupleTypeInfo<Tuple2<String, String[]>> typeInfo = (TupleTypeInfo<Tuple2<String, String[]>>) (TupleTypeInfo<?>) TypeInformation.parse("Tuple2<String, String[]>");

				TypeSerializer<Tuple2<String, String[]>> serializer = typeInfo.createSerializer();
				TypeComparator<Tuple2<String, String[]>> comparator = typeInfo.createComparator(new int[] { 0 }, new boolean[] { true } );
				
				reader = new BufferedReader(new FileReader(input));
				MutableObjectIterator<Tuple2<String, String[]>> inputIterator = new StringTupleReaderMutableObjectIterator(reader);
				
				sorter = new UnilateralSortMerger<Tuple2<String, String[]>>(mm, ioMan, inputIterator, new DummyInvokable(),
						new RuntimeStatelessSerializerFactory<Tuple2<String, String[]>>(serializer, (Class<Tuple2<String, String[]>>) (Class<?>) Tuple2.class), comparator, 1024 * 1024, 4, 0.8f);

				
				
				// use this part to verify that all if good when sorting in memory
				
//				List<MemorySegment> memory = mm.allocatePages(new DummyInvokable(), mm.computeNumberOfPages(1024*1024*1024));
//				NormalizedKeySorter<Tuple2<String, String[]>> nks = new NormalizedKeySorter<Tuple2<String,String[]>>(serializer, comparator, memory);
//
//				{
//					Tuple2<String, String[]> wi = new Tuple2<String, String[]>("", new String[0]);
//					while ((wi = inputIterator.next(wi)) != null) {
//						Assert.assertTrue(nks.write(wi));
//					}
//					
//					new QuickSort().sort(nks);
//				}
//				
//				MutableObjectIterator<Tuple2<String, String[]>> sortedData = nks.getIterator();
				
				MutableObjectIterator<Tuple2<String, String[]>> sortedData = sorter.getIterator();
				reader.close();
				
				// verify
				verifyReader = new BufferedReader(new FileReader(sorted));
				MutableObjectIterator<Tuple2<String, String[]>> verifyIterator = new StringTupleReaderMutableObjectIterator(verifyReader);
				
				Tuple2<String, String[]> next = new Tuple2<String, String[]>("", new String[0]);
				Tuple2<String, String[]> nextFromStratoSort = new Tuple2<String, String[]>("", new String[0]);
				
				int num = 0;
				
				while ((next = verifyIterator.next(next)) != null) {
					num++;
					
					nextFromStratoSort = sortedData.next(nextFromStratoSort);
					Assert.assertNotNull(nextFromStratoSort);
						
					if (nextFromStratoSort.f0.equals("http://some-uri.com/that/is/a/common/prefix/to/all(()HK;V3__.e*")) {
						System.out.println("Found at position " + num);
					}
					
					Assert.assertEquals(next.f0, nextFromStratoSort.f0);
					Assert.assertArrayEquals(next.f1, nextFromStratoSort.f1);
				}
				
				Assert.assertNull(sortedData.next(nextFromStratoSort));
				Assert.assertEquals(NUM_STRINGS, num);

			}
			finally {
				if (reader != null) {
					reader.close();
				}
				if (verifyReader != null) {
					verifyReader.close();
				}
				if (sorter != null) {
					sorter.close();
				}
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
		finally {
			if (input != null) {
				input.delete();
			}
			if (sorted != null) {
				sorted.delete();
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	
	private static final class StringReaderMutableObjectIterator implements MutableObjectIterator<String> {
		
		private final BufferedReader reader;

		public StringReaderMutableObjectIterator(BufferedReader reader) {
			this.reader = reader;
		}
		
		@Override
		public String next(String reuse) throws IOException {
			return reader.readLine();
		}
	}
	
	private static final class StringTupleReaderMutableObjectIterator implements MutableObjectIterator<Tuple2<String, String[]>> {
		
		private final BufferedReader reader;

		public StringTupleReaderMutableObjectIterator(BufferedReader reader) {
			this.reader = reader;
		}
		
		@Override
		public Tuple2<String, String[]> next(Tuple2<String, String[]> reuse) throws IOException {
			String line = reader.readLine();
			if (line == null) {
				return null;
			}
			
			String[] parts = line.split(" ");
			reuse.f0 = parts[0];
			reuse.f1 = parts;
			return reuse;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private File generateFileWithStrings(int numStrings, String prefix) throws IOException {
		final Random rnd = new Random(SEED);
		
		final StringBuilder bld = new StringBuilder();
		final int resetValue = prefix.length();
		
		bld.append(prefix);
		
		File f = File.createTempFile("strings", "txt");
		BufferedWriter wrt = null;
		try {
			wrt = new BufferedWriter(new FileWriter(f));
		
			for (int i = 0 ; i < numStrings; i++) {
				bld.setLength(resetValue);
				
				int len = rnd.nextInt(20) + 300;
				for (int k = 0; k < len; k++) {
					char c = (char) (rnd.nextInt(80) + 40);
					bld.append(c);
				}
				
				String str = bld.toString();
				wrt.write(str);
				wrt.newLine();
			}
		} finally {
			wrt.close();
		}
		
		return f;
	}
	
	private File generateFileWithStringTuples(int numStrings, String prefix) throws IOException {
		final Random rnd = new Random(SEED);
		
		final StringBuilder bld = new StringBuilder();

		File f = File.createTempFile("strings", "txt");
		BufferedWriter wrt = null;
		try {
			wrt = new BufferedWriter(new FileWriter(f));
		
			for (int i = 0 ; i < numStrings; i++) {
				bld.setLength(0);
				
				int numComps = rnd.nextInt(5) + 1;
				
				for (int z = 0; z < numComps; z++) {
					if (z > 0) {
						bld.append(' ');
					}
					bld.append(prefix);
				
					int len = rnd.nextInt(20) + 10;
					for (int k = 0; k < len; k++) {
						char c = (char) (rnd.nextInt(80) + 40);
						bld.append(c);
					}
				}
				
				String str = bld.toString();
				
				wrt.write(str);
				wrt.newLine();
			}
		} finally {
			wrt.close();
		}
		
		return f;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static void main(String[] args) {
		new MassiveStringSortingITCase().testStringSorting();
		new MassiveStringSortingITCase().testStringTuplesSorting();
	}
}
