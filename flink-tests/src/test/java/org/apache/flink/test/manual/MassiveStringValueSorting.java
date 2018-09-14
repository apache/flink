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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueComparator;
import org.apache.flink.api.java.typeutils.runtime.CopyableValueSerializer;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Test {@link UnilateralSortMerger} on a large set of {@link StringValue}.
 */
public class MassiveStringValueSorting {

	private static final long SEED = 347569784659278346L;

	public void testStringValueSorting() {
		File input = null;
		File sorted = null;

		try {
			// the source file
			input = generateFileWithStrings(300000, "http://some-uri.com/that/is/a/common/prefix/to/all");

			// the sorted file
			sorted = File.createTempFile("sorted_strings", "txt");

			String[] command = {"/bin/bash", "-c", "export LC_ALL=\"C\" && cat \"" + input.getAbsolutePath() + "\" | sort > \"" + sorted.getAbsolutePath() + "\""};

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
			UnilateralSortMerger<StringValue> sorter = null;
			BufferedReader reader = null;
			BufferedReader verifyReader = null;
			MemoryManager mm = null;
			IOManager ioMan = null;

			try {
				mm = new MemoryManager(1024 * 1024, 1);
				ioMan = new IOManagerAsync();

				TypeSerializer<StringValue> serializer = new CopyableValueSerializer<StringValue>(StringValue.class);
				TypeComparator<StringValue> comparator = new CopyableValueComparator<StringValue>(true, StringValue.class);

				reader = new BufferedReader(new FileReader(input));
				MutableObjectIterator<StringValue> inputIterator = new StringValueReaderMutableObjectIterator(reader);

				sorter = new UnilateralSortMerger<StringValue>(mm, ioMan, inputIterator, new DummyInvokable(),
						new RuntimeSerializerFactory<StringValue>(serializer, StringValue.class), comparator, 1.0, 4, 0.8f,
						true /* use large record handler */, true);

				MutableObjectIterator<StringValue> sortedData = sorter.getIterator();

				reader.close();

				// verify
				verifyReader = new BufferedReader(new FileReader(sorted));
				String nextVerify;
				StringValue nextFromFlinkSort = new StringValue();

				while ((nextVerify = verifyReader.readLine()) != null) {
					nextFromFlinkSort = sortedData.next(nextFromFlinkSort);

					Assert.assertNotNull(nextFromFlinkSort);
					Assert.assertEquals(nextVerify, nextFromFlinkSort.getValue());
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
				if (mm != null) {
					mm.shutdown();
				}
				if (ioMan != null) {
					ioMan.shutdown();
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
				//noinspection ResultOfMethodCallIgnored
				input.delete();
			}
			if (sorted != null) {
				//noinspection ResultOfMethodCallIgnored
				sorted.delete();
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void testStringValueTuplesSorting() {
		final int numStrings = 300000;

		File input = null;
		File sorted = null;

		try {
			// the source file
			input = generateFileWithStringTuples(numStrings, "http://some-uri.com/that/is/a/common/prefix/to/all");

			// the sorted file
			sorted = File.createTempFile("sorted_strings", "txt");

			String[] command = {"/bin/bash", "-c", "export LC_ALL=\"C\" && cat \"" + input.getAbsolutePath() + "\" | sort > \"" + sorted.getAbsolutePath() + "\""};

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
			UnilateralSortMerger<Tuple2<StringValue, StringValue[]>> sorter = null;
			BufferedReader reader = null;
			BufferedReader verifyReader = null;
			MemoryManager mm = null;
			IOManager ioMan = null;

			try {
				mm = new MemoryManager(1024 * 1024, 1);
				ioMan = new IOManagerAsync();

				TupleTypeInfo<Tuple2<StringValue, StringValue[]>> typeInfo = (TupleTypeInfo<Tuple2<StringValue, StringValue[]>>)
						new TypeHint<Tuple2<StringValue, StringValue[]>>(){}.getTypeInfo();

				TypeSerializer<Tuple2<StringValue, StringValue[]>> serializer = typeInfo.createSerializer(new ExecutionConfig());
				TypeComparator<Tuple2<StringValue, StringValue[]>> comparator = typeInfo.createComparator(new int[] { 0 }, new boolean[] { true }, 0, new ExecutionConfig());

				reader = new BufferedReader(new FileReader(input));
				MutableObjectIterator<Tuple2<StringValue, StringValue[]>> inputIterator = new StringValueTupleReaderMutableObjectIterator(reader);

				sorter = new UnilateralSortMerger<Tuple2<StringValue, StringValue[]>>(mm, ioMan, inputIterator, new DummyInvokable(),
						new RuntimeSerializerFactory<Tuple2<StringValue, StringValue[]>>(serializer, (Class<Tuple2<StringValue, StringValue[]>>) (Class<?>) Tuple2.class), comparator, 1.0, 4, 0.8f,
						true /* use large record handler */, false);

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

				MutableObjectIterator<Tuple2<StringValue, StringValue[]>> sortedData = sorter.getIterator();
				reader.close();

				// verify
				verifyReader = new BufferedReader(new FileReader(sorted));
				MutableObjectIterator<Tuple2<StringValue, StringValue[]>> verifyIterator = new StringValueTupleReaderMutableObjectIterator(verifyReader);

				Tuple2<StringValue, StringValue[]> nextVerify = new Tuple2<StringValue, StringValue[]>(new StringValue(), new StringValue[0]);
				Tuple2<StringValue, StringValue[]> nextFromFlinkSort = new Tuple2<StringValue, StringValue[]>(new StringValue(), new StringValue[0]);

				int num = 0;

				while ((nextVerify = verifyIterator.next(nextVerify)) != null) {
					num++;

					nextFromFlinkSort = sortedData.next(nextFromFlinkSort);
					Assert.assertNotNull(nextFromFlinkSort);

					Assert.assertEquals(nextVerify.f0, nextFromFlinkSort.f0);
					Assert.assertArrayEquals(nextVerify.f1, nextFromFlinkSort.f1);
				}

				Assert.assertNull(sortedData.next(nextFromFlinkSort));
				Assert.assertEquals(numStrings, num);

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
				if (mm != null) {
					mm.shutdown();
				}
				if (ioMan != null) {
					ioMan.shutdown();
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
				//noinspection ResultOfMethodCallIgnored
				input.delete();
			}
			if (sorted != null) {
				//noinspection ResultOfMethodCallIgnored
				sorted.delete();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class StringValueReaderMutableObjectIterator implements MutableObjectIterator<StringValue> {

		private final BufferedReader reader;

		public StringValueReaderMutableObjectIterator(BufferedReader reader) {
			this.reader = reader;
		}

		@Override
		public StringValue next(StringValue reuse) throws IOException {
			String line = reader.readLine();

			if (line == null) {
				return null;
			}

			reuse.setValue(line);
			return reuse;
		}

		@Override
		public StringValue next() throws IOException {
			return next(new StringValue());
		}
	}

	private static final class StringValueTupleReaderMutableObjectIterator implements MutableObjectIterator<Tuple2<StringValue, StringValue[]>> {

		private final BufferedReader reader;

		public StringValueTupleReaderMutableObjectIterator(BufferedReader reader) {
			this.reader = reader;
		}

		@Override
		public Tuple2<StringValue, StringValue[]> next(Tuple2<StringValue, StringValue[]> reuse) throws IOException {
			String line = reader.readLine();
			if (line == null) {
				return null;
			}

			String[] parts = line.split(" ");
			reuse.f0.setValue(parts[0]);
			reuse.f1 = new StringValue[parts.length];

			for (int i = 0; i < parts.length; i++) {
				reuse.f1[i] = new StringValue(parts[i]);
			}

			return reuse;
		}

		@Override
		public Tuple2<StringValue, StringValue[]> next() throws IOException {
			return next(new Tuple2<StringValue, StringValue[]>(new StringValue(), new StringValue[0]));
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

			for (int i = 0; i < numStrings; i++) {
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
			if (wrt != null) {
				wrt.close();
			}
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

			for (int i = 0; i < numStrings; i++) {
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
			if (wrt != null) {
				wrt.close();
			}
		}

		return f;
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		new MassiveStringValueSorting().testStringValueSorting();
		new MassiveStringValueSorting().testStringValueTuplesSorting();
	}
}
