/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.SequentialOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactList;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Tests {@link TestPlan}.
 * 
 * @author Arvid Heise
 */
public class TestPlanTest {
	@SuppressWarnings("unchecked")
	private static final Class<? extends Value>[] IntStringPair = new Class[] { PactInteger.class, PactString.class };

	/**
	 * (int1, string1) x (int2, string2) -&gt; ((int1, int2), (string1, string2))
	 * 
	 * @author Arvid Heise
	 */
	public static final class CartesianProduct extends CrossStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.CrossStub#cross(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void cross(PactRecord record1, PactRecord record2, Collector<PactRecord> out) {
			out.collect(makeRecord(
				record1.getField(0, PactInteger.class),
				record2.getField(0, PactInteger.class),
				record1.getField(1, PactString.class),
				record2.getField(1, PactString.class)));
		}

	}

	private static PactRecord makeRecord(Value... values) {
		PactRecord record = new PactRecord();
		for (int index = 0; index < values.length; index++)
			record.setField(index, values[index]);
		return record;
	}

	/**
	 * (int1, [string1, string2]) x (int1, [string3, string4]) -&gt; (int1, [string1, string2, string3, string4])
	 * 
	 * @author Arvid Heise
	 */
	public static final class AppendingCoGroup extends CoGroupStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.CoGroupStub#coGroup(java.util.Iterator, java.util.Iterator,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@SuppressWarnings("null")
		@Override
		public void coGroup(Iterator<PactRecord> records1, Iterator<PactRecord> records2, Collector<PactRecord> out) {
			StringList values = new StringList();
			PactRecord lastRecord = null;
			while (records1.hasNext())
				values.add(new PactString((lastRecord = records1.next()).getField(1, PactString.class)));
			while (records2.hasNext())
				values.add(new PactString((lastRecord = records2.next()).getField(1, PactString.class)));
			out.collect(new PactRecord(lastRecord.getField(0, PactInteger.class), values));
		}
	}

	/**
	 * (int1, string1) x (int1, string2) -&gt; (int1, (string1, string2))
	 * 
	 * @author Arvid Heise
	 */
	public static final class Join extends MatchStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MatchStub#match(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.type.PactRecord, eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void match(PactRecord record1, PactRecord record2, Collector<PactRecord> out) throws Exception {
			out.collect(makeRecord(record1.getField(0, PactInteger.class),
				record1.getField(1, PactString.class),
				record2.getField(1, PactString.class)));
		}
	}

	/**
	 * @author Arvid Heise
	 */
	public static final class ErroneousPact extends MapStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			throw new IllegalStateException();
		}
	}

	/**
	 * (int1, string1), (int1, string2) -&gt; (int1, [string1, string2])
	 * 
	 * @author Arvid Heise
	 */
	public static final class AppendingReduce extends ReduceStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@SuppressWarnings("null")
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			StringList result = new StringList();
			PactRecord lastRecord = null;
			while (records.hasNext())
				result.add(new PactString((lastRecord = records.next()).getField(1, PactString.class)));
			PactInteger id = lastRecord.getField(0, PactInteger.class);
			out.collect(new PactRecord(id, result));
		}
	}

	/**
	 * List of {@link PactString}s.
	 * 
	 * @author Arvid Heise
	 */
	public static final class StringList extends PactList<PactString> {
		/**
		 * Initializes StringList.
		 */
		public StringList() {
		}

		private StringList(String... strings) {
			for (String string : strings)
				add(new PactString(string));
		}
	}

	/**
	 * Converts a input string (a line) into a PactRecord.
	 */
	public static class IntegerInFormat extends TextInputFormat {
		@Override
		public boolean reachedEnd() {
			return super.reachedEnd();
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.TextInputFormat#readRecord(eu.stratosphere.pact.common.type.PactRecord, byte[], int, int)
		 */
		@Override
		public boolean readRecord(PactRecord target, byte[] bytes, int offset, int numBytes) {
			target.setField(0, new PactInteger(Integer.valueOf(new String(bytes, offset, numBytes))));
			return true;//super.readRecord(target, bytes, offset, numBytes);
		}
	}

	/**
	 * Converts a PactRecord to a string.
	 */
	public static class IntegerOutFormat extends FileOutputFormat {
		private PrintWriter writer;
		
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.FileOutputFormat#open(int)
		 */
		@Override
		public void open(int taskNumber) throws IOException {
			super.open(taskNumber);
			this.writer = new PrintWriter(this.stream);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.io.OutputFormat#writeRecord(eu.stratosphere.pact.common.type.PactRecord)
		 */
		@Override
		public void writeRecord(PactRecord record) throws IOException {
			this.writer.format("%d\n", record.getField(0, PactInteger.class).getValue());
		}
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 */
	@Test
	public void completeTestPasses() {
		final FileDataSource read = createInput(IntegerInFormat.class,
			"TestPlan/test.txt");

		final MapContract map =
			new MapContract(IdentityMap.class, "Map");
		map.setInput(read);

		FileDataSink output = createOutput(map, SequentialOutputFormat.class);

		TestPlan testPlan = new TestPlan(output);
		testPlan.run();
		testPlan.getInput().setSchema(PactInteger.class);
		// testPlan.getActualOutput().setSchema(PactInteger.class);
		assertEquals("input and output should be equal in identity map", testPlan.getInput(), testPlan
			.getActualOutput());
	}

	/**
	 * Tests if a {@link TestPlan} without explicit data sources and sinks can be executed.
	 */
	@Test
	public void adhocInputAndOutputShouldTransparentlyWork() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.run();

		testPlan.getInput().setSchema(IntStringPair);
		testPlan.getActualOutput().setSchema(IntStringPair);
		assertEquals("input and output should be equal in identity map", testPlan.getInput(), testPlan
			.getActualOutput());

		// explicitly check output
		Iterator<PactRecord> outputIterator = testPlan.getActualOutput().iterator();
		Iterator<PactRecord> inputIterator = testPlan.getInput().iterator();
		for (int index = 0; index < 2; index++) {
			assertTrue("too few actual output values", outputIterator.hasNext());
			assertTrue("too few input values", outputIterator.hasNext());
			try {
				assertTrue(PactRecordEqualer.recordsEqual(inputIterator.next(), outputIterator.next(), IntStringPair));
			} catch (AssertionFailedError e) {
				throw new ArrayComparisonFailure("Could not verify output values", e, index);
			}
		}
		assertFalse("too few actual output values", outputIterator.hasNext());
		assertFalse("too few input values", outputIterator.hasNext());
	}

	/**
	 * Tests if a {@link TestPlan} can be executed.
	 */
	@Test
	@Ignore
	public void completeTestPassesWithExpectedValues() {
		final FileDataSource read = createInput(IntegerInFormat.class,
			"TestPlan/test.txt");

		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		map.setInput(read);

		FileDataSink output = createOutput(map, IntegerOutFormat.class);

		TestPlan testPlan = new TestPlan(output);
		testPlan.getExpectedOutput(output, IntStringPair).fromFile(IntegerInFormat.class,
			getResourcePath("TestPlan/test.txt"));
		testPlan.run();
	}

	private String getResourcePath(String resource) {
		try {
			Enumeration<URL> resources = TestPlanTest.class.getClassLoader().getResources(resource);
			if (resources.hasMoreElements())
				return resources.nextElement().toString();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		throw new IllegalArgumentException("no resources found");
	}

	/**
	 * Tests if a {@link TestPlan} without explicit data sources and sinks can be executed.
	 */
	@Test
	public void expectedValuesShouldAlsoWorkWithAdhocInputAndOutput() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.run();
	}

	/**
	 * Tests if a {@link TestPlan} succeeds with values having the same key.
	 */
	@Test
	public void shouldMatchValuesWithSameKey() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		// randomize values
		testPlan.getInput().
			add(new PactInteger(2), new PactString("test3")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test1")).
			add(new PactInteger(1), new PactString("test3")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2")).
			add(new PactInteger(1), new PactString("test3")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CrossContract}.
	 */
	@Test
	public void crossShouldBeSupported() {
		CrossContract crossContract = new CrossContract(CartesianProduct.class);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput(0).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getInput(1).
			add(new PactInteger(3), new PactString("test3")).
			add(new PactInteger(4), new PactString("test4"));

		testPlan.getExpectedOutput(PactInteger.class, PactInteger.class, PactString.class, PactString.class).
			add(new PactInteger(1), new PactInteger(3), new PactString("test1"), new PactString("test3")).
			add(new PactInteger(1), new PactInteger(4), new PactString("test1"), new PactString("test4")).
			add(new PactInteger(2), new PactInteger(3), new PactString("test2"), new PactString("test3")).
			add(new PactInteger(2), new PactInteger(4), new PactString("test2"), new PactString("test4"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CoGroupContract}.
	 */
	@Test
	public void coGroupShouldBeSupported() {
		CoGroupContract crossContract = new CoGroupContract(AppendingCoGroup.class, PactInteger.class, 0, 0);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput(0).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3"));
		testPlan.getInput(1).
			add(new PactInteger(1), new PactString("test4")).
			add(new PactInteger(3), new PactString("test5"));

		testPlan.getExpectedOutput(PactInteger.class, StringList.class).
			add(new PactInteger(1), new StringList("test1", "test2", "test4")).
			add(new PactInteger(2), new StringList("test3")).
			add(new PactInteger(3), new StringList("test5"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CoGroupContract}.
	 */
	@Test
	public void matchShouldBeSupported() {
		MatchContract crossContract = new MatchContract(Join.class, PactInteger.class, 0, 0);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput(0).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3"));
		testPlan.getInput(1).
			add(new PactInteger(1), new PactString("test4")).
			add(new PactInteger(3), new PactString("test5"));

		testPlan.getExpectedOutput(PactInteger.class, PactString.class, PactString.class).
			add(new PactInteger(1), new PactString("test1"), new PactString("test4")).
			add(new PactInteger(1), new PactString("test2"), new PactString("test4"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CoGroupContract}.
	 */
	@Test
	public void reduceShouldBeSupported() {
		ReduceContract crossContract = new ReduceContract(AppendingReduce.class, PactInteger.class, 0);

		TestPlan testPlan = new TestPlan(crossContract);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3")).
			add(new PactInteger(1), new PactString("test4")).
			add(new PactInteger(3), new PactString("test5"));

		testPlan.getExpectedOutput(PactInteger.class, StringList.class).
			add(new PactInteger(1), new StringList("test1", "test2", "test4")).
			add(new PactInteger(2), new StringList("test3")).
			add(new PactInteger(3), new StringList("test5"));
		testPlan.run();
	}

	/**
	 * Tests a {@link TestPlan} with a {@link CrossContract}.
	 */
	@Test
	public void settingValuesShouldWorkWithSourceContracts() {
		CrossContract crossContract = new CrossContract(CartesianProduct.class);

		TestPlan testPlan = new TestPlan(crossContract);
		// first and second input are added in TestPlan
		testPlan.getInput((GenericDataSource<?>) crossContract.getFirstInputs().get(0)).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getInput((GenericDataSource<?>) crossContract.getSecondInputs().get(0)).
			add(new PactInteger(3), new PactString("test3")).
			add(new PactInteger(4), new PactString("test4"));

		testPlan.getExpectedOutput(PactInteger.class, PactInteger.class, PactString.class, PactString.class).
			add(new PactInteger(1), new PactInteger(3), new PactString("test1"), new PactString("test3")).
			add(new PactInteger(1), new PactInteger(4), new PactString("test1"), new PactString("test4")).
			add(new PactInteger(2), new PactInteger(3), new PactString("test2"), new PactString("test3")).
			add(new PactInteger(2), new PactInteger(4), new PactString("test2"), new PactString("test4"));
		testPlan.run();
	}

	/**
	 * Tests {@link TestPlan#setDegreeOfParallelism(int)}.
	 */
	@Test
	public void degreeOfParallelismShouldBeConfigurable() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.setDegreeOfParallelism(2);
		Assert.assertEquals(2, testPlan.getDegreeOfParallelism());
		testPlan.run();
		Assert.assertEquals(2, map.getDegreeOfParallelism());
	}

	/**
	 * Creates an output file in the temporary folder for arbitrary key/value pairs coming from the given input
	 * contract.
	 * 
	 * @param input
	 *        the input from which the values are read
	 * @param outputFormatClass
	 *        the output format
	 * @return the {@link FileDataSinkContract} for the temporary file
	 */
	private FileDataSink createOutput(final Contract input, final Class<? extends FileOutputFormat> outputFormatClass) {
		try {
			final FileDataSink out = new FileDataSink(outputFormatClass, File.createTempFile(
				"output", null).toURI().toString(), "Output");
			out.setInput(input);
			return out;
		} catch (IOException e) {
			fail("cannot create temporary output file" + e);
			return null;
		}
	}

	/**
	 * Creates an {@link FileDataSource} contract for the specified resource file in the temporary folder for
	 * arbitrary key/value pairs coming from the given input
	 * contract.
	 * 
	 * @param input
	 *        the input from which the values are read
	 * @return the {@link FileDataSinkContract} for the temporary file
	 */
	private FileDataSource createInput(Class<? extends FileInputFormat> inputFormat, String resource) {
		final FileDataSource read = new FileDataSource(inputFormat,
			getResourcePath(resource),
			"Input");
		return read;
	}

	/**
	 * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	 * key string is tokenized by spaces. For each token a new
	 * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	 * an Integer(1) is the value.
	 */
	public static class TokenizeLine extends MapStub {
		private static Pattern WORD_PATTERN = Pattern.compile("\\w+");

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.MapStub#map(eu.stratosphere.pact.common.type.PactRecord,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			PactString line = record.getField(0, PactString.class);
			Matcher matcher = WORD_PATTERN.matcher(line.getValue());
			while (matcher.find())
				out.collect(new PactRecord(new PactString(matcher.group(0).toLowerCase()), new PactInteger(1)));
		}
	}

	/**
	 * Counts the number of values for a given key. Hence, the number of
	 * occurences of a given token (word) is computed and emitted. The key is
	 * not modified, hence a SameKey OutputContract is attached to this class.
	 */
	@Combinable
	public static class CountWords extends ReduceStub {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.ReduceStub#reduce(java.util.Iterator,
		 * eu.stratosphere.pact.common.stubs.Collector)
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			PactRecord result = records.next().createCopy();
			int sum = result.getField(1, PactInteger.class).getValue();
			while (records.hasNext())
				sum += records.next().getField(1, PactInteger.class).getValue();
			result.setField(1, new PactInteger(sum));
			out.collect(result);
		}
	}

	/**
	 * Tests if a {@link TestPlan} with two stubs can be executed.
	 */
	@Test
	public void complexTestPassesWithExpectedValues() {
		final MapContract tokenize = new MapContract(TokenizeLine.class, "Map");
		final ReduceContract summing = new ReduceContract(CountWords.class, PactString.class, 0, "Reduce");
		summing.setInput(tokenize);

		TestPlan testPlan = new TestPlan(summing);
		String[] lines =
			{
				"Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
				"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
				"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
				"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum." };
		for (String line : lines)
			testPlan.getInput().add(new PactString(line));

		String[] singleWords = { "voluptate", "veniam", "velit", "ullamco", "tempor", "sunt", "sit", "sint", "sed",
			"reprehenderit", "quis", "qui", "proident", "pariatur", "officia", "occaecat", "nulla", "nostrud", "non",
			"nisi", "mollit", "minim", "magna", "lorem", "laborum", "laboris", "labore", "irure", "ipsum",
			"incididunt", "id", "fugiat", "exercitation", "excepteur", "ex", "eu", "et", "est", "esse", "enim", "elit",
			"eiusmod", "ea", "duis", "do", "deserunt", "cupidatat", "culpa", "consequat", "consectetur", "commodo",
			"cillum", "aute", "anim", "amet", "aliquip", "aliqua", "adipisicing", "ad" };
		for (String singleWord : singleWords)
			testPlan.getExpectedOutput(PactString.class, PactInteger.class).
				add(new PactString(singleWord), new PactInteger(1));
		testPlan.getExpectedOutput(PactString.class, PactInteger.class).
			add(new PactString("ut"), new PactInteger(3)).
			add(new PactString("in"), new PactInteger(3)).
			add(new PactString("dolore"), new PactInteger(2)).
			add(new PactString("dolor"), new PactInteger(2));
		testPlan.run();
	}

	/**
	 * Tests if a {@link TestPlan} fails if the actual values do not match the expected values.
	 */
	@Test
	public void shouldFailIfExpectedAndActualValuesDiffer() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test3"));
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if a {@link TestPlan} fails there are too many values.
	 */
	@Test
	public void shouldFailIfTooManyValues() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1"));
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if a {@link TestPlan} fails there are too few values.
	 */
	@Test
	public void shouldFailIfTooFewValues() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2")).
			add(new PactInteger(3), new PactString("test3"));
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if a {@link TestPlan} fails there are too many values.
	 */
	@Test
	public void shouldFailIfPactThrowsException() {
		final MapContract map = new MapContract(ErroneousPact.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			setEmpty();
		assertTestRunFails(testPlan);
	}

	/**
	 * Fails if the test plan does not fail.
	 * 
	 * @param testPlan
	 *        the test plan expected to fail
	 */
	public static void assertTestRunFails(TestPlan testPlan) {
		boolean failed = false;
		try {
			testPlan.run();
		} catch (AssertionError error) {
			failed = true;
		}
		assertTrue("Test plan should have failed", failed);
	}

	/**
	 * Tests if a {@link TestPlan} succeeds with uninitialized expected values.
	 */
	@Test
	public void shouldSucceedIfNoExpectedValues() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.run();
	}

	/**
	 * Tests if a {@link TestPlan} succeeds with values having the same key.
	 */
	@Test
	public void shouldFailWithEqualValuesWithSameKey() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(1), new PactString("test2")).
			add(new PactInteger(1), new PactString("test3")).
			add(new PactInteger(2), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2")).
			add(new PactInteger(2), new PactString("test3"));
		// randomize values
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2")).
			add(new PactInteger(1), new PactString("test3")).
			add(new PactInteger(1), new PactString("test3")). // <-- duplicate
			add(new PactInteger(2), new PactString("test3"));
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if a {@link TestPlan} fails if actual values appear but empty values are expected.
	 */
	@Test
	public void shouldFailIfNonEmptyExpectedValues() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan = new TestPlan(map);
		testPlan.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan.getExpectedOutput(PactInteger.class, PactString.class).setEmpty();
		assertTestRunFails(testPlan);
	}

	/**
	 * Tests if the outputs of two {@link TestPlan}s can be successfully compared.
	 */
	@Test
	public void shouldCompareTwoTestPlans() {
		final MapContract map = new MapContract(IdentityMap.class,
			"Map");
		TestPlan testPlan1 = new TestPlan(map);
		testPlan1.getInput().
			add(new PactInteger(1), new PactString("test1")).
			add(new PactInteger(2), new PactString("test2"));
		testPlan1.run();
		TestPlan testPlan2 = new TestPlan(map);
		testPlan2.getInput().
			add(new PactInteger(2), new PactString("test2")).
			add(new PactInteger(1), new PactString("test1"));
		testPlan2.run();

		testPlan1.getActualOutput().setSchema(IntStringPair);
		testPlan2.getActualOutput().setSchema(IntStringPair);
		AssertUtil.assertIteratorEquals(testPlan1.getActualOutput().iterator(),
			testPlan2.getActualOutput().iterator(),
			new PactRecordEqualer(IntStringPair));
	}
}
