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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.internal.ArrayComparisonFailure;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.util.StringUtils;
import eu.stratosphere.pact.common.io.FileInputFormat;
import eu.stratosphere.pact.common.io.FormatUtil;
import eu.stratosphere.pact.common.io.SequentialOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.ReduceTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.KeyComparator;

/**
 * Represents the input or output values of a {@link TestPlan}. The class is
 * especially important when setting the expected values in the TestPlan.<br>
 * <br>
 * There are two ways to specify the values:
 * <ol>
 * <li>From a file: with {@link #fromFile(Class, String)} and {@link #fromFile(Class, String, Configuration)} the
 * location, format, and configuration of the data can be specified. The file is lazily loaded and thus can be
 * comparable large.
 * <li>Ad-hoc: key/value records can be added with {@link #add(Key, Value)}, {@link #add(KeyValuePair...)}, and
 * {@link #add(Iterable)}. Please note that the actual amount of records is quite for a test case as the TestPlan
 * already involves a certain degree of overhead.<br>
 * <br>
 * TestPairs are directly comparable with equals and hashCode based on its content. Please note that in the case of
 * large file-based TestPairs, the time needed to compute the {@link #hashCode()} or to compare two instances with
 * {@link #equals(Object)} can become quite long. Currently, the comparison result is order-dependent as TestPairs are
 * interpreted as a list.<br>
 * <br>
 * Currently there is no notion of an empty set of records.
 * 
 * @author Arvid Heise
 * @param <K>
 *        the type of the keys
 * @param <V>
 *        the type of the values
 */
public class TestRecords implements Closeable, Iterable<PactRecord> {
	private static final class TestPairsReader implements MutableObjectIterator<PactRecord> {
		private final InputFileIterator inputFileIterator;

		private TestPairsReader(final InputFileIterator inputFileIterator) {
			this.inputFileIterator = inputFileIterator;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.util.MutableObjectIterator#next(java.lang.Object)
		 */
		@Override
		public boolean next(PactRecord target) throws IOException {
			if (this.inputFileIterator.hasNext()) {
				this.inputFileIterator.next().copyTo(target);
				return true;
			}
			return false;
		}

	}

	private static final Iterator<PactRecord> EMPTY_ITERATOR = new ArrayList<PactRecord>().iterator();

	private static final Comparator<Key> KeyComparator = new KeyComparator();

	private Configuration configuration;

	private Class<? extends FileInputFormat> inputFormatClass;

	private final List<PactRecord> records = new ArrayList<PactRecord>();

	private String path;

	private ClosableManager closableManager = new ClosableManager();

	private boolean empty;

	static class SortInfo implements Cloneable {
		IntList sortKeys = new IntArrayList();

		List<Class<? extends Key>> keyClasses = new ArrayList<Class<? extends Key>>();

		private List<Comparator<Key>> comparators = new ArrayList<Comparator<Key>>();

		public SortInfo(IntList sortKeys, List<Class<? extends Key>> keyClasses,
				List<? extends Comparator<Key>> comparators) {
			this.sortKeys.addAll(sortKeys);
			this.keyClasses.addAll(keyClasses);
			this.comparators.addAll(comparators);
		}

		public SortInfo copy() {
			return new SortInfo(new IntArrayList(this.sortKeys), new ArrayList<Class<? extends Key>>(this.keyClasses),
				new ArrayList<Comparator<Key>>(this.comparators));
		}

		public void remove(int removeIndex) {
			for (int index = 0; index < this.sortKeys.size(); index++)
				if (this.sortKeys.get(index) == removeIndex) {
					this.sortKeys.remove(index);
					this.keyClasses.remove(index);
					this.comparators.remove(index);
				}
		}

		/**
		 * Initializes TestPairs.SortInfo.
		 */
		public SortInfo(IntList sortKeys, List<Class<? extends Key>> keyClasses) {
			this(sortKeys, keyClasses, Arrays.asList(new KeyComparator[keyClasses.size()]));
			Collections.fill(this.comparators, KeyComparator);
		}
	}

	SortInfo sortInfo;

	private Class<? extends Value>[] schema;

	private Value[] emptyTuple;

	public TestRecords(Class<? extends Value>[] schema) {
		this.schema = schema;
		this.emptyTuple = new Value[this.schema.length];
		for (int index = 0; index < this.schema.length; index++)
			this.emptyTuple[index] = InstantiationUtil.instantiate(this.schema[index], Value.class);
		this.sortInfo = this.inferInfo();
	}

	/**
	 * Initializes TestPairs.
	 */
	@SuppressWarnings("unchecked")
	public TestRecords() {
		this.schema = new Class[0];
		this.emptyTuple = new Value[0];
	}

	private boolean isEmpty() {
		return this.empty;
	}

	private void setEmpty(boolean empty) {
		this.empty = empty;
	}

	/**
	 * Specifies that the set of key/value records is empty. This method is primarily used to distinguish between an
	 * empty
	 * uninitialized set and a set deliberately left empty. Further calls to {@link #fromFile(Class, String)} or
	 * {@link #add(Iterable)} will reset the effect of this method invocation and vice-versa.
	 */
	public void setEmpty() {
		this.setEmpty(true);
		this.inputFormatClass = null;
		this.records.clear();
	}

	/**
	 * Adds several records at once.
	 * 
	 * @param records
	 *        the records to add
	 * @return this
	 */
	public TestRecords add(final Iterable<? extends PactRecord> records) {
		for (final PactRecord record : records)
			this.records.add(record);
		this.setEmpty(false);
		this.inputFormatClass = null;
		return this;
	}

	/**
	 * Adds several records at once.
	 * 
	 * @param records
	 *        the records to add
	 * @return this
	 */
	public TestRecords add(final TestRecords records) {
		if (records.isEmpty())
			this.setEmpty();
		else {
			for (final PactRecord record : records)
				this.records.add(record);
			this.setEmpty(false);
			records.close();
		}
		return this;
	}

	/**
	 * Adds several records at once.
	 * 
	 * @param records
	 *        the records to add
	 * @return this
	 */
	public TestRecords add(final PactRecord... records) {
		for (final PactRecord record : records)
			this.records.add(record);
		this.setEmpty(false);
		return this;
	}

	/**
	 * Adds a records.
	 * 
	 * @param fields
	 *        the fields of the record
	 * @return this
	 */
	public TestRecords add(final Value... values) {
		PactRecord record = new PactRecord(values.length);
		for (int index = 0; index < values.length; index++)
			record.setField(index, values[index]);
		this.records.add(record);
		this.setEmpty(false);
		return this;
	}

	private void assignMemory(final TaskConfig config, final int memSize) {
		// set the config
		config.setMemorySize(memSize * 1024L * 1024L);
		config.setNumFilehandles(DEFAUTL_MERGE_FACTOR);
	}

	/**
	 * Uses {@link UnilateralSortMerger} to sort the files of the {@link SplitInputIterator}.
	 */
	private Iterator<PactRecord> createSortedIterator(final InputFileIterator inputFileIterator, SortInfo info) {
		final TaskConfig config = new TaskConfig(GlobalConfiguration.getConfiguration());
		this.assignMemory(config, 10);

		// set up memory and io parameters
		final long totalMemory = config.getMemorySize();
		final int numFileHandles = config.getNumFilehandles();

		try {
			final StringBuilder testName = new StringBuilder();
			StackTraceElement[] stackTrace = new Throwable().getStackTrace();
			for (int index = stackTrace.length - 1; index > 0; index--)
				if (stackTrace[index].getClassName().contains("Test"))
					testName.append(stackTrace[index].toString());
			// instantiate a sort-merger
			AbstractTask parentTask = new ReduceTask<PactRecord, PactRecord>() {
				@Override
				public String toString() {
					return "TestPair Sorter " + testName;
				}
			};

			if (info == null)
				return inputFileIterator;
			@SuppressWarnings("unchecked")
			final PactRecordComparator pactRecordComparator = new PactRecordComparator(info.sortKeys.toIntArray(),
				info.keyClasses.toArray(new Class[0]));
			final UnilateralSortMerger<PactRecord> sortMerger =
				new UnilateralSortMerger<PactRecord>(MockTaskManager.INSTANCE.getMemoryManager(),
					MockTaskManager.INSTANCE.getIoManager(), new TestPairsReader(inputFileIterator), parentTask,
					PactRecordSerializer.get(), pactRecordComparator, totalMemory, numFileHandles, 0.7f);
			this.closableManager.add(sortMerger);

			// obtain and return a grouped iterator from the sort-merger
			return new ImmutableRecordIterator(sortMerger.getIterator());
		} catch (final MemoryAllocationException mae) {
			throw new RuntimeException(
				"MemoryManager is not able to provide the required amount of memory for ReduceTask", mae);
		} catch (final IOException ioe) {
			throw new RuntimeException("IOException caught when obtaining SortMerger for ReduceTask", ioe);
		} catch (final InterruptedException iex) {
			throw new RuntimeException("InterruptedException caught when obtaining iterator over sorted data.", iex);
		}
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private SortInfo inferInfo() {
		IntList sortKeys = new IntArrayList();
		List<Class<? extends Key>> keyClasses = new ArrayList<Class<? extends Key>>();
		for (int fieldIndex = 0; fieldIndex < this.schema.length; fieldIndex++)
			if (Key.class.isAssignableFrom(this.schema[fieldIndex])) {
				keyClasses.add((Class<? extends Key>) this.schema[fieldIndex]);
				sortKeys.add(fieldIndex);
			}
		return new SortInfo(sortKeys, keyClasses);
	}

	@Override
	public void close() {
		try {
			this.closableManager.close();
		} catch (IOException e) {
		}
	}

	/**
	 * Asserts that the contained set of records is equal to the set of records of the given {@link TestPairs}.
	 * 
	 * @param expectedValues
	 *        the other TestPairs defining the expected result
	 * @throws ArrayComparisonFailure
	 *         if the sets differ
	 */
	public void assertEquals(final TestRecords expectedValues) throws ArrayComparisonFailure {
		this.assertEquals(expectedValues, new EqualityValueMatcher(),
			new Int2ObjectOpenHashMap<List<ValueSimilarity<?>>>());
	}

	static <T> T firstNonNull(T... elements) {
		for (int index = 0; index < elements.length; index++)
			if (elements[index] != null)
				return elements[index];
		return null;
	}

	/**
	 * Asserts that the contained set of records is fuzzy equal to the set of records of the given {@link TestPairs}.<br>
	 * Pairs from this and the given set with equal key are compared and matched using the provided
	 * {@link FuzzyTestValueMatcher} and its {@link FuzzyTestValueSimilarity} measure.
	 * 
	 * @param expectedValues
	 *        the other TestPairs defining the expected result
	 * @param fuzzyMatcher
	 *        the fuzzy match algorithm used to globally match the values of records with equal key
	 * @param fuzzySimilarity
	 *        the fuzzy similarity measure used by the matcher or null if supported by the fuzzyMatcher
	 * @throws ArrayComparisonFailure
	 *         if the sets differ
	 */
	public void assertEquals(final TestRecords expectedValues, FuzzyValueMatcher fuzzyMatcher,
			Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap) throws ArrayComparisonFailure {
		@SuppressWarnings("unchecked")
		Class<? extends Value>[] schema = firstNonNull(expectedValues.schema, this.schema);
		new TestRecordsAssertor(schema, fuzzyMatcher, this.canonalizeSimilarityMap(similarityMap, schema),
			firstNonNull(expectedValues.sortInfo, this.sortInfo), expectedValues, this).assertEquals();
	}

	/**
	 * Removes all values column and adds similarities where applicable
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Int2ObjectMap<List<ValueSimilarity<?>>> canonalizeSimilarityMap(
			Int2ObjectMap<List<ValueSimilarity<?>>> similarityMap, Class<? extends Value>[] schema) {
		if (similarityMap.containsKey(ALL_VALUES) && !similarityMap.get(ALL_VALUES).isEmpty()) {
			// add all similarities to individual lists instead
			similarityMap = new Int2ObjectOpenHashMap<List<ValueSimilarity<?>>>(similarityMap);
			List<ValueSimilarity<?>> allSimilarity = similarityMap.remove(ALL_VALUES);
			for (int index = 0; index < schema.length; index++) {
				List<ValueSimilarity<?>> similarities = similarityMap.get(index);
				if (similarities == null)
					similarityMap.put(index, similarities = new ArrayList<ValueSimilarity<?>>());
				for (ValueSimilarity sim : allSimilarity)
					if (sim.isApplicable(schema[index]))
						similarities.add(sim);
			}
		}

		// remove empty lists
		ObjectIterator<Entry<List<ValueSimilarity<?>>>> iterator = similarityMap.int2ObjectEntrySet().iterator();
		while (iterator.hasNext()) {
			Int2ObjectMap.Entry<List<ValueSimilarity<?>>> entry = iterator.next();
			if (entry.getValue().isEmpty())
				iterator.remove();
		}
		return similarityMap;
	}

	public final static int ALL_VALUES = -1;

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final TestRecords other = (TestRecords) obj;

		try {
			other.assertEquals(this);
		} catch (AssertionError e) {
			return false;
		}
		return true;
	}

	/**
	 * Initializes this {@link TestPairs} from the given file.
	 * 
	 * @param inputFormatClass
	 *        the class of the {@link FileInputFormat}
	 * @param file
	 *        the path to the file, can be relative
	 * @return this
	 */
	public TestRecords fromFile(final Class<? extends FileInputFormat> inputFormatClass, final String file) {
		this.fromFile(inputFormatClass, file, new Configuration());
		return this;
	}

	/**
	 * Initializes this {@link TestPairs} from the given file.
	 * 
	 * @param inputFormatClass
	 *        the class of the {@link FileInputFormat}
	 * @param file
	 *        the path to the file, can be relative
	 * @param configuration
	 *        the configuration for the {@link FileInputFormat}.
	 * @return this
	 */
	public TestRecords fromFile(final Class<? extends FileInputFormat> inputFormatClass, final String file,
			final Configuration configuration) {
		this.path = file;
		this.inputFormatClass = inputFormatClass;
		this.configuration = configuration;
		this.setEmpty(false);
		this.records.clear();
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final Iterator<PactRecord> iterator = this.iterator();
		while (iterator.hasNext())
			result = prime * result + iterator.next().hashCode();
		return result;
	}

	/**
	 * Returns true if any add method has been called at least one.
	 * 
	 * @return true if records were specified in an ad-hoc manner
	 */
	public boolean isAdhoc() {
		return !this.records.isEmpty();
	}

	/**
	 * Returns true if either records were added manually or with {@link #fromFile(Class, String, Configuration)}.
	 * 
	 * @return true if either records were added manually or with {@link #fromFile(Class, String, Configuration)}.
	 */
	public boolean isInitialized() {
		return this.isEmpty() || !this.records.isEmpty() || this.inputFormatClass != null;
	}

	/**
	 * Sets the schema to the specified value.
	 * 
	 * @param schema
	 *        the schema to set
	 */
	public void setSchema(Class<? extends Value> firstFieldType, Class<?>... otherFieldTypes) {
		this.schema = SchemaUtils.combineSchema(firstFieldType, otherFieldTypes);
		this.sortInfo = this.inferInfo();
	}

	public Class<? extends Value>[] getSchema() {
		return this.schema;
	}

	public void setSchema(Class<? extends Value>[] schema) {
		if (schema == null)
			throw new NullPointerException("schema must not be null");

		this.schema = schema;
		this.sortInfo = this.inferInfo();
	}

	@Override
	public Iterator<PactRecord> iterator() {
		return this.iterator(this.sortInfo);
	}

	public Iterator<PactRecord> iterator(final SortInfo info) {
		if (this.isEmpty())
			return EMPTY_ITERATOR;

		if (!this.isAdhoc() && this.inputFormatClass != null) {

			final InputFileIterator inputFileIterator = this.getInputFileIterator();

			if (!inputFileIterator.hasNext() || info == null)
				return inputFileIterator;

			return this.createSortedIterator(inputFileIterator, info);
		}

		if (info != null)
			Collections.sort(this.records, new Comparator<PactRecord>() {
				@Override
				public int compare(PactRecord o1, PactRecord o2) {
					for (int index = 0; index < info.keyClasses.size(); index++) {
						Key f1 = o1.getField(info.sortKeys.get(index), info.keyClasses.get(index));
						Key f2 = o2.getField(info.sortKeys.get(index), info.keyClasses.get(index));
						if (f1 == f2)
							continue;
						if (f1 == null)
							return -1;
						if (f2 == null)
							return 1;
						int comparison = info.comparators.get(index).compare(f1, f2);
						if (comparison != 0)
							return comparison;
					}

					return 0;
				}
			});
		return this.records.iterator();
	}

	protected InputFileIterator getInputFileIterator() {
		final InputFileIterator inputFileIterator;
		try {
			inputFileIterator = new InputFileIterator(FormatUtil.openAllInputs(this.inputFormatClass, this.path,
				this.configuration));
		} catch (final IOException e) {
			Assert.fail("reading values from " + this.path + ": " + StringUtils.stringifyException(e));
			return null;
		} catch (final Exception e) {
			Assert.fail("creating input format " + StringUtils.stringifyException(e));
			return null;
		}
		return inputFileIterator;
	}

	protected Iterator<PactRecord> getUnsortedIterator() {
		if (this.isEmpty())
			return EMPTY_ITERATOR;
		if (this.isAdhoc())
			return this.records.iterator();
		if (this.inputFormatClass != null)
			return this.getInputFileIterator();
		return EMPTY_ITERATOR;
	}

	/**
	 * Saves the data to the given path in an internal format.
	 * 
	 * @param path
	 *        the path to write to, may be relative
	 * @throws IOException
	 *         if an I/O error occurred
	 */
	public void saveToFile(final String path) throws IOException {
		final SequentialOutputFormat outputFormat = FormatUtil.openOutput(SequentialOutputFormat.class, path,
			null);

		final Iterator<PactRecord> iterator = this.iterator();
		while (iterator.hasNext())
			outputFormat.writeRecord(iterator.next());
		outputFormat.close();
	}

	@Override
	public String toString() {
		final StringBuilder stringBuilder = new StringBuilder("TestPairs: ");
		final Iterator<PactRecord> iterator = this.iterator(null);
		for (int index = 0; index < 10 && iterator.hasNext(); index++) {
			if (index > 0)
				stringBuilder.append("; ");
			if (this.schema.length > 0)
				stringBuilder.append(PactRecordUtil.stringify(iterator.next(), this.schema));
			else
				stringBuilder.append(iterator.next());
		}
		if (iterator.hasNext())
			stringBuilder.append("...");
		return stringBuilder.toString();
	}

	private static final int DEFAUTL_MERGE_FACTOR = 64; // the number of streams to merge at once
}