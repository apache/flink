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
package eu.stratosphere.api.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.test.util.RecordAPITestBase;
import org.apache.avro.reflect.Nullable;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.record.operators.GenericDataSink;
import eu.stratosphere.api.java.record.operators.GenericDataSource;
import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.api.java.record.io.GenericInputFormat;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class AvroWithEmptyArrayITCase extends RecordAPITestBase {

	@Override
	protected Plan getTestJob() {
		GenericDataSource<RandomInputFormat> bookSource = new GenericDataSource<RandomInputFormat>(
			new RandomInputFormat(true));
		GenericDataSource<RandomInputFormat> authorSource = new GenericDataSource<RandomInputFormat>(
			new RandomInputFormat(false));

		CoGroupOperator coGroupOperator = CoGroupOperator.builder(MyCoGrouper.class, LongValue.class, 0, 0)
			.input1(bookSource).input2(authorSource).name("CoGrouper Test").build();

		GenericDataSink sink = new GenericDataSink(PrintingOutputFormat.class, coGroupOperator);

		Plan plan = new Plan(sink, "CoGroper Test Plan");
		plan.setDefaultParallelism(1);
		return plan;
	}

	public static class SBookAvroValue extends AvroBaseValue<Book> {
		private static final long serialVersionUID = 1L;

		public SBookAvroValue() {}

		public SBookAvroValue(Book datum) {
			super(datum);
		}
	}

	public static class Book {

		long bookId;
		@Nullable
		String title;
		long authorId;

		public Book() {
		}

		public Book(long bookId, String title, long authorId) {
			this.bookId = bookId;
			this.title = title;
			this.authorId = authorId;
		}
	}

	public static class SBookAuthorValue extends AvroBaseValue<BookAuthor> {
		private static final long serialVersionUID = 1L;

		public SBookAuthorValue() {}

		public SBookAuthorValue(BookAuthor datum) {
			super(datum);
		}
	}

	public static class BookAuthor {

		enum BookType {
			book,
			article,
			journal
		}

		long authorId;

		@Nullable
		List<String> bookTitles;

		@Nullable
		List<Book> books;

		String authorName;

		BookType bookType;

		public BookAuthor() {}

		public BookAuthor(long authorId, List<String> bookTitles, String authorName) {
			this.authorId = authorId;
			this.bookTitles = bookTitles;
			this.authorName = authorName;
		}
	}

	public static class RandomInputFormat extends GenericInputFormat {
		private static final long serialVersionUID = 1L;

		private final boolean isBook;

		private boolean touched = false;

		public RandomInputFormat(boolean isBook) {
			this.isBook = isBook;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			return touched;
		}

		@Override
		public Record nextRecord(Record record) throws IOException {
			touched = true;
			record.setField(0, new LongValue(26382648));

			if (isBook) {
				Book b = new Book(123, "This is a test book", 26382648);
				record.setField(1, new SBookAvroValue(b));
			} else {
				List<String> titles = new ArrayList<String>();
				// titles.add("Title1");
				// titles.add("Title2");
				// titles.add("Title3");

				List<Book> books = new ArrayList<Book>();
				books.add(new Book(123, "This is a test book", 1));
				books.add(new Book(24234234, "This is a test book", 1));
				books.add(new Book(1234324, "This is a test book", 3));

				BookAuthor a = new BookAuthor(1, titles, "Test Author");
				a.books = books;
				a.bookType = BookAuthor.BookType.journal;
				record.setField(1, new SBookAuthorValue(a));
			}

			return record;
		}
	}

	public static final class PrintingOutputFormat implements OutputFormat<Record> {

		private static final long serialVersionUID = 1L;

		@Override
		public void configure(Configuration parameters) {}

		@Override
		public void open(int taskNumber, int numTasks) {}

		@Override
		public void writeRecord(Record record) throws IOException {
			long key = record.getField(0, LongValue.class).getValue();
			String val = record.getField(1, StringValue.class).getValue();
			System.out.println(key + " : " + val);
		}

		@Override
		public void close() {}
	}

	public static class MyCoGrouper extends CoGroupFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void coGroup(Iterator<Record> records1, Iterator<Record> records2, Collector<Record> out)
				throws Exception {

			Record r1 = null;
			if (records1.hasNext()) {
				r1 = records1.next();
			}
			Record r2 = null;
			if (records2.hasNext()) {
				r2 = records2.next();
			}

			if (r1 != null) {
				r1.getField(1, SBookAvroValue.class).datum();
			}

			if (r2 != null) {
				r2.getField(1, SBookAuthorValue.class).datum();
			}
		}
	}
}
