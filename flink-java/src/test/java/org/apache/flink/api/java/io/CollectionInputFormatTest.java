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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link CollectionInputFormat}.
 */
public class CollectionInputFormatTest {

	private static class ElementType {
		private final int id;

		public ElementType(){
			this(-1);
		}

		public ElementType(int id){
			this.id = id;
		}

		public int getId() {
			return id;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ElementType) {
				ElementType et = (ElementType) obj;
				return et.getId() == this.getId();
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return id;
		}

		@Override
		public String toString() {
			return "ElementType{" +
				"id=" + id +
				'}';
		}
	}

	@Test
	public void testSerializability() {
		try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(buffer)) {

			Collection<ElementType> inputCollection = new ArrayList<ElementType>();
			ElementType element1 = new ElementType(1);
			ElementType element2 = new ElementType(2);
			ElementType element3 = new ElementType(3);
			inputCollection.add(element1);
			inputCollection.add(element2);
			inputCollection.add(element3);

			@SuppressWarnings("unchecked")
			TypeInformation<ElementType> info = (TypeInformation<ElementType>) TypeExtractor.createTypeInfo(ElementType.class);

			CollectionInputFormat<ElementType> inputFormat = new CollectionInputFormat<ElementType>(inputCollection,
					info.createSerializer(new ExecutionConfig()));

			out.writeObject(inputFormat);

			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()));

			Object serializationResult = in.readObject();

			assertNotNull(serializationResult);
			assertTrue(serializationResult instanceof CollectionInputFormat<?>);

			@SuppressWarnings("unchecked")
			CollectionInputFormat<ElementType> result = (CollectionInputFormat<ElementType>) serializationResult;

			GenericInputSplit inputSplit = new GenericInputSplit(0, 1);
			inputFormat.open(inputSplit);
			result.open(inputSplit);

			while (!inputFormat.reachedEnd() && !result.reachedEnd()){
				ElementType expectedElement = inputFormat.nextRecord(null);
				ElementType actualElement = result.nextRecord(null);

				assertEquals(expectedElement, actualElement);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}

	}

	@Test
	public void testSerializabilityStrings() {

		final String[] data = new String[] {
				"To be, or not to be,--that is the question:--",
				"Whether 'tis nobler in the mind to suffer",
				"The slings and arrows of outrageous fortune",
				"Or to take arms against a sea of troubles,",
				"And by opposing end them?--To die,--to sleep,--",
				"No more; and by a sleep to say we end",
				"The heartache, and the thousand natural shocks",
				"That flesh is heir to,--'tis a consummation",
				"Devoutly to be wish'd. To die,--to sleep;--",
				"To sleep! perchance to dream:--ay, there's the rub;",
				"For in that sleep of death what dreams may come,",
				"When we have shuffled off this mortal coil,",
				"Must give us pause: there's the respect",
				"That makes calamity of so long life;",
				"For who would bear the whips and scorns of time,",
				"The oppressor's wrong, the proud man's contumely,",
				"The pangs of despis'd love, the law's delay,",
				"The insolence of office, and the spurns",
				"That patient merit of the unworthy takes,",
				"When he himself might his quietus make",
				"With a bare bodkin? who would these fardels bear,",
				"To grunt and sweat under a weary life,",
				"But that the dread of something after death,--",
				"The undiscover'd country, from whose bourn",
				"No traveller returns,--puzzles the will,",
				"And makes us rather bear those ills we have",
				"Than fly to others that we know not of?",
				"Thus conscience does make cowards of us all;",
				"And thus the native hue of resolution",
				"Is sicklied o'er with the pale cast of thought;",
				"And enterprises of great pith and moment,",
				"With this regard, their currents turn awry,",
				"And lose the name of action.--Soft you now!",
				"The fair Ophelia!--Nymph, in thy orisons",
				"Be all my sins remember'd."
		};

		try {
			List<String> inputCollection = Arrays.asList(data);
			CollectionInputFormat<String> inputFormat = new CollectionInputFormat<String>(inputCollection, BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()));

			// serialize
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(inputFormat);
			oos.close();

			// deserialize
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);
			Object result = ois.readObject();

			assertTrue(result instanceof CollectionInputFormat);

			int i = 0;
			@SuppressWarnings("unchecked")
			CollectionInputFormat<String> in = (CollectionInputFormat<String>) result;
			in.open(new GenericInputSplit(0, 1));

			while (!in.reachedEnd()) {
				assertEquals(data[i++], in.nextRecord(""));
			}

			assertEquals(data.length, i);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerializationFailure() {
		try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(buffer)) {
			// a mock serializer that fails when writing
			CollectionInputFormat<ElementType> inFormat = new CollectionInputFormat<ElementType>(
					Collections.singleton(new ElementType()), new TestSerializer(false, true));

			try {
				out.writeObject(inFormat);
				fail("should throw an exception");
			}
			catch (TestException e) {
				// expected
			}
			catch (Exception e) {
				fail("Exception not properly forwarded");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testDeserializationFailure() {
		try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(buffer)) {
			// a mock serializer that fails when writing
			CollectionInputFormat<ElementType> inFormat = new CollectionInputFormat<ElementType>(
					Collections.singleton(new ElementType()), new TestSerializer(true, false));

			out.writeObject(inFormat);
			out.close();

			ByteArrayInputStream bais = new ByteArrayInputStream(buffer.toByteArray());
			ObjectInputStream in = new ObjectInputStream(bais);

			try {
				in.readObject();
				fail("should throw an exception");
			}
			catch (Exception e) {
				assertTrue(e.getCause() instanceof TestException);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testToStringOnSmallCollection() {
		ArrayList<ElementType> smallList = new ArrayList<>();
		smallList.add(new ElementType(1));
		smallList.add(new ElementType(2));
		CollectionInputFormat<ElementType> inputFormat = new CollectionInputFormat<>(
			smallList,
			new TestSerializer(true, false)
		);

		assertEquals("[ElementType{id=1}, ElementType{id=2}]", inputFormat.toString());
	}

	@Test
	public void testToStringOnBigCollection() {
		ArrayList<ElementType> list = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			list.add(new ElementType(i));
		}
		CollectionInputFormat<ElementType> inputFormat = new CollectionInputFormat<>(
			list,
			new TestSerializer(true, false)
		);

		assertEquals(
			"[ElementType{id=0}, ElementType{id=1}, ElementType{id=2}, " +
			"ElementType{id=3}, ElementType{id=4}, ElementType{id=5}, ...]",
			inputFormat.toString());
	}

	private static class TestException extends IOException{
		private static final long serialVersionUID = 1L;
	}

	private static class TestSerializer extends TypeSerializer<ElementType> {

		private static final long serialVersionUID = 1L;

		private final boolean failOnRead;
		private final boolean failOnWrite;

		public TestSerializer(boolean failOnRead, boolean failOnWrite) {
			this.failOnRead = failOnRead;
			this.failOnWrite = failOnWrite;
		}

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TestSerializer duplicate() {
			return this;
		}

		@Override
		public ElementType createInstance() {
			return new ElementType();
		}

		@Override
		public ElementType copy(ElementType from) {
			return from;
		}

		@Override
		public ElementType copy(ElementType from, ElementType reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return 4;
		}

		@Override
		public void serialize(ElementType record, DataOutputView target) throws IOException {
			if (failOnWrite) {
				throw new TestException();
			}
			target.writeInt(record.getId());
		}

		@Override
		public ElementType deserialize(DataInputView source) throws IOException {
			if (failOnRead) {
				throw new TestException();
			}
			return new ElementType(source.readInt());
		}

		@Override
		public ElementType deserialize(ElementType reuse, DataInputView source) throws IOException {
			if (failOnRead) {
				throw new TestException();
			}
			return new ElementType(source.readInt());
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeInt(source.readInt());
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TestSerializer) {
				TestSerializer other = (TestSerializer) obj;

				return other.canEqual(this) && failOnRead == other.failOnRead && failOnWrite == other.failOnWrite;
			} else {
				return false;
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof TestSerializer;
		}

		@Override
		public int hashCode() {
			return Objects.hash(failOnRead, failOnWrite);
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CompatibilityResult<ElementType> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			throw new UnsupportedOperationException();
		}
	}
}
