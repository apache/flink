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

package eu.stratosphere.api.java.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.core.io.GenericInputSplit;
import eu.stratosphere.types.TypeInformation;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class CollectionInputFormatTest {
	public static class ElementType{
		private int id;

		public ElementType(){
			this(-1);
		}

		public ElementType(int id){
			this.id = id;
		}

		public int getId(){return id;}

		@Override
		public boolean equals(Object obj){
			if(obj != null && obj instanceof ElementType){
				ElementType et = (ElementType) obj;

				return et.getId() == this.getId();
			}else {
				return false;
			}
		}
	}

	@Test
	public void testSerializability(){
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
				info.createSerializer());

		try{
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(buffer);

			out.writeObject(inputFormat);

			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(buffer.toByteArray()));

			Object serializationResult = in.readObject();

			assertNotNull(serializationResult);
			assertTrue(serializationResult instanceof CollectionInputFormat<?>);

			@SuppressWarnings("unchecked")
			CollectionInputFormat<ElementType> result = (CollectionInputFormat<ElementType>) serializationResult;

			GenericInputSplit inputSplit = new GenericInputSplit();
			inputFormat.open(inputSplit);
			result.open(inputSplit);

			while(!inputFormat.reachedEnd() && !result.reachedEnd()){
				ElementType expectedElement = inputFormat.nextRecord(null);
				ElementType actualElement = result.nextRecord(null);

				assertEquals(expectedElement, actualElement);
			}
		}catch(IOException ex){
			fail(ex.toString());
		}catch(ClassNotFoundException ex){
			fail(ex.toString());
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
			CollectionInputFormat<String> inputFormat = new CollectionInputFormat<String>(inputCollection, BasicTypeInfo.STRING_TYPE_INFO.createSerializer());
			
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
			in.open(new GenericInputSplit());
			
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
}
