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
import java.util.Collection;

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
}
