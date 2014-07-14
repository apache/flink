/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Test;

import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.types.TypeInformation;

public class TypeExtractTest {

	public static class MySuperlass<T> implements Serializable {
		private static final long serialVersionUID = 1L;

	}

	public static class Myclass extends MySuperlass<Integer> {

		private static final long serialVersionUID = 1L;

	}

	@Test
	public void test() throws IOException, ClassNotFoundException {

		Myclass f = new Myclass();

		TypeInformation<?> ts = TypeExtractor.createTypeInfo(MySuperlass.class, f.getClass(), 0,
				null, null);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos;

		oos = new ObjectOutputStream(baos);
		oos.writeObject(f);

		ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));

		assertTrue(true);
	}

}
