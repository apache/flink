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

package eu.stratosphere.nephele.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import eu.stratosphere.nephele.types.Record;

/**
 * This class checks the functionality of the {@link UnionRecordReader} class.
 * 
 * @author mjsax
 */
public class UnionRecordReaderTest {

	/**
	 * Tests if the UnionRecordReader returns {@code false} in .hasNext() on an empty input.
	 * 
	 * Tests bug #212
	 * http://www.stratosphere.eu/projects/Stratosphere/ticket/212
	 * 
	 * @throws InterruptedException		If TimerThread throws InterruptedException in .join();
	 */
	@Test(timeout=3000)
	public void testTerminateOnEmptyInput() throws InterruptedException {
		@SuppressWarnings("unchecked")
		RecordReader<Record>[] r = new RecordReader[] { new RecordReader<Record>(new DummyTask(), null, 0), new RecordReader<Record>(new DummyTask(), null, 0)};
		UnionRecordReader<Record> reader = new UnionRecordReader<Record>(r);

		assertThat(reader.hasNext(), is(false));
	}

}
