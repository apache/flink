/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.serialization;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;
import eu.stratosphere.runtime.io.serialization.types.SerializationTestType;
import eu.stratosphere.runtime.io.serialization.types.SerializationTestTypeFactory;
import eu.stratosphere.runtime.io.serialization.types.Util;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PagedViewsTest {

	@Test
	public void testSequenceOfIntegersWithAlignedBuffers() {
		try {
			final int NUM_INTS = 1000000;

			testSequenceOfTypes(Util.randomRecords(NUM_INTS, SerializationTestTypeFactory.INT), 2048);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	public void testSequenceOfIntegersWithUnalignedBuffers() {
		try {
			final int NUM_INTS = 1000000;

			testSequenceOfTypes(Util.randomRecords(NUM_INTS, SerializationTestTypeFactory.INT), 2047);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	public void testRandomTypes() {
		try {
			final int NUM_TYPES = 100000;

			// test with an odd buffer size to force many unaligned cases
			testSequenceOfTypes(Util.randomRecords(NUM_TYPES), 57);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Test encountered an unexpected exception.");
		}
	}

	@Test
	public void testReadFully() {
		int bufferSize = 100;
		byte[] expected = new byte[bufferSize];
		new Random().nextBytes(expected);

		TestOutputView outputView = new TestOutputView(bufferSize);

		try {
			outputView.write(expected);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not write to TestOutputView.");
		}

		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[bufferSize];

		try {
			inputView.readFully(buffer);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertEquals(inputView.getCurrentPositionInSegment(), bufferSize);
		assertArrayEquals(expected, buffer);
	}

	@Test
	public void testReadFullyAcrossSegments() {
		int bufferSize = 100;
		int segmentSize = 30;
		byte[] expected = new byte[bufferSize];
		new Random().nextBytes(expected);

		TestOutputView outputView = new TestOutputView(segmentSize);

		try {
			outputView.write(expected);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not write to TestOutputView.");
		}

		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[bufferSize];

		try {
			inputView.readFully(buffer);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertEquals(inputView.getCurrentPositionInSegment(), bufferSize % segmentSize);
		assertArrayEquals(expected, buffer);
	}

	@Test
	public void testReadAcrossSegments() {
		int bufferSize = 100;
		int bytes2Write = 75;
		int segmentSize = 30;
		byte[] expected = new byte[bytes2Write];
		new Random().nextBytes(expected);

		TestOutputView outputView = new TestOutputView(segmentSize);

		try {
			outputView.write(expected);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not write to TestOutputView.");
		}

		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[bufferSize];
		int bytesRead = 0;

		try {
			bytesRead = inputView.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertEquals(bytes2Write, bytesRead);
		assertEquals(inputView.getCurrentPositionInSegment(), bytes2Write % segmentSize);

		byte[] tempBuffer = new byte[bytesRead];
		System.arraycopy(buffer,0,tempBuffer,0,bytesRead);
		assertArrayEquals(expected, tempBuffer);
	}

	@Test
	public void testEmptyingInputView() {
		int bufferSize = 100;
		int bytes2Write = 75;
		int segmentSize = 30;
		byte[] expected = new byte[bytes2Write];
		new Random().nextBytes(expected);

		TestOutputView outputView = new TestOutputView(segmentSize);

		try {
			outputView.write(expected);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not write to TestOutputView.");
		}

		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[bufferSize];
		int bytesRead = 0;

		try {
			bytesRead = inputView.read(buffer);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertEquals(bytes2Write, bytesRead);

		byte[] tempBuffer = new byte[bytesRead];
		System.arraycopy(buffer,0,tempBuffer,0,bytesRead);
		assertArrayEquals(expected, tempBuffer);

		try{
			bytesRead = inputView.read(buffer);
		}catch(IOException e){
			e.printStackTrace();
			fail("Unexpected exception: Input view should be empty and thus return -1.");
		}

		assertEquals(-1, bytesRead);
		assertEquals(inputView.getCurrentPositionInSegment(), bytes2Write % segmentSize);
	}

	@Test
	public void testReadFullyWithNotEnoughData() {
		int bufferSize = 100;
		int bytes2Write = 99;
		int segmentSize = 30;
		byte[] expected = new byte[bytes2Write];
		new Random().nextBytes(expected);

		TestOutputView outputView = new TestOutputView(segmentSize);

		try {
			outputView.write(expected);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not write to TestOutputView.");
		}

		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[bufferSize];
		boolean eofException = false;

		try {
			inputView.readFully(buffer);
		}catch(EOFException e){
			//Expected exception
			eofException = true;
		}
		catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertTrue("EOFException should have occurred.", eofException);

		int bytesRead = 0;

		try{
			bytesRead =inputView.read(buffer);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertEquals(-1, bytesRead);
	}

	@Test
	public void testReadFullyWithOffset(){
		int bufferSize = 100;
		int segmentSize = 30;
		byte[] expected = new byte[bufferSize];
		new Random().nextBytes(expected);

		TestOutputView outputView = new TestOutputView(segmentSize);

		try {
			outputView.write(expected);
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not write to TestOutputView.");
		}

		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[2*bufferSize];

		try {
			inputView.readFully(buffer, bufferSize, bufferSize);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertEquals(inputView.getCurrentPositionInSegment(), bufferSize % segmentSize);
		byte[] tempBuffer = new byte[bufferSize];
		System.arraycopy(buffer, bufferSize, tempBuffer,0, bufferSize);
		assertArrayEquals(expected, tempBuffer);
	}

	@Test
	public void testReadFullyEmptyView(){
		int segmentSize = 30;
		TestOutputView outputView = new TestOutputView(segmentSize);
		outputView.close();

		TestInputView inputView = new TestInputView(outputView.segments);
		byte[] buffer = new byte[segmentSize];
		boolean eofException = false;

		try{
			inputView.readFully(buffer);
		}catch(EOFException e){
			//expected Exception
			eofException = true;
		}catch(Exception e){
			e.printStackTrace();
			fail("Unexpected exception: Could not read TestInputView.");
		}

		assertTrue("EOFException expected.", eofException);
	}


	private static void testSequenceOfTypes(Iterable<SerializationTestType> sequence, int segmentSize) throws Exception {

		List<SerializationTestType> elements = new ArrayList<SerializationTestType>(512);
		TestOutputView outView = new TestOutputView(segmentSize);

		// write
		for (SerializationTestType type : sequence) {
			// serialize the record
			type.write(outView);
			elements.add(type);
		}
		outView.close();

		// check the records
		TestInputView inView = new TestInputView(outView.segments);

		for (SerializationTestType reference : elements) {
			SerializationTestType result = reference.getClass().newInstance();
			result.read(inView);
			assertEquals(reference, result);
		}
	}

	// ============================================================================================

	private static final class SegmentWithPosition {

		private final MemorySegment segment;
		private final int position;

		public SegmentWithPosition(MemorySegment segment, int position) {
			this.segment = segment;
			this.position = position;
		}
	}

	private static final class TestOutputView extends AbstractPagedOutputView {

		private final List<SegmentWithPosition> segments = new ArrayList<SegmentWithPosition>();

		private final int segmentSize;

		private TestOutputView(int segmentSize) {
			super(new MemorySegment(new byte[segmentSize]), segmentSize, 0);

			this.segmentSize = segmentSize;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
			segments.add(new SegmentWithPosition(current, positionInCurrent));
			return new MemorySegment(new byte[segmentSize]);
		}

		public void close() {
			segments.add(new SegmentWithPosition(getCurrentSegment(), getCurrentPositionInSegment()));
		}
	}

	private static final class TestInputView extends AbstractPagedInputView {

		private final List<SegmentWithPosition> segments;

		private int num;


		private TestInputView(List<SegmentWithPosition> segments) {
			super(segments.get(0).segment, segments.get(0).position, 0);

			this.segments = segments;
			this.num = 0;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws IOException {
			num++;
			if (num < segments.size()) {
				return segments.get(num).segment;
			} else {
				throw new EOFException();
			}
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return segments.get(num).position;
		}
	}
}