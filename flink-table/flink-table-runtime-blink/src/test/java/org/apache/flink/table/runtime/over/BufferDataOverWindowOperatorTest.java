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

package org.apache.flink.table.runtime.over;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.over.frame.InsensitiveOverFrame;
import org.apache.flink.table.runtime.over.frame.OffsetOverFrame;
import org.apache.flink.table.runtime.over.frame.OverWindowFrame;
import org.apache.flink.table.runtime.over.frame.RangeSlidingOverFrame;
import org.apache.flink.table.runtime.over.frame.RangeUnboundedFollowingOverFrame;
import org.apache.flink.table.runtime.over.frame.RangeUnboundedPrecedingOverFrame;
import org.apache.flink.table.runtime.over.frame.RowSlidingOverFrame;
import org.apache.flink.table.runtime.over.frame.RowUnboundedFollowingOverFrame;
import org.apache.flink.table.runtime.over.frame.RowUnboundedPrecedingOverFrame;
import org.apache.flink.table.runtime.over.frame.UnboundedOverWindowFrame;
import org.apache.flink.table.type.InternalTypes;
import org.apache.flink.table.type.RowType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.table.runtime.over.NonBufferOverWindowOperatorTest.comparator;
import static org.apache.flink.table.runtime.over.NonBufferOverWindowOperatorTest.function;
import static org.apache.flink.table.runtime.over.NonBufferOverWindowOperatorTest.inputSer;
import static org.apache.flink.table.runtime.over.NonBufferOverWindowOperatorTest.inputType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link BufferDataOverWindowOperator}.
 */
public class BufferDataOverWindowOperatorTest {

	private static final int MEMORY_SIZE = 50 * 1024 * 32;
	private RowType valueType = new RowType(InternalTypes.LONG);

	private List<GenericRow> collect;
	private MemoryManager memoryManager = new MemoryManager(MEMORY_SIZE, 1);
	private IOManager ioManager;
	private BufferDataOverWindowOperator operator;
	private GeneratedRecordComparator boundComparator = new GeneratedRecordComparator("", "", new Object[0]) {
		@Override
		public RecordComparator newInstance(ClassLoader classLoader) {
			return (RecordComparator) (o1, o2) -> (int) (o1.getLong(1) - o2.getLong(1));
		}
	};

	@Before
	public void before() throws Exception {
		ioManager = new IOManagerAsync();
		collect = new ArrayList<>();
	}

	@Test
	public void testOffsetWindowFrame() throws Exception {
		test(new OverWindowFrame[] {
				new OffsetOverFrame(function, 2, null),
						new OffsetOverFrame(function, 1, r -> (long) r.getInt(0))},
				new GenericRow[] {
				GenericRow.of(0, 1L, 4L, 1L, 1L),
				GenericRow.of(0, 1L, 1L, 2L, 2L),
				GenericRow.of(0, 1L, 1L, 1L, 3L),
				GenericRow.of(0, 1L, 1L, 0L, 4L),
				GenericRow.of(1, 5L, 2L, -5L, -5L),
				GenericRow.of(2, 5L, 4L, 6L, 6L),
				GenericRow.of(2, 6L, 2L, 12L, 12L),
				GenericRow.of(2, 6L, 2L, 6L, 6L),
				GenericRow.of(2, 6L, 2L, 0L, 0L)
		});
	}

	@Test
	public void testInsensitiveAndUnbounded() throws Exception {
		test(new OverWindowFrame[] {
						new InsensitiveOverFrame(function),
						new UnboundedOverWindowFrame(function, new RowType(InternalTypes.LONG))},
				new GenericRow[] {
						GenericRow.of(0, 1L, 4L, 1L, 4L),
						GenericRow.of(0, 1L, 1L, 2L, 4L),
						GenericRow.of(0, 1L, 1L, 3L, 4L),
						GenericRow.of(0, 1L, 1L, 4L, 4L),
						GenericRow.of(1, 5L, 2L, 5L, 5L),
						GenericRow.of(2, 5L, 4L, 5L, 23L),
						GenericRow.of(2, 6L, 2L, 11L, 23L),
						GenericRow.of(2, 6L, 2L, 17L, 23L),
						GenericRow.of(2, 6L, 2L, 23L, 23L)
				});
	}

	@Test
	public void testPreceding() throws Exception {
		test(new OverWindowFrame[] {
						new RowUnboundedPrecedingOverFrame(function, 1),
						new RangeUnboundedPrecedingOverFrame(function, boundComparator)},
				new GenericRow[] {
						GenericRow.of(0, 1L, 4L, 2L, 4L),
						GenericRow.of(0, 1L, 1L, 3L, 4L),
						GenericRow.of(0, 1L, 1L, 4L, 4L),
						GenericRow.of(0, 1L, 1L, 4L, 4L),
						GenericRow.of(1, 5L, 2L, 5L, 5L),
						GenericRow.of(2, 5L, 4L, 11L, 5L),
						GenericRow.of(2, 6L, 2L, 17L, 23L),
						GenericRow.of(2, 6L, 2L, 23L, 23L),
						GenericRow.of(2, 6L, 2L, 23L, 23L)
				});
	}

	@Test
	public void testFollowing() throws Exception {
		test(new OverWindowFrame[] {
						new RowUnboundedFollowingOverFrame(valueType, function, 1),
						new RangeUnboundedFollowingOverFrame(valueType, function, boundComparator)},
				new GenericRow[] {
						GenericRow.of(0, 1L, 4L, 4L, 4L),
						GenericRow.of(0, 1L, 1L, 4L, 4L),
						GenericRow.of(0, 1L, 1L, 3L, 4L),
						GenericRow.of(0, 1L, 1L, 2L, 4L),
						GenericRow.of(1, 5L, 2L, 5L, 5L),
						GenericRow.of(2, 5L, 4L, 23L, 23L),
						GenericRow.of(2, 6L, 2L, 23L, 18L),
						GenericRow.of(2, 6L, 2L, 18L, 18L),
						GenericRow.of(2, 6L, 2L, 12L, 18L)
				});
	}

	@Test
	public void testSliding() throws Exception {
		test(new OverWindowFrame[] {
						new RowSlidingOverFrame(inputType, valueType, function, 1, 1),
						new RangeSlidingOverFrame(inputType, valueType, function, boundComparator, boundComparator)},
				new GenericRow[] {
						GenericRow.of(0, 1L, 4L, 2L, 4L),
						GenericRow.of(0, 1L, 1L, 3L, 4L),
						GenericRow.of(0, 1L, 1L, 3L, 4L),
						GenericRow.of(0, 1L, 1L, 2L, 4L),
						GenericRow.of(1, 5L, 2L, 5L, 5L),
						GenericRow.of(2, 5L, 4L, 11L, 5L),
						GenericRow.of(2, 6L, 2L, 17L, 18L),
						GenericRow.of(2, 6L, 2L, 18L, 18L),
						GenericRow.of(2, 6L, 2L, 12L, 18L)
				});
	}

	private void test(OverWindowFrame[] frames, GenericRow[] expect) throws Exception {
		operator = new BufferDataOverWindowOperator(MEMORY_SIZE, frames, comparator, true) {
			{
				output = new NonBufferOverWindowOperatorTest.ConsumerOutput(new Consumer<BaseRow>() {
					@Override
					public void accept(BaseRow r) {
						collect.add(GenericRow.of(r.getInt(0), r.getLong(1),
								r.getLong(2), r.getLong(3), r.getLong(4)));
					}
				});
			}

			@Override
			public ClassLoader getUserCodeClassloader() {
				return Thread.currentThread().getContextClassLoader();
			}

			@Override
			public StreamConfig getOperatorConfig() {
				StreamConfig conf = mock(StreamConfig.class);
				when(conf.<BaseRow>getTypeSerializerIn1(getUserCodeClassloader()))
						.thenReturn(inputSer);
				return conf;
			}

			@Override
			public StreamTask<?, ?> getContainingTask() {
				StreamTask task = mock(StreamTask.class);
				Environment env = mock(Environment.class);
				when(task.getEnvironment()).thenReturn(env);
				when(env.getMemoryManager()).thenReturn(memoryManager);
				when(env.getIOManager()).thenReturn(ioManager);
				return task;
			}

			@Override
			public StreamingRuntimeContext getRuntimeContext() {
				return mock(StreamingRuntimeContext.class);
			}
		};
		operator.open();
		addRow(0, 1L, 4L); /* 1 **/
		addRow(0, 1L, 1L); /* 2 **/
		addRow(0, 1L, 1L); /* 3 **/
		addRow(0, 1L, 1L); /* 4 **/
		addRow(1, 5L, 2L); /* 5 **/
		addRow(2, 5L, 4L); /* 6 **/
		addRow(2, 6L, 2L); /* 7 **/
		addRow(2, 6L, 2L); /* 8 **/
		addRow(2, 6L, 2L); /* 9 **/
		operator.endInput();
		GenericRow[] outputs = this.collect.toArray(new GenericRow[0]);
		Assert.assertArrayEquals(expect, outputs);
		operator.close();
	}

	private void addRow(Object... fields) throws Exception {
		operator.processElement(new StreamRecord<>(GenericRow.of(fields)));
	}
}
