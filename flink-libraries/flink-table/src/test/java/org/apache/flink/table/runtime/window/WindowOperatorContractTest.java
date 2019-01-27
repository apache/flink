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

package org.apache.flink.table.runtime.window;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.api.window.TimeWindow;
import org.apache.flink.table.api.window.Window;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.plan.util.StreamExecUtil;
import org.apache.flink.table.runtime.BaseRowKeySelector;
import org.apache.flink.table.runtime.functions.SubKeyedAggsHandleFunction;
import org.apache.flink.table.runtime.sort.RecordEqualiser;
import org.apache.flink.table.runtime.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.window.triggers.Trigger;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.runtime.window.WindowTestUtils.baserow;
import static org.apache.flink.table.runtime.window.WindowTestUtils.record;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * These tests verify that {@link WindowOperator} correctly interacts with the other windowing
 * components: {@link WindowAssigner}, {@link Trigger}, AggsHandleFunction and window state.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
public class WindowOperatorContractTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testAssignerIsInvokedOncePerElement() throws Exception {
		WindowAssigner<TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();
		SubKeyedAggsHandleFunction<TimeWindow> mockAggregate = mockAggsHandleFunction();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			createWindowOperator(mockAssigner, mockTrigger, mockAggregate, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(any(), anyLong()))
			.thenReturn(Collections.singletonList(new TimeWindow(0, 0)));

		testHarness.processElement(record("String", 1, 0L));

		verify(mockAssigner, times(1))
			.assignWindows(eq(baserow("String", 1, 0L)), eq(0L));

		testHarness.processElement(record("String", 1, 0L));

		verify(mockAssigner, times(2))
			.assignWindows(eq(baserow("String", 1, 0L)), eq(0L));
	}

	@Test
	public void testAssignerWithMultipleWindows() throws Exception {
		WindowAssigner<TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();
		SubKeyedAggsHandleFunction<TimeWindow> mockAggregate = mockAggsHandleFunction();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			createWindowOperator(mockAssigner, mockTrigger, mockAggregate, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(any(), anyLong()))
			.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		shouldFireOnElement(mockTrigger);

		testHarness.processElement(record("String", 1, 0L));

		verify(mockAggregate, times(2))
			.getValue(anyTimeWindow());
		verify(mockAggregate, times(1))
			.getValue(eq(new TimeWindow(0, 2)));
		verify(mockAggregate, times(1))
			.getValue(eq(new TimeWindow(2, 4)));
	}

	@Test
	public void testOnElementCalledPerWindow() throws Exception {

		WindowAssigner<TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();
		SubKeyedAggsHandleFunction<TimeWindow> mockAggregate = mockAggsHandleFunction();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			createWindowOperator(mockAssigner, mockTrigger, mockAggregate, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyGenericRow(), anyLong()))
			.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		testHarness.processElement(record("String", 42, 1L));

		verify(mockTrigger).onElement(eq(baserow("String", 42, 1L)), eq(1L), eq(new TimeWindow(2, 4)));
		verify(mockTrigger).onElement(eq(baserow("String", 42, 1L)), eq(1L), eq(new TimeWindow(0, 2)));

		verify(mockTrigger, times(2)).onElement(anyInt(), anyLong(), anyTimeWindow());
	}

	@Test
	public void testMergeWindowsIsCalled() throws Exception {
		MergingWindowAssigner<TimeWindow> mockAssigner = mockMergingAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();
		SubKeyedAggsHandleFunction<TimeWindow> mockAggregate = mockAggsHandleFunction();

		OneInputStreamOperatorTestHarness<BaseRow, BaseRow> testHarness =
			createWindowOperator(mockAssigner, mockTrigger, mockAggregate, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyGenericRow(), anyLong()))
			.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());

		testHarness.processElement(record("String", 42, 0L));

		verify(mockAssigner)
			.mergeWindows(eq(new TimeWindow(2, 4)), any(), anyMergeCallback());
		verify(mockAssigner)
			.mergeWindows(eq(new TimeWindow(0, 2)), any(), anyMergeCallback());

		verify(mockAssigner, times(2)).mergeWindows(anyTimeWindow(), any(), anyMergeCallback());
	}

	// ------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private <W extends Window> KeyedOneInputStreamOperatorTestHarness<BaseRow, BaseRow, BaseRow> createWindowOperator(
		WindowAssigner<W> assigner,
		Trigger<W> trigger,
		SubKeyedAggsHandleFunction<W> aggregationsFunction,
		long allowedLateness) throws Exception {

		BaseRowTypeInfo inputType = new BaseRowTypeInfo(Types.STRING(), Types.INT());
		RowType inputT = (RowType) TypeConverters.createInternalTypeFromTypeInfo(inputType);
		BaseRowKeySelector keySelector = StreamExecUtil.getKeySelector(new int[]{0}, inputType);
		TypeInformation<BaseRow> keyType = keySelector.getProducedType();
		InternalType[] accTypes = new InternalType[]{DataTypes.LONG, DataTypes.LONG};
		InternalType[] windowTypes = new InternalType[]{DataTypes.LONG, DataTypes.LONG};
		InternalType[] outputTypeWithoutKeys =
			new InternalType[]{DataTypes.LONG, DataTypes.LONG, DataTypes.LONG, DataTypes.LONG};

		boolean sendRetraction = allowedLateness > 0;

		WindowOperator operator = new WindowOperator(
			aggregationsFunction,
			mock(RecordEqualiser.class),
			assigner,
			trigger,
			assigner.getWindowSerializer(new ExecutionConfig()),
			inputT.getFieldInternalTypes(),
			outputTypeWithoutKeys,
			accTypes,
			windowTypes,
			2,
			sendRetraction,
			allowedLateness);

		return new KeyedOneInputStreamOperatorTestHarness<BaseRow, BaseRow, BaseRow>(
			operator, keySelector, keyType);
	}

	private static <W extends Window> SubKeyedAggsHandleFunction<W> mockAggsHandleFunction() throws Exception {
		return mock(SubKeyedAggsHandleFunction.class);
	}

	private <W extends Window> Trigger<W> mockTrigger() throws Exception {
		@SuppressWarnings("unchecked")
		Trigger<W> mockTrigger = mock(Trigger.class);

		when(mockTrigger.onElement(Matchers.<BaseRow>any(), anyLong(), Matchers.any())).thenReturn(false);
		when(mockTrigger.onEventTime(anyLong(), Matchers.any())).thenReturn(false);
		when(mockTrigger.onProcessingTime(anyLong(), Matchers.any())).thenReturn(false);

		return mockTrigger;
	}

	private static TimeWindow anyTimeWindow() {
		return Mockito.any();
	}

	private static GenericRow anyGenericRow() {
		return Mockito.any();
	}

	private static WindowAssigner<TimeWindow> mockTimeWindowAssigner() throws Exception {
		@SuppressWarnings("unchecked")
		WindowAssigner<TimeWindow> mockAssigner = mock(WindowAssigner.class);

		when(mockAssigner.getWindowSerializer(Mockito.any())).thenReturn(new TimeWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);

		return mockAssigner;
	}

	private static MergingWindowAssigner<TimeWindow> mockMergingAssigner() throws Exception {
		@SuppressWarnings("unchecked")
		MergingWindowAssigner<TimeWindow> mockAssigner = mock(MergingWindowAssigner.class);

		when(mockAssigner.getWindowSerializer(Mockito.any())).thenReturn(new TimeWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);

		return mockAssigner;
	}

	private static MergingWindowAssigner.MergeCallback<TimeWindow> anyMergeCallback() {
		return Mockito.any();
	}

	// ------------------------------------------------------------------------------------

	private static <T> void shouldFireOnElement(Trigger<TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onElement(Matchers.<T>anyObject(), anyLong(), anyTimeWindow()))
			.thenReturn(true);
	}
}
