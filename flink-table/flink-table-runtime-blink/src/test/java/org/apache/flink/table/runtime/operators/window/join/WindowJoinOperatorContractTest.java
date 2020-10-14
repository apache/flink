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

package org.apache.flink.table.runtime.operators.window.join;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.Window;
import org.apache.flink.table.runtime.operators.window.WindowTestUtils;
import org.apache.flink.table.runtime.operators.window.assigners.MergingWindowAssigner;
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner;
import org.apache.flink.table.runtime.operators.window.triggers.Trigger;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * These tests verify that {@link WindowJoinOperator} correctly interacts with the other windowing
 * components: {@link WindowAssigner}, {@link Trigger} and
 * {@link org.apache.flink.table.runtime.operators.window.join.state.WindowJoinRecordStateView}.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
public class WindowJoinOperatorContractTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testAssignerIsInvokedOncePerElement() throws Exception {
		WindowAssigner<TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
				createWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(any(), anyLong()))
				.thenReturn(Collections.singletonList(new TimeWindow(0, 0)));

		testHarness.processElement1(insertRecord(1, "String", 0L));

		verify(mockAssigner, times(1))
				.assignWindows(eq(row(1, "String", 0L)), eq(0L));

		testHarness.processElement1(insertRecord(1, "String", 0L));

		verify(mockAssigner, times(2))
				.assignWindows(eq(row(1, "String", 0L)), eq(0L));

		testHarness.processElement2(insertRecord(1, "String", 0L));

		verify(mockAssigner, times(3))
				.assignWindows(eq(row(1, "String", 0L)), eq(0L));

		testHarness.processElement2(insertRecord(1, "String", 0L));

		verify(mockAssigner, times(4))
				.assignWindows(eq(row(1, "String", 0L)), eq(0L));
	}

	@Test
	public void testAssignerWithMultipleWindowsForJoin() throws Exception {
		WindowAssigner<TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
				createWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(any(), anyLong()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		shouldFireOnElement(mockTrigger);

		testHarness.processElement1(insertRecord(1, "String", 0L));

		assertThat(
				outputToString(testHarness.getOutput()),
				is("+I{row1=+I{row1=+I(1,String,0),"
						+ " row2=+I(null,null,null)},"
						+ " row2=+I(1970-01-01T00:00:00.002,1970-01-01T00:00:00.004)}\n"
						+ "+I{row1=+I{row1=+I(1,String,0),"
						+ " row2=+I(null,null,null)},"
						+ " row2=+I(1970-01-01T00:00,1970-01-01T00:00:00.002)}"));

		testHarness.processElement2(insertRecord(1, "String", 1L));

		assertThat(
				outputToString(testHarness.getOutput()),
				is("+I{row1=+I{row1=+I(1,String,0),"
						+ " row2=+I(null,null,null)},"
						+ " row2=+I(1970-01-01T00:00:00.002,1970-01-01T00:00:00.004)}\n"
						+ "+I{row1=+I{row1=+I(1,String,0),"
						+ " row2=+I(null,null,null)},"
						+ " row2=+I(1970-01-01T00:00,1970-01-01T00:00:00.002)}\n"
						+ "+I{row1=+I{row1=+I(1,String,0),"
						+ " row2=+I(1,String,1)},"
						+ " row2=+I(1970-01-01T00:00:00.002,1970-01-01T00:00:00.004)}\n"
						+ "+I{row1=+I{row1=+I(1,String,0),"
						+ " row2=+I(1,String,1)},"
						+ " row2=+I(1970-01-01T00:00,1970-01-01T00:00:00.002)}"));
	}

	@Test
	public void testOnElementCalledPerWindow() throws Exception {

		WindowAssigner<TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
				createWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(any(), anyLong()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		testHarness.processElement1(insertRecord(42, "String", 1L));

		verify(mockTrigger).onElement(eq(row(42, "String", 1L)), eq(1L), eq(new TimeWindow(2, 4)));
		verify(mockTrigger).onElement(eq(row(42, "String", 1L)), eq(1L), eq(new TimeWindow(0, 2)));
		verify(mockTrigger, times(2)).onElement(any(), anyLong(), any());

		testHarness.processElement2(insertRecord(43, "String", 1L));

		verify(mockTrigger).onElement(eq(row(43, "String", 1L)), eq(1L), eq(new TimeWindow(2, 4)));
		verify(mockTrigger).onElement(eq(row(43, "String", 1L)), eq(1L), eq(new TimeWindow(0, 2)));
		verify(mockTrigger, times(4)).onElement(any(), anyLong(), any());
	}

	@Test
	public void testMergeWindowsIsCalled() throws Exception {
		MergingWindowAssigner<TimeWindow> mockAssigner = mockMergingAssigner();
		Trigger<TimeWindow> mockTrigger = mockTrigger();

		TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
				createWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(any(), anyLong()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());

		testHarness.processElement1(insertRecord(42, "String", 0L));

		verify(mockAssigner).mergeWindows(eq(new TimeWindow(2, 4)), any(), anyMergeCallback());
		verify(mockAssigner).mergeWindows(eq(new TimeWindow(0, 2)), any(), anyMergeCallback());
		verify(mockAssigner, times(2)).mergeWindows(any(), any(), anyMergeCallback());

		testHarness.processElement2(insertRecord(43, "String", 0L));
		verify(mockAssigner, times(2)).mergeWindows(eq(new TimeWindow(2, 4)), any(), anyMergeCallback());
		verify(mockAssigner, times(2)).mergeWindows(eq(new TimeWindow(0, 2)), any(), anyMergeCallback());
		verify(mockAssigner, times(4)).mergeWindows(any(), any(), anyMergeCallback());
	}

	// -------------------------------------------------------------------------
	//  Utilities
	// -------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private String outputToString(Collection<Object> output) {
		return output.stream()
				.map(o -> ((StreamRecord<RowData>) o).getValue().toString())
				.collect(Collectors.joining("\n"));
	}

	private <K, W extends Window> KeyedTwoInputStreamOperatorTestHarness
			<RowData, RowData, RowData, RowData> createWindowOperator(
			WindowAssigner<W> assigner,
			Trigger<W> trigger,
			long allowedLateness) throws Exception {

		LogicalType[] leftTypes = new LogicalType[]{new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()};
		LogicalType[] rightTypes = new LogicalType[]{new IntType(), new VarCharType(VarCharType.MAX_LENGTH), new BigIntType()};
		BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[]{0}, leftTypes);
		TypeInformation<RowData> keyType = keySelector.getProducedType();
		JoinInputSideSpec leftJoinInputSideSpec = JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
				InternalTypeInfo.ofFields(new IntType()),
				keySelector);
		JoinInputSideSpec rightJoinInputSideSpec = JoinInputSideSpec.withUniqueKey(
				InternalTypeInfo.ofFields(new IntType()),
				keySelector);

		WindowJoinOperator<K, W> operator = WindowJoinOperator
				.builder()
				.assigner(assigner)
				.trigger(trigger)
				.allowedLateness(Duration.ofMillis(allowedLateness))
				.inputType(
						InternalTypeInfo.ofFields(leftTypes),
						InternalTypeInfo.ofFields(rightTypes))
				.joinCondition(WindowTestUtils.alwaysTrueCondition())
				.joinInputSpec(
						leftJoinInputSideSpec,
						rightJoinInputSideSpec)
				.joinType(FlinkJoinType.LEFT)
				.eventTime(2, 2)
				.filterNullKeys(false).build();
		return new KeyedTwoInputStreamOperatorTestHarness<>(
				operator, keySelector, keySelector, keyType);
	}

	private <W extends Window> Trigger<W> mockTrigger() throws Exception {
		@SuppressWarnings("unchecked")
		Trigger<W> mockTrigger = mock(Trigger.class);

		when(mockTrigger.onElement(any(), anyLong(), any())).thenReturn(false);
		when(mockTrigger.onEventTime(anyLong(), any())).thenReturn(false);
		when(mockTrigger.onProcessingTime(anyLong(), any())).thenReturn(false);

		return mockTrigger;
	}

	private static WindowAssigner<TimeWindow> mockTimeWindowAssigner() {
		@SuppressWarnings("unchecked")
		WindowAssigner<TimeWindow> mockAssigner = mock(WindowAssigner.class);

		when(mockAssigner.getWindowSerializer(any())).thenReturn(new TimeWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);

		return mockAssigner;
	}

	private static MergingWindowAssigner<TimeWindow> mockMergingAssigner() {
		@SuppressWarnings("unchecked")
		MergingWindowAssigner<TimeWindow> mockAssigner = mock(MergingWindowAssigner.class);

		when(mockAssigner.getWindowSerializer(any())).thenReturn(new TimeWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);

		return mockAssigner;
	}

	private static MergingWindowAssigner.MergeCallback<TimeWindow> anyMergeCallback() {
		return any();
	}

	private static void shouldFireOnElement(Trigger<TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onElement(any(), anyLong(), any())).thenReturn(true);
	}
}
