package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.util.BinaryRowDataKeySelector;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link RowTimeRangeBoundedPrecedingFunction}.
 */
public class ProcTimeRangeBoundedPrecedingFunctionTest {

	private static GeneratedAggsHandleFunction aggsHandleFunction =
		new GeneratedAggsHandleFunction("Function", "", new Object[0]) {
			@Override
			public AggsHandleFunction newInstance(ClassLoader classLoader) {
				return new SumAggsHandleFunction(1);
			}
		};

	private LogicalType[] inputFieldTypes = new LogicalType[]{
		new VarCharType(VarCharType.MAX_LENGTH),
		new BigIntType(),
	};
	private LogicalType[] accTypes = new LogicalType[]{ new BigIntType() };

	private BinaryRowDataKeySelector keySelector = new BinaryRowDataKeySelector(new int[]{ 0 }, inputFieldTypes);
	private TypeInformation<RowData> keyType = keySelector.getProducedType();

	@Test
	public void testRecordRetraction() throws Exception {
		ProcTimeRangeBoundedPrecedingFunction<RowData> function = new ProcTimeRangeBoundedPrecedingFunction<>(0, 0, aggsHandleFunction, accTypes, inputFieldTypes, 2000);
		KeyedProcessOperator<RowData, RowData, RowData> operator = new KeyedProcessOperator<>(function);

		OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(operator);

		testHarness.open();

		HeapKeyedStateBackend stateBackend = (HeapKeyedStateBackend) operator.getKeyedStateBackend();

		assertEquals("Initial state is not empty", 0, stateBackend.numKeyValueStateEntries());

		// put some records
		testHarness.setProcessingTime(100);
		testHarness.processElement(insertRecord("key", 1L));
		testHarness.processElement(insertRecord("key", 1L));
		testHarness.setProcessingTime(500);
		testHarness.processElement(insertRecord("key", 1L));

		testHarness.setProcessingTime(1000);
		// at this moment we expect the function to have some records in state

		testHarness.setProcessingTime(3000);
		// at this moment the function should have retracted all records

		assertEquals("Records are not fully retracted", 0, stateBackend.numKeyValueStateEntries());
	}

	private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(KeyedProcessOperator<RowData, RowData, RowData> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);
	}

}
