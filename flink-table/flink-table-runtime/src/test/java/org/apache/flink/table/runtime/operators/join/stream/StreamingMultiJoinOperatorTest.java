package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.*;

@ExtendWith(ParameterizedTestExtension.class)
class StreamingTwoWayMultiJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false);
    }

    @Parameter private boolean enableAsyncState;

    public StreamingTwoWayMultiJoinOperatorTest() {
        super(2); // Two-way join specific test class
    }

    @TestTemplate
    void testTwoWayInnerJoin() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(
                0,
                insertRecord(
                        "order_1", // id
                        "1", // key
                        "Order 1 Details" // payload
                        ));

        // No output yet since we haven't received matching record from second input
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(
                1,
                insertRecord(
                        "shipment_1", // id
                        "1", // key
                        "Shipment 1 Details" // payload
                        ));

        // Should emit joined record since keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_1",
                        "1",
                        "Order 1 Details",
                        "shipment_1",
                        "1",
                        "Shipment 1 Details"));

        // Process second input with non-matching key
        testHarness.processElement(1, insertRecord("shipment_2", "2", "Shipment 2 Details"));

        // Should not emit since keys don't match
        assertor.shouldEmitNothing(testHarness);

        // Add matching record to first input
        testHarness.processElement(0, insertRecord("order_2", "2", "Order 2 Details"));

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_2",
                        "2",
                        "Order 2 Details",
                        "shipment_2",
                        "2",
                        "Shipment 2 Details"));
    }

    @TestTemplate
    void testTwoWayInnerJoinUpdating() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(0, insertRecord("order_1", "1", "Order 1 Details"));

        // No output yet since we haven't received matching record from second input
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(1, insertRecord("shipment_1", "1", "Shipment 1 Details"));

        // Should emit joined record since keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_1",
                        "1",
                        "Order 1 Details",
                        "shipment_1",
                        "1",
                        "Shipment 1 Details"));

        // Update first input record
        testHarness.processElement(0, updateAfterRecord("order_1", "1", "Order 1 Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Details"));

        // Update second input record
        testHarness.processElement(1, updateAfterRecord("shipment_1", "1", "Shipment 1 Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated"));

        // Delete the shipment record for key 1, which should generate a deletion for the join
        testHarness.processElement(1, deleteRecord("shipment_1", "1", "Shipment 1 Updated"));

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated"));

        // Add a matching shipment record back for key "1"
        testHarness.processElement(1, insertRecord("shipment_1", "1", "Shipment 1 Updated 2"));

        // Should emit joined record for key "1"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated 2"));
    }
}

@ExtendWith(ParameterizedTestExtension.class)
class StreamingThreeWayJoinOperatorTest extends StreamingMultiJoinOperatorTestBase {

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false);
    }

    @Parameter private boolean enableAsyncState;

    public StreamingThreeWayJoinOperatorTest() {
        super(3);
    }

    @TestTemplate
    void testThreeWayInnerJoin() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(0, insertRecord("order_1", "1", "Order 1 Details"));

        // No output yet since we haven't received matching records from other inputs
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(1, insertRecord("shipment_1", "1", "Shipment 1 Details"));

        // Still no output - need all three inputs to match
        assertor.shouldEmitNothing(testHarness);

        // Process third input - add a record with matching key "1"
        testHarness.processElement(2, insertRecord("payment_1", "1", "Payment 1 Details"));

        // Should emit joined record since all three keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_1",
                        "1",
                        "Order 1 Details",
                        "shipment_1",
                        "1",
                        "Shipment 1 Details",
                        "payment_1",
                        "1",
                        "Payment 1 Details"));

        // Test non-matching keys
        testHarness.processElement(0, insertRecord("order_2", "2", "Order 2 Details"));

        testHarness.processElement(1, insertRecord("shipment_2", "2", "Shipment 2 Details"));

        // No output yet - need all three to match
        assertor.shouldEmitNothing(testHarness);

        testHarness.processElement(2, insertRecord("payment_2", "2", "Payment 2 Details"));

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_2",
                        "2",
                        "Order 2 Details",
                        "shipment_2",
                        "2",
                        "Shipment 2 Details",
                        "payment_2",
                        "2",
                        "Payment 2 Details"));
    }

    @TestTemplate
    void testThreeWayInnerJoinUpdating() throws Exception {
        // Process first input - add a record with key "1"
        testHarness.processElement(0, insertRecord("order_1", "1", "Order 1 Details"));

        // No output yet since we haven't received matching records from other inputs
        assertor.shouldEmitNothing(testHarness);

        // Process second input - add a record with matching key "1"
        testHarness.processElement(1, insertRecord("shipment_1", "1", "Shipment 1 Details"));

        // Still no output - need all three inputs to match
        assertor.shouldEmitNothing(testHarness);

        // Process third input - add a record with matching key "1"
        testHarness.processElement(2, insertRecord("payment_1", "1", "Payment 1 Details"));

        // Should emit joined record since all three keys match
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_1",
                        "1",
                        "Order 1 Details",
                        "shipment_1",
                        "1",
                        "Shipment 1 Details",
                        "payment_1",
                        "1",
                        "Payment 1 Details"));

        // Update first input record
        testHarness.processElement(0, updateAfterRecord("order_1", "1", "Order 1 Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Details",
                        "payment_1",
                        "1",
                        "Payment 1 Details"));

        // Update second input record
        testHarness.processElement(1, updateAfterRecord("shipment_1", "1", "Shipment 1 Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated",
                        "payment_1",
                        "1",
                        "Payment 1 Details"));

        // Update third input record
        testHarness.processElement(2, updateAfterRecord("payment_1", "1", "Payment 1 Updated"));

        // Should emit updated joined record
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.UPDATE_AFTER,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated",
                        "payment_1",
                        "1",
                        "Payment 1 Updated"));

        // Delete the payment record for key 1, which should generate a deletion for the join
        testHarness.processElement(2, deleteRecord("payment_1", "1", "Payment 1 Updated"));

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated",
                        "payment_1",
                        "1",
                        "Payment 1 Updated"));

        // Add a matching payment record back for key "1"
        testHarness.processElement(2, insertRecord("payment_1", "1", "Payment 1 Updated 2"));

        // Should emit joined record for key "1"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_1",
                        "1",
                        "Order 1 Updated",
                        "shipment_1",
                        "1",
                        "Shipment 1 Updated",
                        "payment_1",
                        "1",
                        "Payment 1 Updated 2"));

        // Test key updates by inserting records with key "2"
        testHarness.processElement(0, insertRecord("order_2", "2", "Order 2 Details"));

        testHarness.processElement(1, insertRecord("shipment_2", "2", "Shipment 2 Details"));

        testHarness.processElement(2, insertRecord("payment_2", "2", "Payment 2 Details"));

        // Should emit joined record for key "2"
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_2",
                        "2",
                        "Order 2 Details",
                        "shipment_2",
                        "2",
                        "Shipment 2 Details",
                        "payment_2",
                        "2",
                        "Payment 2 Details"));

        // Update key of order_2 from "2" to "3"
        testHarness.processElement(0, deleteRecord("order_2", "2", "Order 2 Details"));

        // Should emit a delete for the old join result
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.DELETE,
                        "order_2",
                        "2",
                        "Order 2 Details",
                        "shipment_2",
                        "2",
                        "Shipment 2 Details",
                        "payment_2",
                        "2",
                        "Payment 2 Details"));

        // Add updated matching record for key "2"
        testHarness.processElement(0, insertRecord("order_2", "2", "Order 2 Details Updated"));

        // Should emit the row again with updated records
        assertor.shouldEmit(
                testHarness,
                rowOfKind(
                        RowKind.INSERT,
                        "order_2",
                        "2",
                        "Order 2 Details Updated",
                        "shipment_2",
                        "2",
                        "Shipment 2 Details",
                        "payment_2",
                        "2",
                        "Payment 2 Details"));
    }
}
