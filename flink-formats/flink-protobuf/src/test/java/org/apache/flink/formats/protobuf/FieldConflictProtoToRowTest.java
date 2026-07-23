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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.testproto.FieldConflictPbMessage;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for protobuf field conflict resolution covering:
 *
 * <ul>
 *   <li>Enum numeric accessor vs *_value field conflicts.
 *   <li>Repeated field accessor base name vs singular field conflicts.
 * </ul>
 */
public class FieldConflictProtoToRowTest {

    @Test
    public void testEnumAndValueConflict() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .setStatus1(FieldConflictPbMessage.Status.STATUS_ACTIVE)
                        .setStatusValue2("custom_status")
                        .setControlField(42)
                        .setOtherValue("nv")
                        .build();

        // Normal (string enum) mode
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // Row schema order now: status, status_value, control_field, tags, tags_list, tags_count,
        // other_value
        assertEquals("STATUS_ACTIVE", row.getString(0).toString());
        assertEquals("custom_status", row.getString(1).toString());
        assertEquals(42, row.getInt(2));
        assertEquals("nv", row.getString(6).toString());

        // Int enum mode (numeric values)
        RowData intEnumRow =
                ProtobufTestHelper.pbBytesToRow(
                        FieldConflictPbMessage.class, msg.toByteArray(), true);
        assertEquals(1, intEnumRow.getInt(0)); // numeric value of STATUS_ACTIVE
        assertEquals("custom_status", intEnumRow.getString(1).toString());
        assertEquals(42, intEnumRow.getInt(2));
        assertEquals("nv", intEnumRow.getString(6).toString());
    }

    @Test
    public void testEnumMissingValueFieldPresent() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .setStatusValue2("only_value")
                        .setControlField(7)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        assertFalse(row.isNullAt(1)); // status_value present
        assertEquals("only_value", row.getString(1).toString());
    }

    @Test
    public void testRepeatedAndSingularAllPresent() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .addTags4("tag1")
                        .addTags4("tag2")
                        .setTagsList5("joined_tags")
                        .setTagsCount6(2)
                        .setOtherValue("ov")
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        assertEquals(2, row.getArray(3).size());
        assertEquals(StringData.fromString("tag1"), row.getArray(3).getString(0));
        assertEquals(StringData.fromString("tag2"), row.getArray(3).getString(1));
        assertEquals("joined_tags", row.getString(4).toString());
        assertEquals(2, row.getInt(5));
        assertEquals("ov", row.getString(6).toString());
    }

    @Test
    public void testOnlyRepeatedPresent() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder().addTags4("single_tag").build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        assertEquals(1, row.getArray(3).size());
        assertEquals(StringData.fromString("single_tag"), row.getArray(3).getString(0));
        // Proto3 implicit presence: absent fields materialize to their default values
        assertFalse(row.isNullAt(4)); // tags_list
        assertEquals("", row.getString(4).toString());
        assertFalse(row.isNullAt(5)); // tags_count
        assertEquals(0, row.getInt(5));
    }

    @Test
    public void testOnlySingularPresent() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder().setTagsList5("x").setTagsCount6(1).build();

        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        assertTrue(row.isNullAt(3)); // tags field should be null
        assertEquals("x", row.getString(4).toString());
        assertEquals(1, row.getInt(5));
    }

    @Test
    public void testOtherValueOnlyAccessor() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder().setOtherValue("abc").build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        assertFalse(row.isNullAt(6));
        assertEquals("abc", row.getString(6).toString());
    }

    @Test
    public void testOtherValueDefaultsMaterialization() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .setStatus1(FieldConflictPbMessage.Status.STATUS_UNSPECIFIED)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // other_value absent -> proto3 default empty string should materialize (not null)
        assertFalse(row.isNullAt(6));
        assertEquals("", row.getString(6).toString());
    }

    @Test
    public void testMapFieldNoConflict() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .putTagsMap("key1", 10)
                        .putTagsMap("key2", 20)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // Map field should work correctly without conflicts
        assertFalse(row.isNullAt(7)); // tags_map at index 7
        org.apache.flink.table.data.MapData mapData = row.getMap(7);
        assertEquals(2, mapData.size());
    }

    @Test
    public void testBooleanPresence() throws Exception {
        // Test with boolean fields set
        FieldConflictPbMessage msg1 =
                FieldConflictPbMessage.newBuilder().setActive(true).setActiveValue(false).build();
        RowData row1 =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg1.toByteArray());
        assertEquals(true, row1.getBoolean(8)); // active at index 8
        assertEquals(false, row1.getBoolean(9)); // active_value at index 9

        // Test with boolean fields unset
        FieldConflictPbMessage msg2 = FieldConflictPbMessage.newBuilder().build();
        RowData row2 =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg2.toByteArray());
        assertEquals(false, row2.getBoolean(8)); // proto3 default for bool
        assertEquals(false, row2.getBoolean(9)); // proto3 default for bool
    }

    @Test
    public void testMultipleEnumConflicts() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .setStatus1(FieldConflictPbMessage.Status.STATUS_ACTIVE)
                        .setStatusValue2("status_str")
                        .setPriority11(FieldConflictPbMessage.Priority.PRIORITY_HIGH)
                        .setPriorityValue12(99)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // Both enum/value pairs should work correctly
        assertEquals("STATUS_ACTIVE", row.getString(0).toString());
        assertEquals("status_str", row.getString(1).toString());
        assertEquals("PRIORITY_HIGH", row.getString(10).toString());
        assertEquals(99, row.getInt(11));
    }

    @Test
    public void testNestedMessageFields() throws Exception {
        FieldConflictPbMessage.NestedMessage nested =
                FieldConflictPbMessage.NestedMessage.newBuilder()
                        .setNestedField("nested_value")
                        .build();
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .setNested(nested)
                        .addNestedList(nested)
                        .addNestedList(nested)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // Nested message fields should not have conflict issues
        assertFalse(row.isNullAt(14)); // nested
        assertFalse(row.isNullAt(15)); // nested_list
        assertEquals(2, row.getArray(15).size());
    }

    @Test
    public void testMultipleRepeatedFieldConflicts() throws Exception {
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .addItems17(100)
                        .addItems17(200)
                        .setItemsList18("items_string")
                        .setItemsCount19(999L)
                        .build();
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // Multiple repeated field conflict scenarios in same message
        assertEquals(2, row.getArray(16).size());
        assertEquals(100, row.getArray(16).getInt(0));
        assertEquals(200, row.getArray(16).getInt(1));
        assertEquals("items_string", row.getString(17).toString());
        assertEquals(999L, row.getLong(18));
    }

    @Test
    public void testEnumEndingWithListConflictsWithRepeatedField() throws Exception {
        // Repeated field 'data' generates getDataList()
        // Enum field 'data_list' also tries to generate getDataList()
        // Resolution: Both get field number suffixes:
        // - repeated 'data' (field 20) → getData20(), getData20Count()
        // - enum 'data_list' (field 21) → getDataList21()
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .addData20("data1")
                        .addData20("data2")
                        .setDataList21(FieldConflictPbMessage.DataList.DATA_LIST_JSON)
                        .build();

        // Normal (string enum) mode
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // data at index 19, data_list at index 20
        assertEquals(2, row.getArray(19).size());
        assertEquals(StringData.fromString("data1"), row.getArray(19).getString(0));
        assertEquals(StringData.fromString("data2"), row.getArray(19).getString(1));
        assertEquals("DATA_LIST_JSON", row.getString(20).toString());

        // Int enum mode (numeric values)
        RowData intEnumRow =
                ProtobufTestHelper.pbBytesToRow(
                        FieldConflictPbMessage.class, msg.toByteArray(), true);
        assertEquals(2, intEnumRow.getArray(19).size());
        assertEquals(1, intEnumRow.getInt(20)); // numeric value of DATA_LIST_JSON
    }

    @Test
    public void testEnumEndingWithCountConflictsWithRepeatedField() throws Exception {
        // Repeated field 'record' generates getRecordCount()
        // Enum field 'record_count' also tries to generate getRecordCount()
        // Resolution: Both get field number suffixes:
        // - repeated 'record' (field 22) → getRecord22(), getRecord22Count()
        // - enum 'record_count' (field 23) → getRecordCount23()
        FieldConflictPbMessage msg =
                FieldConflictPbMessage.newBuilder()
                        .addRecord22("record1")
                        .addRecord22("record2")
                        .addRecord22("record3")
                        .setRecordCount23(FieldConflictPbMessage.RecordCount.RECORD_COUNT_LARGE)
                        .build();

        // Normal (string enum) mode
        RowData row =
                ProtobufTestHelper.pbBytesToRow(FieldConflictPbMessage.class, msg.toByteArray());
        // record at index 21, record_count at index 22
        assertEquals(3, row.getArray(21).size());
        assertEquals(StringData.fromString("record1"), row.getArray(21).getString(0));
        assertEquals(StringData.fromString("record2"), row.getArray(21).getString(1));
        assertEquals(StringData.fromString("record3"), row.getArray(21).getString(2));
        assertEquals("RECORD_COUNT_LARGE", row.getString(22).toString());

        // Int enum mode (numeric values)
        RowData intEnumRow =
                ProtobufTestHelper.pbBytesToRow(
                        FieldConflictPbMessage.class, msg.toByteArray(), true);
        assertEquals(3, intEnumRow.getArray(21).size());
        assertEquals(2, intEnumRow.getInt(22)); // numeric value of RECORD_COUNT_LARGE
    }
}
