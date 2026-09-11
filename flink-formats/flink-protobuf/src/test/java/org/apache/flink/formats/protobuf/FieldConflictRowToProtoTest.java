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
import org.apache.flink.table.data.GenericRowData;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests serialization (RowData -> Proto bytes) for field conflict scenarios ensuring builder method
 * suffixes (setStatus1, setStatusValue2, addTags4, setTagsList5, setTagsCount6) are correctly
 * invoked by generated code.
 */
public class FieldConflictRowToProtoTest {

    @Test
    public void testEnumAndValueConflictSerialization() throws Exception {
        // Row schema order (zero-based indices):
        // 0=status, 1=status_value, 2=control_field, 3=tags, 4=tags_list, 5=tags_count,
        // 6=other_value, 7=tags_map, 8=active, 9=active_value, 10=priority, 11=priority_value,
        // 12=nested, 13=nested_list, 14=items, 15=items_list, 16=items_count, 17=mode,
        // 18=mode_value, 19=data, 20=data_list, 21=record, 22=record_count
        GenericRowData row = new GenericRowData(23);
        row.setField(0, org.apache.flink.table.data.StringData.fromString("STATUS_ACTIVE"));
        row.setField(1, org.apache.flink.table.data.StringData.fromString("custom_status"));
        row.setField(2, 42);
        // Leave repeated tags empty; supply singular fields and other_value
        // Empty array for repeated tags: create explicit empty GenericArrayData
        row.setField(3, new org.apache.flink.table.data.GenericArrayData(new Object[0]));
        row.setField(4, org.apache.flink.table.data.StringData.fromString("joined_tags"));
        row.setField(5, 2);
        row.setField(6, org.apache.flink.table.data.StringData.fromString("nv"));
        // New fields - set to null/empty for most
        row.setField(7, null); // tags_map
        row.setField(8, false); // active
        row.setField(9, false); // active_value
        row.setField(10, null); // priority
        row.setField(11, null); // priority_value
        row.setField(12, null); // nested
        row.setField(13, null); // nested_list
        row.setField(14, null); // items
        row.setField(15, null); // items_list
        row.setField(16, null); // items_count
        row.setField(17, null); // mode
        row.setField(18, null); // mode_value
        row.setField(19, null); // data
        row.setField(20, null); // data_list
        row.setField(21, null); // record
        row.setField(22, null); // record_count

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, FieldConflictPbMessage.class);
        FieldConflictPbMessage parsed = FieldConflictPbMessage.parseFrom(bytes);
        assertEquals(FieldConflictPbMessage.Status.STATUS_ACTIVE, parsed.getStatus1());
        assertEquals("custom_status", parsed.getStatusValue2());
        assertEquals(42, parsed.getControlField());
        assertEquals("joined_tags", parsed.getTagsList5());
        assertEquals(2, parsed.getTagsCount6());
        assertEquals("nv", parsed.getOtherValue());
    }

    @Test
    public void testRepeatedFieldConflictSerialization() throws Exception {
        GenericRowData row = new GenericRowData(23);
        row.setField(0, org.apache.flink.table.data.StringData.fromString("STATUS_ACTIVE"));
        row.setField(1, org.apache.flink.table.data.StringData.fromString("cs"));
        row.setField(2, 100);
        // Provide repeated tags array with two elements
        org.apache.flink.table.data.StringData[] tagArray =
                new org.apache.flink.table.data.StringData[] {
                    org.apache.flink.table.data.StringData.fromString("t1"),
                    org.apache.flink.table.data.StringData.fromString("t2")
                };
        row.setField(3, new org.apache.flink.table.data.GenericArrayData(tagArray));
        row.setField(4, org.apache.flink.table.data.StringData.fromString("joined"));
        row.setField(5, 2);
        row.setField(6, org.apache.flink.table.data.StringData.fromString("ov"));
        // Fill additional fields with null/defaults
        row.setField(7, null); // tags_map
        row.setField(8, false); // active
        row.setField(9, false); // active_value
        row.setField(10, null); // priority
        row.setField(11, null); // priority_value
        row.setField(12, null); // nested
        row.setField(13, null); // nested_list
        row.setField(14, null); // items
        row.setField(15, null); // items_list
        row.setField(16, null); // items_count
        row.setField(17, null); // mode
        row.setField(18, null); // mode_value
        row.setField(19, null); // data
        row.setField(20, null); // data_list
        row.setField(21, null); // record
        row.setField(22, null); // record_count

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, FieldConflictPbMessage.class);
        FieldConflictPbMessage parsed = FieldConflictPbMessage.parseFrom(bytes);
        assertEquals(2, parsed.getTags4Count());
        assertEquals("t1", parsed.getTags4(0));
        assertEquals("t2", parsed.getTags4(1));
        assertEquals("joined", parsed.getTagsList5());
        assertEquals(2, parsed.getTagsCount6());
    }

    @Test
    public void testEnumNumericModeSerialization() throws Exception {
        GenericRowData row = new GenericRowData(23);
        // Numeric enum mode: supply integer at position 0
        row.setField(0, 1); // STATUS_ACTIVE numeric value
        row.setField(1, org.apache.flink.table.data.StringData.fromString("custom_status"));
        row.setField(2, 7);
        row.setField(3, new org.apache.flink.table.data.GenericArrayData(new Object[0]));
        row.setField(4, org.apache.flink.table.data.StringData.fromString("tags"));
        row.setField(5, 0);
        row.setField(6, org.apache.flink.table.data.StringData.fromString("other"));
        // Fill additional fields with null/defaults
        row.setField(7, null); // tags_map
        row.setField(8, false); // active
        row.setField(9, false); // active_value
        row.setField(10, null); // priority
        row.setField(11, null); // priority_value
        row.setField(12, null); // nested
        row.setField(13, null); // nested_list
        row.setField(14, null); // items
        row.setField(15, null); // items_list
        row.setField(16, null); // items_count
        row.setField(17, null); // mode
        row.setField(18, null); // mode_value
        row.setField(19, null); // data
        row.setField(20, null); // data_list
        row.setField(21, null); // record
        row.setField(22, null); // record_count

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, FieldConflictPbMessage.class, true);
        FieldConflictPbMessage parsed = FieldConflictPbMessage.parseFrom(bytes);
        assertEquals(FieldConflictPbMessage.Status.STATUS_ACTIVE, parsed.getStatus1());
        assertEquals("custom_status", parsed.getStatusValue2());
        assertEquals(7, parsed.getControlField());
    }

    @Test
    public void testEnumListAndCountConflictSerialization() throws Exception {
        // data (index 19), data_list (20), record (21), record_count (22)
        GenericRowData row = new GenericRowData(23);
        // Populate minimal earlier fields to satisfy builder logic
        row.setField(0, org.apache.flink.table.data.StringData.fromString("STATUS_ACTIVE"));
        row.setField(1, org.apache.flink.table.data.StringData.fromString("status_val"));
        row.setField(2, 1); // control_field
        // Minimal placeholders for intervening indices
        for (int i = 3; i < 19; i++) {
            row.setField(i, null);
        }
        // Repeated 'data' with two entries via suffixed accessors addData20()
        org.apache.flink.table.data.StringData[] dataArr =
                new org.apache.flink.table.data.StringData[] {
                    org.apache.flink.table.data.StringData.fromString("d1"),
                    org.apache.flink.table.data.StringData.fromString("d2")
                };
        row.setField(19, new org.apache.flink.table.data.GenericArrayData(dataArr));
        // Enum 'data_list'
        row.setField(20, org.apache.flink.table.data.StringData.fromString("DATA_LIST_XML"));
        // Repeated 'record'
        org.apache.flink.table.data.StringData[] recordArr =
                new org.apache.flink.table.data.StringData[] {
                    org.apache.flink.table.data.StringData.fromString("r1"),
                    org.apache.flink.table.data.StringData.fromString("r2"),
                    org.apache.flink.table.data.StringData.fromString("r3")
                };
        row.setField(21, new org.apache.flink.table.data.GenericArrayData(recordArr));
        // Enum 'record_count'
        row.setField(22, org.apache.flink.table.data.StringData.fromString("RECORD_COUNT_SMALL"));

        byte[] bytes = ProtobufTestHelper.rowToPbBytes(row, FieldConflictPbMessage.class);
        FieldConflictPbMessage parsed = FieldConflictPbMessage.parseFrom(bytes);
        // Validate repeated field serialization using suffixed builder methods
        assertEquals(2, parsed.getData20Count());
        assertEquals("d1", parsed.getData20(0));
        assertEquals("d2", parsed.getData20(1));
        assertEquals(FieldConflictPbMessage.DataList.DATA_LIST_XML, parsed.getDataList21());
        assertEquals(3, parsed.getRecord22Count());
        assertEquals("r1", parsed.getRecord22(0));
        assertEquals("r2", parsed.getRecord22(1));
        assertEquals("r3", parsed.getRecord22(2));
        assertEquals(
                FieldConflictPbMessage.RecordCount.RECORD_COUNT_SMALL, parsed.getRecordCount23());
    }
}
