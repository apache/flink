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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * ClinkHouse.
 */
public class ClinkHouseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "ClinkHouseSQL";
    }

    public ClinkHouseRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public RowData toInternal(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(this.rowType.getFieldCount());

        for (int pos = 0; pos < this.rowType.getFieldCount(); ++pos) {
            Object field = resultSet.getObject(pos + 1);
            if (field instanceof BigInteger) {
                int length = ((BigInteger) field).toString().length();
                if (length > 8) {
                    field = ((BigInteger) field).toString();
                } else {
                    field = Long.parseLong(((BigInteger) field).toString());
                }
            }
            genericRowData.setField(pos, this.toInternalConverters[pos].deserialize(field));
        }

        return genericRowData;
    }
}
