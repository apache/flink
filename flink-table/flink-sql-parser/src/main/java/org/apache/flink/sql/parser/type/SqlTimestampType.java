/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser.type;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Parse type "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP(3) WITHOUT TIME ZONE",
 * "TIMESTAMP WITH LOCAL TIME ZONE", or "TIMESTAMP(3) WITH LOCAL TIME ZONE".
 */
public class SqlTimestampType extends SqlIdentifier implements ExtendedSqlType {
	private final int precision;
	private final boolean withLocalTimeZone;

	public SqlTimestampType(SqlParserPos pos, int precision, boolean withLocalTimeZone) {
		super(getTypeName(withLocalTimeZone), pos);
		this.precision = precision;
		this.withLocalTimeZone = withLocalTimeZone;
	}

	private static String getTypeName(boolean withLocalTimeZone) {
		if (withLocalTimeZone) {
			return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name();
		} else {
			return SqlTypeName.TIMESTAMP.name();
		}
	}

	public SqlTypeName getSqlTypeName() {
		if (withLocalTimeZone) {
			return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
		} else {
			return SqlTypeName.TIMESTAMP;
		}
	}

	public int getPrecision() {
		return this.precision;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(SqlTypeName.TIMESTAMP.name());
		if (this.precision >= 0) {
			final SqlWriter.Frame frame =
				writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
			writer.print(precision);
			writer.endList(frame);
		}
		if (this.withLocalTimeZone) {
			writer.keyword("WITH LOCAL TIME ZONE");
		}
	}
}
