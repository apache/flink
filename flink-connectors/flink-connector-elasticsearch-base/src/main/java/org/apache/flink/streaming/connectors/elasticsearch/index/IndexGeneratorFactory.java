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

package org.apache.flink.streaming.connectors.elasticsearch.index;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory of {@link IndexGenerator}.
 *
 * <p>Flink supports both static index and dynamic index.
 *
 * <p>If you want to have a static index, this option value should be a plain string, e.g.
 * 'myusers', all the records will be consistently written into "myusers" index.
 *
 * <p>If you want to have a dynamic index, you can use '{field_name}' to reference a field value in
 * the record to dynamically generate a target index. You can also use
 * '{field_name|date_format_string}' to convert a field value of TIMESTAMP/DATE/TIME type into the
 * format specified by date_format_string. The date_format_string is compatible with {@link
 * java.text.SimpleDateFormat}. For example, if the option value is 'myusers_{log_ts|yyyy-MM-dd}',
 * then a record with log_ts field value 2020-03-27 12:25:55 will be written into
 * "myusers-2020-03-27" index.
 */
@Internal
public class IndexGeneratorFactory {

    private IndexGeneratorFactory() {}

    public static IndexGenerator createIndexGenerator(String index, TableSchema schema) {
        final IndexHelper indexHelper = new IndexHelper();
        if (indexHelper.checkIsDynamicIndex(index)) {
            return createRuntimeIndexGenerator(
                    index, schema.getFieldNames(), schema.getFieldDataTypes(), indexHelper);
        } else {
            return new StaticIndexGenerator(index);
        }
    }

    private static IndexGenerator createRuntimeIndexGenerator(
            String index, String[] fieldNames, DataType[] fieldTypes, IndexHelper indexHelper) {
        final String dynamicIndexPatternStr = indexHelper.extractDynamicIndexPatternStr(index);
        final String indexPrefix = index.substring(0, index.indexOf(dynamicIndexPatternStr));
        final String indexSuffix =
                index.substring(indexPrefix.length() + dynamicIndexPatternStr.length());

        final boolean isDynamicIndexWithFormat = indexHelper.checkIsDynamicIndexWithFormat(index);
        final int indexFieldPos =
                indexHelper.extractIndexFieldPos(index, fieldNames, isDynamicIndexWithFormat);
        final TypeInformation<?> indexFieldType =
                TypeConversions.fromDataTypeToLegacyInfo(fieldTypes[indexFieldPos]);

        // validate index field type
        indexHelper.validateIndexFieldType(indexFieldType);

        // time extract dynamic index pattern
        if (isDynamicIndexWithFormat) {
            final String dateTimeFormat = indexHelper.extractDateFormat(index, indexFieldType);
            // DataTypes.SQL_TIMESTAMP
            if (indexFieldType == Types.LOCAL_DATE_TIME) {
                return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
                    @Override
                    public String generate(Row row) {
                        LocalDateTime indexField = (LocalDateTime) row.getField(indexFieldPos);
                        String indexFieldValueStr = indexField.format(dateTimeFormatter);
                        return indexPrefix.concat(indexFieldValueStr).concat(indexSuffix);
                    }
                };
            } else if (indexFieldType == Types.SQL_TIMESTAMP) {
                return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
                    @Override
                    public String generate(Row row) {
                        Timestamp indexField = (Timestamp) row.getField(indexFieldPos);
                        String indexFieldValueStr =
                                indexField.toLocalDateTime().format(dateTimeFormatter);
                        return indexPrefix.concat(indexFieldValueStr).concat(indexSuffix);
                    }
                };
            }
            // DataTypes.SQL_DATE
            else if (indexFieldType == Types.LOCAL_DATE) {
                return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
                    @Override
                    public String generate(Row row) {
                        LocalDate indexField = (LocalDate) row.getField(indexFieldPos);
                        String indexFieldValueStr = indexField.format(dateTimeFormatter);
                        return indexPrefix.concat(indexFieldValueStr).concat(indexSuffix);
                    }
                };
            } else if (indexFieldType == Types.SQL_DATE) {
                return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
                    @Override
                    public String generate(Row row) {
                        Date indexField = (Date) row.getField(indexFieldPos);
                        String indexFieldValueStr =
                                indexField.toLocalDate().format(dateTimeFormatter);
                        return indexPrefix.concat(indexFieldValueStr).concat(indexSuffix);
                    }
                };
            } // DataTypes.TIME
            else if (indexFieldType == Types.LOCAL_TIME) {
                return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
                    @Override
                    public String generate(Row row) {
                        LocalTime indexField = (LocalTime) row.getField(indexFieldPos);
                        String indexFieldValueStr = indexField.format(dateTimeFormatter);
                        return indexPrefix.concat(indexFieldValueStr).concat(indexSuffix);
                    }
                };
            } else if (indexFieldType == Types.SQL_TIME) {
                return new AbstractTimeIndexGenerator(index, dateTimeFormat) {
                    @Override
                    public String generate(Row row) {
                        Time indexField = (Time) row.getField(indexFieldPos);
                        String indexFieldValueStr =
                                indexField.toLocalTime().format(dateTimeFormatter);
                        return indexPrefix.concat(indexFieldValueStr).concat(indexSuffix);
                    }
                };
            } else {
                throw new TableException(
                        String.format(
                                "Unsupported type '%s' found in Elasticsearch dynamic index field, "
                                        + "time-related pattern only support types are: DATE,TIME,TIMESTAMP.",
                                TypeConversions.fromLegacyInfoToDataType(indexFieldType)));
            }
        }
        // general dynamic index pattern
        return new IndexGeneratorBase(index) {
            @Override
            public String generate(Row row) {
                Object indexField = row.getField(indexFieldPos);
                return indexPrefix
                        .concat(indexField == null ? "null" : indexField.toString())
                        .concat(indexSuffix);
            }
        };
    }

    /**
     * Helper class for {@link IndexGeneratorFactory}, this helper can use to validate index field
     * type ans parse index format from pattern.
     */
    private static class IndexHelper {
        private static final Pattern dynamicIndexPattern = Pattern.compile("\\{[^\\{\\}]+\\}?");
        private static final Pattern dynamicIndexTimeExtractPattern =
                Pattern.compile(".*\\{.+\\|.*\\}.*");
        private static final List<TypeInformation> supportedTypes = new ArrayList<>();
        private static final Map<TypeInformation, String> defaultFormats = new HashMap<>();

        static {
            // time related types
            supportedTypes.add(Types.LOCAL_DATE_TIME);
            supportedTypes.add(Types.SQL_TIMESTAMP);
            supportedTypes.add(Types.LOCAL_DATE);
            supportedTypes.add(Types.SQL_DATE);
            supportedTypes.add(Types.LOCAL_TIME);
            supportedTypes.add(Types.SQL_TIME);
            // general types
            supportedTypes.add(Types.STRING);
            supportedTypes.add(Types.SHORT);
            supportedTypes.add(Types.INT);
            supportedTypes.add(Types.LONG);
        }

        static {
            defaultFormats.put(Types.LOCAL_DATE_TIME, "yyyy_MM_dd_HH_mm_ss");
            defaultFormats.put(Types.SQL_TIMESTAMP, "yyyy_MM_dd_HH_mm_ss");
            defaultFormats.put(Types.LOCAL_DATE, "yyyy_MM_dd");
            defaultFormats.put(Types.SQL_DATE, "yyyy_MM_dd");
            defaultFormats.put(Types.LOCAL_TIME, "HH_mm_ss");
            defaultFormats.put(Types.SQL_TIME, "HH_mm_ss");
        }

        /** Validate the index field Type. */
        void validateIndexFieldType(TypeInformation indexTypeInfo) {
            if (!supportedTypes.contains(indexTypeInfo)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported type %s of index field, " + "Supported types are: %s",
                                indexTypeInfo, supportedTypes));
            }
        }

        /** Get the default date format. */
        String getDefaultFormat(TypeInformation indexTypeInfo) {
            return defaultFormats.get(indexTypeInfo);
        }

        /** Check general dynamic index is enabled or not by index pattern. */
        boolean checkIsDynamicIndex(String index) {
            final Matcher matcher = dynamicIndexPattern.matcher(index);
            int count = 0;
            while (matcher.find()) {
                count++;
            }
            if (count > 1) {
                throw new TableException(
                        String.format(
                                "Chaining dynamic index pattern %s is not supported,"
                                        + " only support single dynamic index pattern.",
                                index));
            }
            return count == 1;
        }

        /** Check time extract dynamic index is enabled or not by index pattern. */
        boolean checkIsDynamicIndexWithFormat(String index) {
            return dynamicIndexTimeExtractPattern.matcher(index).matches();
        }

        /** Extract dynamic index pattern string from index pattern string. */
        String extractDynamicIndexPatternStr(String index) {
            int start = index.indexOf("{");
            int end = index.lastIndexOf("}");
            return index.substring(start, end + 1);
        }

        /** Extract index field position in a fieldNames, return the field position. */
        int extractIndexFieldPos(
                String index, String[] fieldNames, boolean isDynamicIndexWithFormat) {
            List<String> fieldList = Arrays.asList(fieldNames);
            String indexFieldName;
            if (isDynamicIndexWithFormat) {
                indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("|"));
            } else {
                indexFieldName = index.substring(index.indexOf("{") + 1, index.indexOf("}"));
            }
            if (!fieldList.contains(indexFieldName)) {
                throw new TableException(
                        String.format(
                                "Unknown field '%s' in index pattern '%s', please check the field name.",
                                indexFieldName, index));
            }
            return fieldList.indexOf(indexFieldName);
        }

        /** Extract dateTime format by the date format that extracted from index pattern string. */
        private String extractDateFormat(String index, TypeInformation indexTypeInfo) {
            String format = index.substring(index.indexOf("|") + 1, index.indexOf("}"));
            if ("".equals(format)) {
                format = getDefaultFormat(indexTypeInfo);
            }
            return format;
        }
    }
}
