/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.table.converter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import com.mongodb.internal.HexUtils;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonRegularExpression;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Tool class used to convert from {@link BsonValue} to {@link RowData}. * */
@Internal
public class BsonToRowDataConverters {

    // -------------------------------------------------------------------------------------
    // Runtime Converters
    // -------------------------------------------------------------------------------------

    /**
     * Runtime converter that converts {@link BsonValue} into objects of Flink Table & SQL internal
     * data structures.
     */
    @FunctionalInterface
    public interface BsonToRowDataConverter extends Serializable {
        Object convert(BsonValue bsonValue);
    }

    // --------------------------------------------------------------------------------
    // IMPORTANT! We use anonymous classes instead of lambdas for a reason here. It is
    // necessary because the maven shade plugin cannot relocate classes in
    // SerializedLambdas (MSHADE-260). On the other hand we want to relocate Bson for
    // sql-connector uber jars.
    // --------------------------------------------------------------------------------

    /** Creates a runtime converter which is null safe. */
    public static BsonToRowDataConverter createNullableConverter(LogicalType type) {
        return wrapIntoNullableInternalConverter(createConverter(type));
    }

    private static BsonToRowDataConverter wrapIntoNullableInternalConverter(
            BsonToRowDataConverter bsonToRowDataConverter) {
        return new BsonToRowDataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(BsonValue bsonValue) {
                if (bsonValue == null || bsonValue.isNull() || bsonValue instanceof BsonUndefined) {
                    return null;
                }
                if (bsonValue.isDecimal128() && bsonValue.asDecimal128().getValue().isNaN()) {
                    return null;
                }
                return bsonToRowDataConverter.convert(bsonValue);
            }
        };
    }

    /** Creates a runtime converter which assuming input object is not null. */
    private static BsonToRowDataConverter createConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return null;
                    }
                };
            case BOOLEAN:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToBoolean(bsonValue);
                    }
                };
            case TINYINT:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToTinyInt(bsonValue);
                    }
                };
            case SMALLINT:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToSmallInt(bsonValue);
                    }
                };
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToInt(bsonValue);
                    }
                };
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToLong(bsonValue);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return TimestampData.fromLocalDateTime(convertToLocalDateTime(bsonValue));
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return TimestampData.fromInstant(convertToInstant(bsonValue));
                    }
                };
            case FLOAT:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToFloat(bsonValue);
                    }
                };
            case DOUBLE:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToDouble(bsonValue);
                    }
                };
            case CHAR:
            case VARCHAR:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return StringData.fromString(convertToString(bsonValue));
                    }
                };
            case BINARY:
            case VARBINARY:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        return convertToBinary(bsonValue);
                    }
                };
            case DECIMAL:
                return new BsonToRowDataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(BsonValue bsonValue) {
                        DecimalType decimalType = (DecimalType) type;
                        BigDecimal decimalValue = convertToBigDecimal(bsonValue);
                        return DecimalData.fromBigDecimal(
                                decimalValue, decimalType.getPrecision(), decimalType.getScale());
                    }
                };
            case ROW:
                return createRowConverter((RowType) type);
            case ARRAY:
                return createArrayConverter((ArrayType) type);
            case MAP:
                return createMapConverter((MapType) type);
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static BsonToRowDataConverter createRowConverter(RowType rowType) {
        final BsonToRowDataConverter[] fieldConverters =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .map(BsonToRowDataConverters::createNullableConverter)
                        .toArray(BsonToRowDataConverter[]::new);
        final int arity = rowType.getFieldCount();
        final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        return new BsonToRowDataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(BsonValue bsonValue) {
                if (!bsonValue.isDocument()) {
                    throw new IllegalArgumentException(
                            "Unable to convert to rowType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                BsonDocument document = bsonValue.asDocument();
                GenericRowData row = new GenericRowData(arity);
                for (int i = 0; i < arity; i++) {
                    String fieldName = fieldNames[i];
                    BsonValue fieldValue = document.get(fieldName);
                    Object convertedField = fieldConverters[i].convert(fieldValue);
                    row.setField(i, convertedField);
                }
                return row;
            }
        };
    }

    private static BsonToRowDataConverter createArrayConverter(ArrayType arrayType) {
        final Class<?> elementClass =
                LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
        final BsonToRowDataConverter elementConverter =
                createNullableConverter(arrayType.getElementType());

        return new BsonToRowDataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(BsonValue bsonValue) {
                if (!bsonValue.isArray()) {
                    throw new IllegalArgumentException(
                            "Unable to convert to arrayType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                List<BsonValue> in = bsonValue.asArray();
                final Object[] elementArray = (Object[]) Array.newInstance(elementClass, in.size());
                for (int i = 0; i < in.size(); i++) {
                    elementArray[i] = elementConverter.convert(in.get(i));
                }
                return new GenericArrayData(elementArray);
            }
        };
    }

    private static BsonToRowDataConverter createMapConverter(MapType mapType) {
        LogicalType keyType = mapType.getKeyType();
        checkArgument(keyType.supportsInputConversion(String.class));

        LogicalType valueType = mapType.getValueType();
        BsonToRowDataConverter valueConverter = createNullableConverter(valueType);

        return new BsonToRowDataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(BsonValue bsonValue) {
                if (!bsonValue.isDocument()) {
                    throw new IllegalArgumentException(
                            "Unable to convert to rowType from unexpected value '"
                                    + bsonValue
                                    + "' of type "
                                    + bsonValue.getBsonType());
                }

                BsonDocument document = bsonValue.asDocument();
                Map<StringData, Object> map = new HashMap<>();
                for (String key : document.keySet()) {
                    map.put(StringData.fromString(key), valueConverter.convert(document.get(key)));
                }
                return new GenericMapData(map);
            }
        };
    }

    private static boolean convertToBoolean(BsonValue bsonValue) {
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue();
        }
        if (bsonValue.isInt32()) {
            return bsonValue.asInt32().getValue() == 1;
        }
        if (bsonValue.isInt64()) {
            return bsonValue.asInt64().getValue() == 1L;
        }
        if (bsonValue.isString()) {
            return Boolean.parseBoolean(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to boolean from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static byte convertToTinyInt(BsonValue bsonValue) {
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue() ? (byte) 1 : (byte) 0;
        }
        if (bsonValue.isInt32()) {
            return (byte) bsonValue.asInt32().getValue();
        }
        if (bsonValue.isInt64()) {
            return (byte) bsonValue.asInt64().getValue();
        }
        if (bsonValue.isString()) {
            return Byte.parseByte(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to tinyint from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static short convertToSmallInt(BsonValue bsonValue) {
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue() ? (short) 1 : (short) 0;
        }
        if (bsonValue.isInt32()) {
            return (short) bsonValue.asInt32().getValue();
        }
        if (bsonValue.isInt64()) {
            return (short) bsonValue.asInt64().getValue();
        }
        if (bsonValue.isString()) {
            return Short.parseShort(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to smallint from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static int convertToInt(BsonValue bsonValue) {
        if (bsonValue.isNumber()) {
            return bsonValue.asNumber().intValue();
        }
        if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.intValue();
            } else if (decimal128Value.isNegative()) {
                return Integer.MIN_VALUE;
            } else {
                return Integer.MAX_VALUE;
            }
        }
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue() ? 1 : 0;
        }
        if (bsonValue.isDateTime()) {
            return (int) Instant.ofEpochMilli(bsonValue.asDateTime().getValue()).getEpochSecond();
        }
        if (bsonValue.isTimestamp()) {
            return bsonValue.asTimestamp().getTime();
        }
        if (bsonValue.isString()) {
            return Integer.parseInt(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to integer from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static long convertToLong(BsonValue bsonValue) {
        if (bsonValue.isNumber()) {
            return bsonValue.asNumber().longValue();
        }
        if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.longValue();
            } else if (decimal128Value.isNegative()) {
                return Long.MIN_VALUE;
            } else {
                return Long.MAX_VALUE;
            }
        }
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue() ? 1L : 0L;
        }
        if (bsonValue.isDateTime()) {
            return bsonValue.asDateTime().getValue();
        }
        if (bsonValue.isTimestamp()) {
            return bsonValue.asTimestamp().getTime() * 1000L;
        }
        if (bsonValue.isString()) {
            return Long.parseLong(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to long from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static double convertToDouble(BsonValue bsonValue) {
        if (bsonValue.isNumber()) {
            return bsonValue.asNumber().doubleValue();
        }
        if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.doubleValue();
            } else if (decimal128Value.isNegative()) {
                return -Double.MAX_VALUE;
            } else {
                return Double.MAX_VALUE;
            }
        }
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue() ? 1 : 0;
        }
        if (bsonValue.isString()) {
            return Double.parseDouble(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to double from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static float convertToFloat(BsonValue bsonValue) {
        if (bsonValue.isInt32()) {
            return bsonValue.asInt32().getValue();
        }
        if (bsonValue.isInt64()) {
            return bsonValue.asInt64().getValue();
        }
        if (bsonValue.isDouble()) {
            return ((Double) bsonValue.asDouble().getValue()).floatValue();
        }
        if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                return decimal128Value.floatValue();
            } else if (decimal128Value.isNegative()) {
                return -Float.MAX_VALUE;
            } else {
                return Float.MAX_VALUE;
            }
        }
        if (bsonValue.isBoolean()) {
            return bsonValue.asBoolean().getValue() ? 1f : 0f;
        }
        if (bsonValue.isString()) {
            return Float.parseFloat(bsonValue.asString().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to float from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static Instant convertToInstant(BsonValue bsonValue) {
        if (bsonValue.isTimestamp()) {
            return Instant.ofEpochSecond(bsonValue.asTimestamp().getTime());
        }
        if (bsonValue.isDateTime()) {
            return Instant.ofEpochMilli(bsonValue.asDateTime().getValue());
        }
        throw new IllegalArgumentException(
                "Unable to convert to Instant from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static LocalDateTime convertToLocalDateTime(BsonValue bsonValue) {
        Instant instant;
        if (bsonValue.isTimestamp()) {
            instant = Instant.ofEpochSecond(bsonValue.asTimestamp().getTime());
        } else if (bsonValue.isDateTime()) {
            instant = Instant.ofEpochMilli(bsonValue.asDateTime().getValue());
        } else {
            throw new IllegalArgumentException(
                    "Unable to convert to LocalDateTime from unexpected value '"
                            + bsonValue
                            + "' of type "
                            + bsonValue.getBsonType());
        }
        return Timestamp.from(instant).toLocalDateTime();
    }

    private static byte[] convertToBinary(BsonValue bsonValue) {
        if (bsonValue.isBinary()) {
            return bsonValue.asBinary().getData();
        }
        throw new IllegalArgumentException(
                "Unsupported BYTES value type: " + bsonValue.getClass().getSimpleName());
    }

    private static String convertToString(BsonValue bsonValue) {
        if (bsonValue.isString()) {
            return bsonValue.asString().getValue();
        }
        if (bsonValue.isBinary()) {
            BsonBinary bsonBinary = bsonValue.asBinary();
            if (BsonBinarySubType.isUuid(bsonBinary.getType())) {
                return bsonBinary.asUuid().toString();
            }
            return HexUtils.toHex(bsonBinary.getData());
        }
        if (bsonValue.isObjectId()) {
            return bsonValue.asObjectId().getValue().toHexString();
        }
        if (bsonValue.isInt32()) {
            return String.valueOf(bsonValue.asInt32().getValue());
        }
        if (bsonValue.isInt64()) {
            return String.valueOf(bsonValue.asInt64().getValue());
        }
        if (bsonValue.isDouble()) {
            return String.valueOf(bsonValue.asDouble().getValue());
        }
        if (bsonValue.isDecimal128()) {
            return bsonValue.asDecimal128().getValue().toString();
        }
        if (bsonValue.isBoolean()) {
            return String.valueOf(bsonValue.asBoolean().getValue());
        }
        if (bsonValue.isDateTime() || bsonValue.isTimestamp()) {
            Instant instant = convertToInstant(bsonValue);
            return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME);
        }
        if (bsonValue.isRegularExpression()) {
            BsonRegularExpression regex = bsonValue.asRegularExpression();
            return String.format("/%s/%s", regex.getPattern(), regex.getOptions());
        }
        if (bsonValue.isJavaScript()) {
            return bsonValue.asJavaScript().getCode();
        }
        if (bsonValue.isJavaScriptWithScope()) {
            return bsonValue.asJavaScriptWithScope().getCode();
        }
        if (bsonValue.isSymbol()) {
            return bsonValue.asSymbol().getSymbol();
        }
        if (bsonValue.isDBPointer()) {
            return bsonValue.asDBPointer().getId().toHexString();
        }
        if (bsonValue.isDocument()) {
            // convert document to json string
            return bsonValue.asDocument().toJson();
        }
        if (bsonValue.isArray()) {
            // convert bson array to json string
            Writer writer = new StringWriter();
            JsonWriter jsonArrayWriter =
                    new JsonWriter(writer) {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public void writeStartArray() {
                            doWriteStartArray();
                            setState(State.VALUE);
                        }

                        @Override
                        public void writeEndArray() {
                            doWriteEndArray();
                            setState(getNextState());
                        }
                    };

            new BsonArrayCodec()
                    .encode(jsonArrayWriter, bsonValue.asArray(), EncoderContext.builder().build());
            return writer.toString();
        }
        throw new IllegalArgumentException(
                "Unable to convert to string from unexpected value '"
                        + bsonValue
                        + "' of type "
                        + bsonValue.getBsonType());
    }

    private static BigDecimal convertToBigDecimal(BsonValue bsonValue) {
        BigDecimal decimalValue;
        if (bsonValue.isString()) {
            decimalValue = new BigDecimal(bsonValue.asString().getValue());
        } else if (bsonValue.isDecimal128()) {
            Decimal128 decimal128Value = bsonValue.asDecimal128().decimal128Value();
            if (decimal128Value.isFinite()) {
                decimalValue = bsonValue.asDecimal128().decimal128Value().bigDecimalValue();
            } else {
                // DecimalData doesn't have the concept of infinity.
                throw new IllegalArgumentException(
                        "Unable to convert infinite bson decimal to Decimal type.");
            }
        } else if (bsonValue.isDouble()) {
            decimalValue = BigDecimal.valueOf(bsonValue.asDouble().doubleValue());
        } else if (bsonValue.isInt32()) {
            decimalValue = BigDecimal.valueOf(bsonValue.asInt32().getValue());
        } else if (bsonValue.isInt64()) {
            decimalValue = BigDecimal.valueOf(bsonValue.asInt64().getValue());
        } else {
            throw new IllegalArgumentException(
                    "Unable to convert to decimal from unexpected value '"
                            + bsonValue
                            + "' of type "
                            + bsonValue.getBsonType());
        }
        return decimalValue;
    }
}
