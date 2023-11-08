################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CoderInfoDescriptor(_message.Message):
    __slots__ = ["arrow_type", "flatten_row_type", "mode", "over_window_arrow_type", "raw_type", "row_type", "separated_with_end_message"]
    class Mode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class ArrowType(_message.Message):
        __slots__ = ["schema"]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class FlattenRowType(_message.Message):
        __slots__ = ["schema"]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class OverWindowArrowType(_message.Message):
        __slots__ = ["schema"]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class RawType(_message.Message):
        __slots__ = ["type_info"]
        TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
        type_info: TypeInfo
        def __init__(self, type_info: _Optional[_Union[TypeInfo, _Mapping]] = ...) -> None: ...
    class RowType(_message.Message):
        __slots__ = ["schema"]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLATTEN_ROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    MULTIPLE: CoderInfoDescriptor.Mode
    OVER_WINDOW_ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    RAW_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    SEPARATED_WITH_END_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SINGLE: CoderInfoDescriptor.Mode
    arrow_type: CoderInfoDescriptor.ArrowType
    flatten_row_type: CoderInfoDescriptor.FlattenRowType
    mode: CoderInfoDescriptor.Mode
    over_window_arrow_type: CoderInfoDescriptor.OverWindowArrowType
    raw_type: CoderInfoDescriptor.RawType
    row_type: CoderInfoDescriptor.RowType
    separated_with_end_message: bool
    def __init__(self, flatten_row_type: _Optional[_Union[CoderInfoDescriptor.FlattenRowType, _Mapping]] = ..., row_type: _Optional[_Union[CoderInfoDescriptor.RowType, _Mapping]] = ..., arrow_type: _Optional[_Union[CoderInfoDescriptor.ArrowType, _Mapping]] = ..., over_window_arrow_type: _Optional[_Union[CoderInfoDescriptor.OverWindowArrowType, _Mapping]] = ..., raw_type: _Optional[_Union[CoderInfoDescriptor.RawType, _Mapping]] = ..., mode: _Optional[_Union[CoderInfoDescriptor.Mode, str]] = ..., separated_with_end_message: bool = ...) -> None: ...

class GroupWindow(_message.Message):
    __slots__ = ["allowedLateness", "is_row_time", "is_time_window", "namedProperties", "shift_timezone", "time_field_index", "window_gap", "window_size", "window_slide", "window_type"]
    class WindowProperty(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class WindowType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    ALLOWEDLATENESS_FIELD_NUMBER: _ClassVar[int]
    IS_ROW_TIME_FIELD_NUMBER: _ClassVar[int]
    IS_TIME_WINDOW_FIELD_NUMBER: _ClassVar[int]
    NAMEDPROPERTIES_FIELD_NUMBER: _ClassVar[int]
    PROC_TIME_ATTRIBUTE: GroupWindow.WindowProperty
    ROW_TIME_ATTRIBUTE: GroupWindow.WindowProperty
    SESSION_GROUP_WINDOW: GroupWindow.WindowType
    SHIFT_TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    SLIDING_GROUP_WINDOW: GroupWindow.WindowType
    TIME_FIELD_INDEX_FIELD_NUMBER: _ClassVar[int]
    TUMBLING_GROUP_WINDOW: GroupWindow.WindowType
    WINDOW_END: GroupWindow.WindowProperty
    WINDOW_GAP_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SIZE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SLIDE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_START: GroupWindow.WindowProperty
    WINDOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    allowedLateness: int
    is_row_time: bool
    is_time_window: bool
    namedProperties: _containers.RepeatedScalarFieldContainer[GroupWindow.WindowProperty]
    shift_timezone: str
    time_field_index: int
    window_gap: int
    window_size: int
    window_slide: int
    window_type: GroupWindow.WindowType
    def __init__(self, window_type: _Optional[_Union[GroupWindow.WindowType, str]] = ..., is_time_window: bool = ..., window_slide: _Optional[int] = ..., window_size: _Optional[int] = ..., window_gap: _Optional[int] = ..., is_row_time: bool = ..., time_field_index: _Optional[int] = ..., allowedLateness: _Optional[int] = ..., namedProperties: _Optional[_Iterable[_Union[GroupWindow.WindowProperty, str]]] = ..., shift_timezone: _Optional[str] = ...) -> None: ...

class Input(_message.Message):
    __slots__ = ["inputConstant", "inputOffset", "udf"]
    INPUTCONSTANT_FIELD_NUMBER: _ClassVar[int]
    INPUTOFFSET_FIELD_NUMBER: _ClassVar[int]
    UDF_FIELD_NUMBER: _ClassVar[int]
    inputConstant: bytes
    inputOffset: int
    udf: UserDefinedFunction
    def __init__(self, udf: _Optional[_Union[UserDefinedFunction, _Mapping]] = ..., inputOffset: _Optional[int] = ..., inputConstant: _Optional[bytes] = ...) -> None: ...

class JobParameter(_message.Message):
    __slots__ = ["key", "value"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class OverWindow(_message.Message):
    __slots__ = ["lower_boundary", "upper_boundary", "window_type"]
    class WindowType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    LOWER_BOUNDARY_FIELD_NUMBER: _ClassVar[int]
    RANGE_SLIDING: OverWindow.WindowType
    RANGE_UNBOUNDED: OverWindow.WindowType
    RANGE_UNBOUNDED_FOLLOWING: OverWindow.WindowType
    RANGE_UNBOUNDED_PRECEDING: OverWindow.WindowType
    ROW_SLIDING: OverWindow.WindowType
    ROW_UNBOUNDED: OverWindow.WindowType
    ROW_UNBOUNDED_FOLLOWING: OverWindow.WindowType
    ROW_UNBOUNDED_PRECEDING: OverWindow.WindowType
    UPPER_BOUNDARY_FIELD_NUMBER: _ClassVar[int]
    WINDOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    lower_boundary: int
    upper_boundary: int
    window_type: OverWindow.WindowType
    def __init__(self, window_type: _Optional[_Union[OverWindow.WindowType, str]] = ..., lower_boundary: _Optional[int] = ..., upper_boundary: _Optional[int] = ...) -> None: ...

class Schema(_message.Message):
    __slots__ = ["fields"]
    class TypeName(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class BinaryInfo(_message.Message):
        __slots__ = ["length"]
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class CharInfo(_message.Message):
        __slots__ = ["length"]
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class DecimalInfo(_message.Message):
        __slots__ = ["precision", "scale"]
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        SCALE_FIELD_NUMBER: _ClassVar[int]
        precision: int
        scale: int
        def __init__(self, precision: _Optional[int] = ..., scale: _Optional[int] = ...) -> None: ...
    class Field(_message.Message):
        __slots__ = ["description", "name", "type"]
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        description: str
        name: str
        type: Schema.FieldType
        def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
    class FieldType(_message.Message):
        __slots__ = ["binary_info", "char_info", "collection_element_type", "decimal_info", "local_zoned_timestamp_info", "map_info", "nullable", "row_schema", "time_info", "timestamp_info", "type_name", "var_binary_info", "var_char_info", "zoned_timestamp_info"]
        BINARY_INFO_FIELD_NUMBER: _ClassVar[int]
        CHAR_INFO_FIELD_NUMBER: _ClassVar[int]
        COLLECTION_ELEMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
        DECIMAL_INFO_FIELD_NUMBER: _ClassVar[int]
        LOCAL_ZONED_TIMESTAMP_INFO_FIELD_NUMBER: _ClassVar[int]
        MAP_INFO_FIELD_NUMBER: _ClassVar[int]
        NULLABLE_FIELD_NUMBER: _ClassVar[int]
        ROW_SCHEMA_FIELD_NUMBER: _ClassVar[int]
        TIMESTAMP_INFO_FIELD_NUMBER: _ClassVar[int]
        TIME_INFO_FIELD_NUMBER: _ClassVar[int]
        TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
        VAR_BINARY_INFO_FIELD_NUMBER: _ClassVar[int]
        VAR_CHAR_INFO_FIELD_NUMBER: _ClassVar[int]
        ZONED_TIMESTAMP_INFO_FIELD_NUMBER: _ClassVar[int]
        binary_info: Schema.BinaryInfo
        char_info: Schema.CharInfo
        collection_element_type: Schema.FieldType
        decimal_info: Schema.DecimalInfo
        local_zoned_timestamp_info: Schema.LocalZonedTimestampInfo
        map_info: Schema.MapInfo
        nullable: bool
        row_schema: Schema
        time_info: Schema.TimeInfo
        timestamp_info: Schema.TimestampInfo
        type_name: Schema.TypeName
        var_binary_info: Schema.VarBinaryInfo
        var_char_info: Schema.VarCharInfo
        zoned_timestamp_info: Schema.ZonedTimestampInfo
        def __init__(self, type_name: _Optional[_Union[Schema.TypeName, str]] = ..., nullable: bool = ..., collection_element_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., map_info: _Optional[_Union[Schema.MapInfo, _Mapping]] = ..., row_schema: _Optional[_Union[Schema, _Mapping]] = ..., decimal_info: _Optional[_Union[Schema.DecimalInfo, _Mapping]] = ..., time_info: _Optional[_Union[Schema.TimeInfo, _Mapping]] = ..., timestamp_info: _Optional[_Union[Schema.TimestampInfo, _Mapping]] = ..., local_zoned_timestamp_info: _Optional[_Union[Schema.LocalZonedTimestampInfo, _Mapping]] = ..., zoned_timestamp_info: _Optional[_Union[Schema.ZonedTimestampInfo, _Mapping]] = ..., binary_info: _Optional[_Union[Schema.BinaryInfo, _Mapping]] = ..., var_binary_info: _Optional[_Union[Schema.VarBinaryInfo, _Mapping]] = ..., char_info: _Optional[_Union[Schema.CharInfo, _Mapping]] = ..., var_char_info: _Optional[_Union[Schema.VarCharInfo, _Mapping]] = ...) -> None: ...
    class LocalZonedTimestampInfo(_message.Message):
        __slots__ = ["precision"]
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class MapInfo(_message.Message):
        __slots__ = ["key_type", "value_type"]
        KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
        key_type: Schema.FieldType
        value_type: Schema.FieldType
        def __init__(self, key_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., value_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
    class TimeInfo(_message.Message):
        __slots__ = ["precision"]
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class TimestampInfo(_message.Message):
        __slots__ = ["precision"]
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class VarBinaryInfo(_message.Message):
        __slots__ = ["length"]
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class VarCharInfo(_message.Message):
        __slots__ = ["length"]
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class ZonedTimestampInfo(_message.Message):
        __slots__ = ["precision"]
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    BASIC_ARRAY: Schema.TypeName
    BIGINT: Schema.TypeName
    BINARY: Schema.TypeName
    BOOLEAN: Schema.TypeName
    CHAR: Schema.TypeName
    DATE: Schema.TypeName
    DECIMAL: Schema.TypeName
    DOUBLE: Schema.TypeName
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    FLOAT: Schema.TypeName
    INT: Schema.TypeName
    LOCAL_ZONED_TIMESTAMP: Schema.TypeName
    MAP: Schema.TypeName
    MULTISET: Schema.TypeName
    NULL: Schema.TypeName
    ROW: Schema.TypeName
    SMALLINT: Schema.TypeName
    TIME: Schema.TypeName
    TIMESTAMP: Schema.TypeName
    TINYINT: Schema.TypeName
    VARBINARY: Schema.TypeName
    VARCHAR: Schema.TypeName
    ZONED_TIMESTAMP: Schema.TypeName
    fields: _containers.RepeatedCompositeFieldContainer[Schema.Field]
    def __init__(self, fields: _Optional[_Iterable[_Union[Schema.Field, _Mapping]]] = ...) -> None: ...

class StateDescriptor(_message.Message):
    __slots__ = ["state_name", "state_ttl_config"]
    class StateTTLConfig(_message.Message):
        __slots__ = ["cleanup_strategies", "state_visibility", "ttl", "ttl_time_characteristic", "update_type"]
        class StateVisibility(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = []
        class TtlTimeCharacteristic(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = []
        class UpdateType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = []
        class CleanupStrategies(_message.Message):
            __slots__ = ["is_cleanup_in_background", "strategies"]
            class EmptyCleanupStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = []
            class Strategies(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = []
            class IncrementalCleanupStrategy(_message.Message):
                __slots__ = ["cleanup_size", "run_cleanup_for_every_record"]
                CLEANUP_SIZE_FIELD_NUMBER: _ClassVar[int]
                RUN_CLEANUP_FOR_EVERY_RECORD_FIELD_NUMBER: _ClassVar[int]
                cleanup_size: int
                run_cleanup_for_every_record: bool
                def __init__(self, cleanup_size: _Optional[int] = ..., run_cleanup_for_every_record: bool = ...) -> None: ...
            class MapStrategiesEntry(_message.Message):
                __slots__ = ["empty_strategy", "incremental_cleanup_strategy", "rocksdb_compact_filter_cleanup_strategy", "strategy"]
                EMPTY_STRATEGY_FIELD_NUMBER: _ClassVar[int]
                INCREMENTAL_CLEANUP_STRATEGY_FIELD_NUMBER: _ClassVar[int]
                ROCKSDB_COMPACT_FILTER_CLEANUP_STRATEGY_FIELD_NUMBER: _ClassVar[int]
                STRATEGY_FIELD_NUMBER: _ClassVar[int]
                empty_strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy
                incremental_cleanup_strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.IncrementalCleanupStrategy
                rocksdb_compact_filter_cleanup_strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy
                strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
                def __init__(self, strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies, str]] = ..., empty_strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy, str]] = ..., incremental_cleanup_strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.IncrementalCleanupStrategy, _Mapping]] = ..., rocksdb_compact_filter_cleanup_strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy, _Mapping]] = ...) -> None: ...
            class RocksdbCompactFilterCleanupStrategy(_message.Message):
                __slots__ = ["query_time_after_num_entries"]
                QUERY_TIME_AFTER_NUM_ENTRIES_FIELD_NUMBER: _ClassVar[int]
                query_time_after_num_entries: int
                def __init__(self, query_time_after_num_entries: _Optional[int] = ...) -> None: ...
            EMPTY_STRATEGY: StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy
            FULL_STATE_SCAN_SNAPSHOT: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
            INCREMENTAL_CLEANUP: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
            IS_CLEANUP_IN_BACKGROUND_FIELD_NUMBER: _ClassVar[int]
            ROCKSDB_COMPACTION_FILTER: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
            STRATEGIES_FIELD_NUMBER: _ClassVar[int]
            is_cleanup_in_background: bool
            strategies: _containers.RepeatedCompositeFieldContainer[StateDescriptor.StateTTLConfig.CleanupStrategies.MapStrategiesEntry]
            def __init__(self, is_cleanup_in_background: bool = ..., strategies: _Optional[_Iterable[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.MapStrategiesEntry, _Mapping]]] = ...) -> None: ...
        CLEANUP_STRATEGIES_FIELD_NUMBER: _ClassVar[int]
        Disabled: StateDescriptor.StateTTLConfig.UpdateType
        NeverReturnExpired: StateDescriptor.StateTTLConfig.StateVisibility
        OnCreateAndWrite: StateDescriptor.StateTTLConfig.UpdateType
        OnReadAndWrite: StateDescriptor.StateTTLConfig.UpdateType
        ProcessingTime: StateDescriptor.StateTTLConfig.TtlTimeCharacteristic
        ReturnExpiredIfNotCleanedUp: StateDescriptor.StateTTLConfig.StateVisibility
        STATE_VISIBILITY_FIELD_NUMBER: _ClassVar[int]
        TTL_FIELD_NUMBER: _ClassVar[int]
        TTL_TIME_CHARACTERISTIC_FIELD_NUMBER: _ClassVar[int]
        UPDATE_TYPE_FIELD_NUMBER: _ClassVar[int]
        cleanup_strategies: StateDescriptor.StateTTLConfig.CleanupStrategies
        state_visibility: StateDescriptor.StateTTLConfig.StateVisibility
        ttl: int
        ttl_time_characteristic: StateDescriptor.StateTTLConfig.TtlTimeCharacteristic
        update_type: StateDescriptor.StateTTLConfig.UpdateType
        def __init__(self, update_type: _Optional[_Union[StateDescriptor.StateTTLConfig.UpdateType, str]] = ..., state_visibility: _Optional[_Union[StateDescriptor.StateTTLConfig.StateVisibility, str]] = ..., ttl_time_characteristic: _Optional[_Union[StateDescriptor.StateTTLConfig.TtlTimeCharacteristic, str]] = ..., ttl: _Optional[int] = ..., cleanup_strategies: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies, _Mapping]] = ...) -> None: ...
    STATE_NAME_FIELD_NUMBER: _ClassVar[int]
    STATE_TTL_CONFIG_FIELD_NUMBER: _ClassVar[int]
    state_name: str
    state_ttl_config: StateDescriptor.StateTTLConfig
    def __init__(self, state_name: _Optional[str] = ..., state_ttl_config: _Optional[_Union[StateDescriptor.StateTTLConfig, _Mapping]] = ...) -> None: ...

class TypeInfo(_message.Message):
    __slots__ = ["avro_type_info", "collection_element_type", "map_type_info", "row_type_info", "tuple_type_info", "type_name"]
    class TypeName(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class AvroTypeInfo(_message.Message):
        __slots__ = ["schema"]
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: str
        def __init__(self, schema: _Optional[str] = ...) -> None: ...
    class MapTypeInfo(_message.Message):
        __slots__ = ["key_type", "value_type"]
        KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
        key_type: TypeInfo
        value_type: TypeInfo
        def __init__(self, key_type: _Optional[_Union[TypeInfo, _Mapping]] = ..., value_type: _Optional[_Union[TypeInfo, _Mapping]] = ...) -> None: ...
    class RowTypeInfo(_message.Message):
        __slots__ = ["fields"]
        class Field(_message.Message):
            __slots__ = ["field_name", "field_type"]
            FIELD_NAME_FIELD_NUMBER: _ClassVar[int]
            FIELD_TYPE_FIELD_NUMBER: _ClassVar[int]
            field_name: str
            field_type: TypeInfo
            def __init__(self, field_name: _Optional[str] = ..., field_type: _Optional[_Union[TypeInfo, _Mapping]] = ...) -> None: ...
        FIELDS_FIELD_NUMBER: _ClassVar[int]
        fields: _containers.RepeatedCompositeFieldContainer[TypeInfo.RowTypeInfo.Field]
        def __init__(self, fields: _Optional[_Iterable[_Union[TypeInfo.RowTypeInfo.Field, _Mapping]]] = ...) -> None: ...
    class TupleTypeInfo(_message.Message):
        __slots__ = ["field_types"]
        FIELD_TYPES_FIELD_NUMBER: _ClassVar[int]
        field_types: _containers.RepeatedCompositeFieldContainer[TypeInfo]
        def __init__(self, field_types: _Optional[_Iterable[_Union[TypeInfo, _Mapping]]] = ...) -> None: ...
    AVRO: TypeInfo.TypeName
    AVRO_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    BASIC_ARRAY: TypeInfo.TypeName
    BIG_DEC: TypeInfo.TypeName
    BIG_INT: TypeInfo.TypeName
    BOOLEAN: TypeInfo.TypeName
    BYTE: TypeInfo.TypeName
    CHAR: TypeInfo.TypeName
    COLLECTION_ELEMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE: TypeInfo.TypeName
    FLOAT: TypeInfo.TypeName
    INSTANT: TypeInfo.TypeName
    INT: TypeInfo.TypeName
    LIST: TypeInfo.TypeName
    LOCAL_DATE: TypeInfo.TypeName
    LOCAL_DATETIME: TypeInfo.TypeName
    LOCAL_TIME: TypeInfo.TypeName
    LOCAL_ZONED_TIMESTAMP: TypeInfo.TypeName
    LONG: TypeInfo.TypeName
    MAP: TypeInfo.TypeName
    MAP_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ARRAY: TypeInfo.TypeName
    PICKLED_BYTES: TypeInfo.TypeName
    PRIMITIVE_ARRAY: TypeInfo.TypeName
    ROW: TypeInfo.TypeName
    ROW_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    SHORT: TypeInfo.TypeName
    SQL_DATE: TypeInfo.TypeName
    SQL_TIME: TypeInfo.TypeName
    SQL_TIMESTAMP: TypeInfo.TypeName
    STRING: TypeInfo.TypeName
    TUPLE: TypeInfo.TypeName
    TUPLE_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    avro_type_info: TypeInfo.AvroTypeInfo
    collection_element_type: TypeInfo
    map_type_info: TypeInfo.MapTypeInfo
    row_type_info: TypeInfo.RowTypeInfo
    tuple_type_info: TypeInfo.TupleTypeInfo
    type_name: TypeInfo.TypeName
    def __init__(self, type_name: _Optional[_Union[TypeInfo.TypeName, str]] = ..., collection_element_type: _Optional[_Union[TypeInfo, _Mapping]] = ..., row_type_info: _Optional[_Union[TypeInfo.RowTypeInfo, _Mapping]] = ..., tuple_type_info: _Optional[_Union[TypeInfo.TupleTypeInfo, _Mapping]] = ..., map_type_info: _Optional[_Union[TypeInfo.MapTypeInfo, _Mapping]] = ..., avro_type_info: _Optional[_Union[TypeInfo.AvroTypeInfo, _Mapping]] = ...) -> None: ...

class UserDefinedAggregateFunction(_message.Message):
    __slots__ = ["distinct", "filter_arg", "inputs", "payload", "specs", "takes_row_as_input"]
    class DataViewSpec(_message.Message):
        __slots__ = ["field_index", "list_view", "map_view", "name"]
        class ListView(_message.Message):
            __slots__ = ["element_type"]
            ELEMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
            element_type: Schema.FieldType
            def __init__(self, element_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
        class MapView(_message.Message):
            __slots__ = ["key_type", "value_type"]
            KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
            VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
            key_type: Schema.FieldType
            value_type: Schema.FieldType
            def __init__(self, key_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., value_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
        FIELD_INDEX_FIELD_NUMBER: _ClassVar[int]
        LIST_VIEW_FIELD_NUMBER: _ClassVar[int]
        MAP_VIEW_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        field_index: int
        list_view: UserDefinedAggregateFunction.DataViewSpec.ListView
        map_view: UserDefinedAggregateFunction.DataViewSpec.MapView
        name: str
        def __init__(self, name: _Optional[str] = ..., field_index: _Optional[int] = ..., list_view: _Optional[_Union[UserDefinedAggregateFunction.DataViewSpec.ListView, _Mapping]] = ..., map_view: _Optional[_Union[UserDefinedAggregateFunction.DataViewSpec.MapView, _Mapping]] = ...) -> None: ...
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    FILTER_ARG_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    TAKES_ROW_AS_INPUT_FIELD_NUMBER: _ClassVar[int]
    distinct: bool
    filter_arg: int
    inputs: _containers.RepeatedCompositeFieldContainer[Input]
    payload: bytes
    specs: _containers.RepeatedCompositeFieldContainer[UserDefinedAggregateFunction.DataViewSpec]
    takes_row_as_input: bool
    def __init__(self, payload: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[Input, _Mapping]]] = ..., specs: _Optional[_Iterable[_Union[UserDefinedAggregateFunction.DataViewSpec, _Mapping]]] = ..., filter_arg: _Optional[int] = ..., distinct: bool = ..., takes_row_as_input: bool = ...) -> None: ...

class UserDefinedAggregateFunctions(_message.Message):
    __slots__ = ["count_star_inserted", "generate_update_before", "group_window", "grouping", "index_of_count_star", "job_parameters", "key_type", "map_state_read_cache_size", "map_state_write_cache_size", "metric_enabled", "profile_enabled", "state_cache_size", "state_cleaning_enabled", "udfs"]
    COUNT_STAR_INSERTED_FIELD_NUMBER: _ClassVar[int]
    GENERATE_UPDATE_BEFORE_FIELD_NUMBER: _ClassVar[int]
    GROUPING_FIELD_NUMBER: _ClassVar[int]
    GROUP_WINDOW_FIELD_NUMBER: _ClassVar[int]
    INDEX_OF_COUNT_STAR_FIELD_NUMBER: _ClassVar[int]
    JOB_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_READ_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_WRITE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    METRIC_ENABLED_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    STATE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    STATE_CLEANING_ENABLED_FIELD_NUMBER: _ClassVar[int]
    UDFS_FIELD_NUMBER: _ClassVar[int]
    count_star_inserted: bool
    generate_update_before: bool
    group_window: GroupWindow
    grouping: _containers.RepeatedScalarFieldContainer[int]
    index_of_count_star: int
    job_parameters: _containers.RepeatedCompositeFieldContainer[JobParameter]
    key_type: Schema.FieldType
    map_state_read_cache_size: int
    map_state_write_cache_size: int
    metric_enabled: bool
    profile_enabled: bool
    state_cache_size: int
    state_cleaning_enabled: bool
    udfs: _containers.RepeatedCompositeFieldContainer[UserDefinedAggregateFunction]
    def __init__(self, udfs: _Optional[_Iterable[_Union[UserDefinedAggregateFunction, _Mapping]]] = ..., metric_enabled: bool = ..., grouping: _Optional[_Iterable[int]] = ..., generate_update_before: bool = ..., key_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., index_of_count_star: _Optional[int] = ..., state_cleaning_enabled: bool = ..., state_cache_size: _Optional[int] = ..., map_state_read_cache_size: _Optional[int] = ..., map_state_write_cache_size: _Optional[int] = ..., count_star_inserted: bool = ..., group_window: _Optional[_Union[GroupWindow, _Mapping]] = ..., profile_enabled: bool = ..., job_parameters: _Optional[_Iterable[_Union[JobParameter, _Mapping]]] = ...) -> None: ...

class UserDefinedDataStreamFunction(_message.Message):
    __slots__ = ["function_type", "has_side_output", "key_type_info", "map_state_read_cache_size", "map_state_write_cache_size", "metric_enabled", "payload", "profile_enabled", "runtime_context", "state_cache_size"]
    class FunctionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class RuntimeContext(_message.Message):
        __slots__ = ["attempt_number", "in_batch_execution_mode", "index_of_this_subtask", "job_parameters", "max_number_of_parallel_subtasks", "number_of_parallel_subtasks", "task_name", "task_name_with_subtasks"]
        ATTEMPT_NUMBER_FIELD_NUMBER: _ClassVar[int]
        INDEX_OF_THIS_SUBTASK_FIELD_NUMBER: _ClassVar[int]
        IN_BATCH_EXECUTION_MODE_FIELD_NUMBER: _ClassVar[int]
        JOB_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
        MAX_NUMBER_OF_PARALLEL_SUBTASKS_FIELD_NUMBER: _ClassVar[int]
        NUMBER_OF_PARALLEL_SUBTASKS_FIELD_NUMBER: _ClassVar[int]
        TASK_NAME_FIELD_NUMBER: _ClassVar[int]
        TASK_NAME_WITH_SUBTASKS_FIELD_NUMBER: _ClassVar[int]
        attempt_number: int
        in_batch_execution_mode: bool
        index_of_this_subtask: int
        job_parameters: _containers.RepeatedCompositeFieldContainer[JobParameter]
        max_number_of_parallel_subtasks: int
        number_of_parallel_subtasks: int
        task_name: str
        task_name_with_subtasks: str
        def __init__(self, task_name: _Optional[str] = ..., task_name_with_subtasks: _Optional[str] = ..., number_of_parallel_subtasks: _Optional[int] = ..., max_number_of_parallel_subtasks: _Optional[int] = ..., index_of_this_subtask: _Optional[int] = ..., attempt_number: _Optional[int] = ..., job_parameters: _Optional[_Iterable[_Union[JobParameter, _Mapping]]] = ..., in_batch_execution_mode: bool = ...) -> None: ...
    CO_BROADCAST_PROCESS: UserDefinedDataStreamFunction.FunctionType
    CO_PROCESS: UserDefinedDataStreamFunction.FunctionType
    FUNCTION_TYPE_FIELD_NUMBER: _ClassVar[int]
    HAS_SIDE_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    KEYED_CO_BROADCAST_PROCESS: UserDefinedDataStreamFunction.FunctionType
    KEYED_CO_PROCESS: UserDefinedDataStreamFunction.FunctionType
    KEYED_PROCESS: UserDefinedDataStreamFunction.FunctionType
    KEY_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_READ_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_WRITE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    METRIC_ENABLED_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PROCESS: UserDefinedDataStreamFunction.FunctionType
    PROFILE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    REVISE_OUTPUT: UserDefinedDataStreamFunction.FunctionType
    RUNTIME_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    STATE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    WINDOW: UserDefinedDataStreamFunction.FunctionType
    function_type: UserDefinedDataStreamFunction.FunctionType
    has_side_output: bool
    key_type_info: TypeInfo
    map_state_read_cache_size: int
    map_state_write_cache_size: int
    metric_enabled: bool
    payload: bytes
    profile_enabled: bool
    runtime_context: UserDefinedDataStreamFunction.RuntimeContext
    state_cache_size: int
    def __init__(self, function_type: _Optional[_Union[UserDefinedDataStreamFunction.FunctionType, str]] = ..., runtime_context: _Optional[_Union[UserDefinedDataStreamFunction.RuntimeContext, _Mapping]] = ..., payload: _Optional[bytes] = ..., metric_enabled: bool = ..., key_type_info: _Optional[_Union[TypeInfo, _Mapping]] = ..., profile_enabled: bool = ..., has_side_output: bool = ..., state_cache_size: _Optional[int] = ..., map_state_read_cache_size: _Optional[int] = ..., map_state_write_cache_size: _Optional[int] = ...) -> None: ...

class UserDefinedFunction(_message.Message):
    __slots__ = ["inputs", "is_pandas_udf", "payload", "takes_row_as_input", "window_index"]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    IS_PANDAS_UDF_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    TAKES_ROW_AS_INPUT_FIELD_NUMBER: _ClassVar[int]
    WINDOW_INDEX_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[Input]
    is_pandas_udf: bool
    payload: bytes
    takes_row_as_input: bool
    window_index: int
    def __init__(self, payload: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[Input, _Mapping]]] = ..., window_index: _Optional[int] = ..., takes_row_as_input: bool = ..., is_pandas_udf: bool = ...) -> None: ...

class UserDefinedFunctions(_message.Message):
    __slots__ = ["job_parameters", "metric_enabled", "profile_enabled", "udfs", "windows"]
    JOB_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    METRIC_ENABLED_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    UDFS_FIELD_NUMBER: _ClassVar[int]
    WINDOWS_FIELD_NUMBER: _ClassVar[int]
    job_parameters: _containers.RepeatedCompositeFieldContainer[JobParameter]
    metric_enabled: bool
    profile_enabled: bool
    udfs: _containers.RepeatedCompositeFieldContainer[UserDefinedFunction]
    windows: _containers.RepeatedCompositeFieldContainer[OverWindow]
    def __init__(self, udfs: _Optional[_Iterable[_Union[UserDefinedFunction, _Mapping]]] = ..., metric_enabled: bool = ..., windows: _Optional[_Iterable[_Union[OverWindow, _Mapping]]] = ..., profile_enabled: bool = ..., job_parameters: _Optional[_Iterable[_Union[JobParameter, _Mapping]]] = ...) -> None: ...
