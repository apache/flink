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

class JobParameter(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class Input(_message.Message):
    __slots__ = ("udf", "inputOffset", "inputConstant")
    UDF_FIELD_NUMBER: _ClassVar[int]
    INPUTOFFSET_FIELD_NUMBER: _ClassVar[int]
    INPUTCONSTANT_FIELD_NUMBER: _ClassVar[int]
    udf: UserDefinedFunction
    inputOffset: int
    inputConstant: bytes
    def __init__(self, udf: _Optional[_Union[UserDefinedFunction, _Mapping]] = ..., inputOffset: _Optional[int] = ..., inputConstant: _Optional[bytes] = ...) -> None: ...

class UserDefinedFunction(_message.Message):
    __slots__ = ("payload", "inputs", "window_index", "takes_row_as_input", "is_pandas_udf")
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    WINDOW_INDEX_FIELD_NUMBER: _ClassVar[int]
    TAKES_ROW_AS_INPUT_FIELD_NUMBER: _ClassVar[int]
    IS_PANDAS_UDF_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    inputs: _containers.RepeatedCompositeFieldContainer[Input]
    window_index: int
    takes_row_as_input: bool
    is_pandas_udf: bool
    def __init__(self, payload: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[Input, _Mapping]]] = ..., window_index: _Optional[int] = ..., takes_row_as_input: bool = ..., is_pandas_udf: bool = ...) -> None: ...

class UserDefinedFunctions(_message.Message):
    __slots__ = ("udfs", "metric_enabled", "windows", "profile_enabled", "job_parameters")
    UDFS_FIELD_NUMBER: _ClassVar[int]
    METRIC_ENABLED_FIELD_NUMBER: _ClassVar[int]
    WINDOWS_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    JOB_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    udfs: _containers.RepeatedCompositeFieldContainer[UserDefinedFunction]
    metric_enabled: bool
    windows: _containers.RepeatedCompositeFieldContainer[OverWindow]
    profile_enabled: bool
    job_parameters: _containers.RepeatedCompositeFieldContainer[JobParameter]
    def __init__(self, udfs: _Optional[_Iterable[_Union[UserDefinedFunction, _Mapping]]] = ..., metric_enabled: bool = ..., windows: _Optional[_Iterable[_Union[OverWindow, _Mapping]]] = ..., profile_enabled: bool = ..., job_parameters: _Optional[_Iterable[_Union[JobParameter, _Mapping]]] = ...) -> None: ...

class OverWindow(_message.Message):
    __slots__ = ("window_type", "lower_boundary", "upper_boundary")
    class WindowType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RANGE_UNBOUNDED: _ClassVar[OverWindow.WindowType]
        RANGE_UNBOUNDED_PRECEDING: _ClassVar[OverWindow.WindowType]
        RANGE_UNBOUNDED_FOLLOWING: _ClassVar[OverWindow.WindowType]
        RANGE_SLIDING: _ClassVar[OverWindow.WindowType]
        ROW_UNBOUNDED: _ClassVar[OverWindow.WindowType]
        ROW_UNBOUNDED_PRECEDING: _ClassVar[OverWindow.WindowType]
        ROW_UNBOUNDED_FOLLOWING: _ClassVar[OverWindow.WindowType]
        ROW_SLIDING: _ClassVar[OverWindow.WindowType]
    RANGE_UNBOUNDED: OverWindow.WindowType
    RANGE_UNBOUNDED_PRECEDING: OverWindow.WindowType
    RANGE_UNBOUNDED_FOLLOWING: OverWindow.WindowType
    RANGE_SLIDING: OverWindow.WindowType
    ROW_UNBOUNDED: OverWindow.WindowType
    ROW_UNBOUNDED_PRECEDING: OverWindow.WindowType
    ROW_UNBOUNDED_FOLLOWING: OverWindow.WindowType
    ROW_SLIDING: OverWindow.WindowType
    WINDOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    LOWER_BOUNDARY_FIELD_NUMBER: _ClassVar[int]
    UPPER_BOUNDARY_FIELD_NUMBER: _ClassVar[int]
    window_type: OverWindow.WindowType
    lower_boundary: int
    upper_boundary: int
    def __init__(self, window_type: _Optional[_Union[OverWindow.WindowType, str]] = ..., lower_boundary: _Optional[int] = ..., upper_boundary: _Optional[int] = ...) -> None: ...

class UserDefinedAggregateFunction(_message.Message):
    __slots__ = ("payload", "inputs", "specs", "filter_arg", "distinct", "takes_row_as_input")
    class DataViewSpec(_message.Message):
        __slots__ = ("name", "field_index", "list_view", "map_view")
        class ListView(_message.Message):
            __slots__ = ("element_type",)
            ELEMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
            element_type: Schema.FieldType
            def __init__(self, element_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
        class MapView(_message.Message):
            __slots__ = ("key_type", "value_type")
            KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
            VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
            key_type: Schema.FieldType
            value_type: Schema.FieldType
            def __init__(self, key_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., value_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
        NAME_FIELD_NUMBER: _ClassVar[int]
        FIELD_INDEX_FIELD_NUMBER: _ClassVar[int]
        LIST_VIEW_FIELD_NUMBER: _ClassVar[int]
        MAP_VIEW_FIELD_NUMBER: _ClassVar[int]
        name: str
        field_index: int
        list_view: UserDefinedAggregateFunction.DataViewSpec.ListView
        map_view: UserDefinedAggregateFunction.DataViewSpec.MapView
        def __init__(self, name: _Optional[str] = ..., field_index: _Optional[int] = ..., list_view: _Optional[_Union[UserDefinedAggregateFunction.DataViewSpec.ListView, _Mapping]] = ..., map_view: _Optional[_Union[UserDefinedAggregateFunction.DataViewSpec.MapView, _Mapping]] = ...) -> None: ...
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    SPECS_FIELD_NUMBER: _ClassVar[int]
    FILTER_ARG_FIELD_NUMBER: _ClassVar[int]
    DISTINCT_FIELD_NUMBER: _ClassVar[int]
    TAKES_ROW_AS_INPUT_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    inputs: _containers.RepeatedCompositeFieldContainer[Input]
    specs: _containers.RepeatedCompositeFieldContainer[UserDefinedAggregateFunction.DataViewSpec]
    filter_arg: int
    distinct: bool
    takes_row_as_input: bool
    def __init__(self, payload: _Optional[bytes] = ..., inputs: _Optional[_Iterable[_Union[Input, _Mapping]]] = ..., specs: _Optional[_Iterable[_Union[UserDefinedAggregateFunction.DataViewSpec, _Mapping]]] = ..., filter_arg: _Optional[int] = ..., distinct: bool = ..., takes_row_as_input: bool = ...) -> None: ...

class GroupWindow(_message.Message):
    __slots__ = ("window_type", "is_time_window", "window_slide", "window_size", "window_gap", "is_row_time", "time_field_index", "allowedLateness", "namedProperties", "shift_timezone")
    class WindowType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TUMBLING_GROUP_WINDOW: _ClassVar[GroupWindow.WindowType]
        SLIDING_GROUP_WINDOW: _ClassVar[GroupWindow.WindowType]
        SESSION_GROUP_WINDOW: _ClassVar[GroupWindow.WindowType]
    TUMBLING_GROUP_WINDOW: GroupWindow.WindowType
    SLIDING_GROUP_WINDOW: GroupWindow.WindowType
    SESSION_GROUP_WINDOW: GroupWindow.WindowType
    class WindowProperty(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        WINDOW_START: _ClassVar[GroupWindow.WindowProperty]
        WINDOW_END: _ClassVar[GroupWindow.WindowProperty]
        ROW_TIME_ATTRIBUTE: _ClassVar[GroupWindow.WindowProperty]
        PROC_TIME_ATTRIBUTE: _ClassVar[GroupWindow.WindowProperty]
    WINDOW_START: GroupWindow.WindowProperty
    WINDOW_END: GroupWindow.WindowProperty
    ROW_TIME_ATTRIBUTE: GroupWindow.WindowProperty
    PROC_TIME_ATTRIBUTE: GroupWindow.WindowProperty
    WINDOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    IS_TIME_WINDOW_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SLIDE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SIZE_FIELD_NUMBER: _ClassVar[int]
    WINDOW_GAP_FIELD_NUMBER: _ClassVar[int]
    IS_ROW_TIME_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_INDEX_FIELD_NUMBER: _ClassVar[int]
    ALLOWEDLATENESS_FIELD_NUMBER: _ClassVar[int]
    NAMEDPROPERTIES_FIELD_NUMBER: _ClassVar[int]
    SHIFT_TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    window_type: GroupWindow.WindowType
    is_time_window: bool
    window_slide: int
    window_size: int
    window_gap: int
    is_row_time: bool
    time_field_index: int
    allowedLateness: int
    namedProperties: _containers.RepeatedScalarFieldContainer[GroupWindow.WindowProperty]
    shift_timezone: str
    def __init__(self, window_type: _Optional[_Union[GroupWindow.WindowType, str]] = ..., is_time_window: bool = ..., window_slide: _Optional[int] = ..., window_size: _Optional[int] = ..., window_gap: _Optional[int] = ..., is_row_time: bool = ..., time_field_index: _Optional[int] = ..., allowedLateness: _Optional[int] = ..., namedProperties: _Optional[_Iterable[_Union[GroupWindow.WindowProperty, str]]] = ..., shift_timezone: _Optional[str] = ...) -> None: ...

class UserDefinedAggregateFunctions(_message.Message):
    __slots__ = ("udfs", "metric_enabled", "grouping", "generate_update_before", "key_type", "index_of_count_star", "state_cleaning_enabled", "state_cache_size", "map_state_read_cache_size", "map_state_write_cache_size", "count_star_inserted", "group_window", "profile_enabled", "job_parameters")
    UDFS_FIELD_NUMBER: _ClassVar[int]
    METRIC_ENABLED_FIELD_NUMBER: _ClassVar[int]
    GROUPING_FIELD_NUMBER: _ClassVar[int]
    GENERATE_UPDATE_BEFORE_FIELD_NUMBER: _ClassVar[int]
    KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
    INDEX_OF_COUNT_STAR_FIELD_NUMBER: _ClassVar[int]
    STATE_CLEANING_ENABLED_FIELD_NUMBER: _ClassVar[int]
    STATE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_READ_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_WRITE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    COUNT_STAR_INSERTED_FIELD_NUMBER: _ClassVar[int]
    GROUP_WINDOW_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    JOB_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    udfs: _containers.RepeatedCompositeFieldContainer[UserDefinedAggregateFunction]
    metric_enabled: bool
    grouping: _containers.RepeatedScalarFieldContainer[int]
    generate_update_before: bool
    key_type: Schema.FieldType
    index_of_count_star: int
    state_cleaning_enabled: bool
    state_cache_size: int
    map_state_read_cache_size: int
    map_state_write_cache_size: int
    count_star_inserted: bool
    group_window: GroupWindow
    profile_enabled: bool
    job_parameters: _containers.RepeatedCompositeFieldContainer[JobParameter]
    def __init__(self, udfs: _Optional[_Iterable[_Union[UserDefinedAggregateFunction, _Mapping]]] = ..., metric_enabled: bool = ..., grouping: _Optional[_Iterable[int]] = ..., generate_update_before: bool = ..., key_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., index_of_count_star: _Optional[int] = ..., state_cleaning_enabled: bool = ..., state_cache_size: _Optional[int] = ..., map_state_read_cache_size: _Optional[int] = ..., map_state_write_cache_size: _Optional[int] = ..., count_star_inserted: bool = ..., group_window: _Optional[_Union[GroupWindow, _Mapping]] = ..., profile_enabled: bool = ..., job_parameters: _Optional[_Iterable[_Union[JobParameter, _Mapping]]] = ...) -> None: ...

class Schema(_message.Message):
    __slots__ = ("fields",)
    class TypeName(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ROW: _ClassVar[Schema.TypeName]
        TINYINT: _ClassVar[Schema.TypeName]
        SMALLINT: _ClassVar[Schema.TypeName]
        INT: _ClassVar[Schema.TypeName]
        BIGINT: _ClassVar[Schema.TypeName]
        DECIMAL: _ClassVar[Schema.TypeName]
        FLOAT: _ClassVar[Schema.TypeName]
        DOUBLE: _ClassVar[Schema.TypeName]
        DATE: _ClassVar[Schema.TypeName]
        TIME: _ClassVar[Schema.TypeName]
        TIMESTAMP: _ClassVar[Schema.TypeName]
        BOOLEAN: _ClassVar[Schema.TypeName]
        BINARY: _ClassVar[Schema.TypeName]
        VARBINARY: _ClassVar[Schema.TypeName]
        CHAR: _ClassVar[Schema.TypeName]
        VARCHAR: _ClassVar[Schema.TypeName]
        BASIC_ARRAY: _ClassVar[Schema.TypeName]
        MAP: _ClassVar[Schema.TypeName]
        MULTISET: _ClassVar[Schema.TypeName]
        LOCAL_ZONED_TIMESTAMP: _ClassVar[Schema.TypeName]
        ZONED_TIMESTAMP: _ClassVar[Schema.TypeName]
        NULL: _ClassVar[Schema.TypeName]
    ROW: Schema.TypeName
    TINYINT: Schema.TypeName
    SMALLINT: Schema.TypeName
    INT: Schema.TypeName
    BIGINT: Schema.TypeName
    DECIMAL: Schema.TypeName
    FLOAT: Schema.TypeName
    DOUBLE: Schema.TypeName
    DATE: Schema.TypeName
    TIME: Schema.TypeName
    TIMESTAMP: Schema.TypeName
    BOOLEAN: Schema.TypeName
    BINARY: Schema.TypeName
    VARBINARY: Schema.TypeName
    CHAR: Schema.TypeName
    VARCHAR: Schema.TypeName
    BASIC_ARRAY: Schema.TypeName
    MAP: Schema.TypeName
    MULTISET: Schema.TypeName
    LOCAL_ZONED_TIMESTAMP: Schema.TypeName
    ZONED_TIMESTAMP: Schema.TypeName
    NULL: Schema.TypeName
    class MapInfo(_message.Message):
        __slots__ = ("key_type", "value_type")
        KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
        key_type: Schema.FieldType
        value_type: Schema.FieldType
        def __init__(self, key_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., value_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
    class TimeInfo(_message.Message):
        __slots__ = ("precision",)
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class TimestampInfo(_message.Message):
        __slots__ = ("precision",)
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class LocalZonedTimestampInfo(_message.Message):
        __slots__ = ("precision",)
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class ZonedTimestampInfo(_message.Message):
        __slots__ = ("precision",)
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        precision: int
        def __init__(self, precision: _Optional[int] = ...) -> None: ...
    class DecimalInfo(_message.Message):
        __slots__ = ("precision", "scale")
        PRECISION_FIELD_NUMBER: _ClassVar[int]
        SCALE_FIELD_NUMBER: _ClassVar[int]
        precision: int
        scale: int
        def __init__(self, precision: _Optional[int] = ..., scale: _Optional[int] = ...) -> None: ...
    class BinaryInfo(_message.Message):
        __slots__ = ("length",)
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class VarBinaryInfo(_message.Message):
        __slots__ = ("length",)
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class CharInfo(_message.Message):
        __slots__ = ("length",)
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class VarCharInfo(_message.Message):
        __slots__ = ("length",)
        LENGTH_FIELD_NUMBER: _ClassVar[int]
        length: int
        def __init__(self, length: _Optional[int] = ...) -> None: ...
    class FieldType(_message.Message):
        __slots__ = ("type_name", "nullable", "collection_element_type", "map_info", "row_schema", "decimal_info", "time_info", "timestamp_info", "local_zoned_timestamp_info", "zoned_timestamp_info", "binary_info", "var_binary_info", "char_info", "var_char_info")
        TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
        NULLABLE_FIELD_NUMBER: _ClassVar[int]
        COLLECTION_ELEMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
        MAP_INFO_FIELD_NUMBER: _ClassVar[int]
        ROW_SCHEMA_FIELD_NUMBER: _ClassVar[int]
        DECIMAL_INFO_FIELD_NUMBER: _ClassVar[int]
        TIME_INFO_FIELD_NUMBER: _ClassVar[int]
        TIMESTAMP_INFO_FIELD_NUMBER: _ClassVar[int]
        LOCAL_ZONED_TIMESTAMP_INFO_FIELD_NUMBER: _ClassVar[int]
        ZONED_TIMESTAMP_INFO_FIELD_NUMBER: _ClassVar[int]
        BINARY_INFO_FIELD_NUMBER: _ClassVar[int]
        VAR_BINARY_INFO_FIELD_NUMBER: _ClassVar[int]
        CHAR_INFO_FIELD_NUMBER: _ClassVar[int]
        VAR_CHAR_INFO_FIELD_NUMBER: _ClassVar[int]
        type_name: Schema.TypeName
        nullable: bool
        collection_element_type: Schema.FieldType
        map_info: Schema.MapInfo
        row_schema: Schema
        decimal_info: Schema.DecimalInfo
        time_info: Schema.TimeInfo
        timestamp_info: Schema.TimestampInfo
        local_zoned_timestamp_info: Schema.LocalZonedTimestampInfo
        zoned_timestamp_info: Schema.ZonedTimestampInfo
        binary_info: Schema.BinaryInfo
        var_binary_info: Schema.VarBinaryInfo
        char_info: Schema.CharInfo
        var_char_info: Schema.VarCharInfo
        def __init__(self, type_name: _Optional[_Union[Schema.TypeName, str]] = ..., nullable: bool = ..., collection_element_type: _Optional[_Union[Schema.FieldType, _Mapping]] = ..., map_info: _Optional[_Union[Schema.MapInfo, _Mapping]] = ..., row_schema: _Optional[_Union[Schema, _Mapping]] = ..., decimal_info: _Optional[_Union[Schema.DecimalInfo, _Mapping]] = ..., time_info: _Optional[_Union[Schema.TimeInfo, _Mapping]] = ..., timestamp_info: _Optional[_Union[Schema.TimestampInfo, _Mapping]] = ..., local_zoned_timestamp_info: _Optional[_Union[Schema.LocalZonedTimestampInfo, _Mapping]] = ..., zoned_timestamp_info: _Optional[_Union[Schema.ZonedTimestampInfo, _Mapping]] = ..., binary_info: _Optional[_Union[Schema.BinaryInfo, _Mapping]] = ..., var_binary_info: _Optional[_Union[Schema.VarBinaryInfo, _Mapping]] = ..., char_info: _Optional[_Union[Schema.CharInfo, _Mapping]] = ..., var_char_info: _Optional[_Union[Schema.VarCharInfo, _Mapping]] = ...) -> None: ...
    class Field(_message.Message):
        __slots__ = ("name", "description", "type")
        NAME_FIELD_NUMBER: _ClassVar[int]
        DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        name: str
        description: str
        type: Schema.FieldType
        def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., type: _Optional[_Union[Schema.FieldType, _Mapping]] = ...) -> None: ...
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.RepeatedCompositeFieldContainer[Schema.Field]
    def __init__(self, fields: _Optional[_Iterable[_Union[Schema.Field, _Mapping]]] = ...) -> None: ...

class TypeInfo(_message.Message):
    __slots__ = ("type_name", "collection_element_type", "row_type_info", "tuple_type_info", "map_type_info", "avro_type_info")
    class TypeName(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ROW: _ClassVar[TypeInfo.TypeName]
        STRING: _ClassVar[TypeInfo.TypeName]
        BYTE: _ClassVar[TypeInfo.TypeName]
        BOOLEAN: _ClassVar[TypeInfo.TypeName]
        SHORT: _ClassVar[TypeInfo.TypeName]
        INT: _ClassVar[TypeInfo.TypeName]
        LONG: _ClassVar[TypeInfo.TypeName]
        FLOAT: _ClassVar[TypeInfo.TypeName]
        DOUBLE: _ClassVar[TypeInfo.TypeName]
        CHAR: _ClassVar[TypeInfo.TypeName]
        BIG_INT: _ClassVar[TypeInfo.TypeName]
        BIG_DEC: _ClassVar[TypeInfo.TypeName]
        SQL_DATE: _ClassVar[TypeInfo.TypeName]
        SQL_TIME: _ClassVar[TypeInfo.TypeName]
        SQL_TIMESTAMP: _ClassVar[TypeInfo.TypeName]
        BASIC_ARRAY: _ClassVar[TypeInfo.TypeName]
        PRIMITIVE_ARRAY: _ClassVar[TypeInfo.TypeName]
        TUPLE: _ClassVar[TypeInfo.TypeName]
        LIST: _ClassVar[TypeInfo.TypeName]
        MAP: _ClassVar[TypeInfo.TypeName]
        PICKLED_BYTES: _ClassVar[TypeInfo.TypeName]
        OBJECT_ARRAY: _ClassVar[TypeInfo.TypeName]
        INSTANT: _ClassVar[TypeInfo.TypeName]
        AVRO: _ClassVar[TypeInfo.TypeName]
        LOCAL_DATE: _ClassVar[TypeInfo.TypeName]
        LOCAL_TIME: _ClassVar[TypeInfo.TypeName]
        LOCAL_DATETIME: _ClassVar[TypeInfo.TypeName]
        LOCAL_ZONED_TIMESTAMP: _ClassVar[TypeInfo.TypeName]
    ROW: TypeInfo.TypeName
    STRING: TypeInfo.TypeName
    BYTE: TypeInfo.TypeName
    BOOLEAN: TypeInfo.TypeName
    SHORT: TypeInfo.TypeName
    INT: TypeInfo.TypeName
    LONG: TypeInfo.TypeName
    FLOAT: TypeInfo.TypeName
    DOUBLE: TypeInfo.TypeName
    CHAR: TypeInfo.TypeName
    BIG_INT: TypeInfo.TypeName
    BIG_DEC: TypeInfo.TypeName
    SQL_DATE: TypeInfo.TypeName
    SQL_TIME: TypeInfo.TypeName
    SQL_TIMESTAMP: TypeInfo.TypeName
    BASIC_ARRAY: TypeInfo.TypeName
    PRIMITIVE_ARRAY: TypeInfo.TypeName
    TUPLE: TypeInfo.TypeName
    LIST: TypeInfo.TypeName
    MAP: TypeInfo.TypeName
    PICKLED_BYTES: TypeInfo.TypeName
    OBJECT_ARRAY: TypeInfo.TypeName
    INSTANT: TypeInfo.TypeName
    AVRO: TypeInfo.TypeName
    LOCAL_DATE: TypeInfo.TypeName
    LOCAL_TIME: TypeInfo.TypeName
    LOCAL_DATETIME: TypeInfo.TypeName
    LOCAL_ZONED_TIMESTAMP: TypeInfo.TypeName
    class MapTypeInfo(_message.Message):
        __slots__ = ("key_type", "value_type")
        KEY_TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
        key_type: TypeInfo
        value_type: TypeInfo
        def __init__(self, key_type: _Optional[_Union[TypeInfo, _Mapping]] = ..., value_type: _Optional[_Union[TypeInfo, _Mapping]] = ...) -> None: ...
    class RowTypeInfo(_message.Message):
        __slots__ = ("fields",)
        class Field(_message.Message):
            __slots__ = ("field_name", "field_type")
            FIELD_NAME_FIELD_NUMBER: _ClassVar[int]
            FIELD_TYPE_FIELD_NUMBER: _ClassVar[int]
            field_name: str
            field_type: TypeInfo
            def __init__(self, field_name: _Optional[str] = ..., field_type: _Optional[_Union[TypeInfo, _Mapping]] = ...) -> None: ...
        FIELDS_FIELD_NUMBER: _ClassVar[int]
        fields: _containers.RepeatedCompositeFieldContainer[TypeInfo.RowTypeInfo.Field]
        def __init__(self, fields: _Optional[_Iterable[_Union[TypeInfo.RowTypeInfo.Field, _Mapping]]] = ...) -> None: ...
    class TupleTypeInfo(_message.Message):
        __slots__ = ("field_types",)
        FIELD_TYPES_FIELD_NUMBER: _ClassVar[int]
        field_types: _containers.RepeatedCompositeFieldContainer[TypeInfo]
        def __init__(self, field_types: _Optional[_Iterable[_Union[TypeInfo, _Mapping]]] = ...) -> None: ...
    class AvroTypeInfo(_message.Message):
        __slots__ = ("schema",)
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: str
        def __init__(self, schema: _Optional[str] = ...) -> None: ...
    TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ELEMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROW_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    TUPLE_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    MAP_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    AVRO_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    type_name: TypeInfo.TypeName
    collection_element_type: TypeInfo
    row_type_info: TypeInfo.RowTypeInfo
    tuple_type_info: TypeInfo.TupleTypeInfo
    map_type_info: TypeInfo.MapTypeInfo
    avro_type_info: TypeInfo.AvroTypeInfo
    def __init__(self, type_name: _Optional[_Union[TypeInfo.TypeName, str]] = ..., collection_element_type: _Optional[_Union[TypeInfo, _Mapping]] = ..., row_type_info: _Optional[_Union[TypeInfo.RowTypeInfo, _Mapping]] = ..., tuple_type_info: _Optional[_Union[TypeInfo.TupleTypeInfo, _Mapping]] = ..., map_type_info: _Optional[_Union[TypeInfo.MapTypeInfo, _Mapping]] = ..., avro_type_info: _Optional[_Union[TypeInfo.AvroTypeInfo, _Mapping]] = ...) -> None: ...

class UserDefinedDataStreamFunction(_message.Message):
    __slots__ = ("function_type", "runtime_context", "payload", "metric_enabled", "key_type_info", "profile_enabled", "has_side_output", "state_cache_size", "map_state_read_cache_size", "map_state_write_cache_size")
    class FunctionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PROCESS: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        CO_PROCESS: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        KEYED_PROCESS: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        KEYED_CO_PROCESS: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        WINDOW: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        CO_BROADCAST_PROCESS: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        KEYED_CO_BROADCAST_PROCESS: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
        REVISE_OUTPUT: _ClassVar[UserDefinedDataStreamFunction.FunctionType]
    PROCESS: UserDefinedDataStreamFunction.FunctionType
    CO_PROCESS: UserDefinedDataStreamFunction.FunctionType
    KEYED_PROCESS: UserDefinedDataStreamFunction.FunctionType
    KEYED_CO_PROCESS: UserDefinedDataStreamFunction.FunctionType
    WINDOW: UserDefinedDataStreamFunction.FunctionType
    CO_BROADCAST_PROCESS: UserDefinedDataStreamFunction.FunctionType
    KEYED_CO_BROADCAST_PROCESS: UserDefinedDataStreamFunction.FunctionType
    REVISE_OUTPUT: UserDefinedDataStreamFunction.FunctionType
    class RuntimeContext(_message.Message):
        __slots__ = ("task_name", "task_name_with_subtasks", "number_of_parallel_subtasks", "max_number_of_parallel_subtasks", "index_of_this_subtask", "attempt_number", "job_parameters", "in_batch_execution_mode")
        TASK_NAME_FIELD_NUMBER: _ClassVar[int]
        TASK_NAME_WITH_SUBTASKS_FIELD_NUMBER: _ClassVar[int]
        NUMBER_OF_PARALLEL_SUBTASKS_FIELD_NUMBER: _ClassVar[int]
        MAX_NUMBER_OF_PARALLEL_SUBTASKS_FIELD_NUMBER: _ClassVar[int]
        INDEX_OF_THIS_SUBTASK_FIELD_NUMBER: _ClassVar[int]
        ATTEMPT_NUMBER_FIELD_NUMBER: _ClassVar[int]
        JOB_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
        IN_BATCH_EXECUTION_MODE_FIELD_NUMBER: _ClassVar[int]
        task_name: str
        task_name_with_subtasks: str
        number_of_parallel_subtasks: int
        max_number_of_parallel_subtasks: int
        index_of_this_subtask: int
        attempt_number: int
        job_parameters: _containers.RepeatedCompositeFieldContainer[JobParameter]
        in_batch_execution_mode: bool
        def __init__(self, task_name: _Optional[str] = ..., task_name_with_subtasks: _Optional[str] = ..., number_of_parallel_subtasks: _Optional[int] = ..., max_number_of_parallel_subtasks: _Optional[int] = ..., index_of_this_subtask: _Optional[int] = ..., attempt_number: _Optional[int] = ..., job_parameters: _Optional[_Iterable[_Union[JobParameter, _Mapping]]] = ..., in_batch_execution_mode: bool = ...) -> None: ...
    FUNCTION_TYPE_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    METRIC_ENABLED_FIELD_NUMBER: _ClassVar[int]
    KEY_TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ENABLED_FIELD_NUMBER: _ClassVar[int]
    HAS_SIDE_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    STATE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_READ_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    MAP_STATE_WRITE_CACHE_SIZE_FIELD_NUMBER: _ClassVar[int]
    function_type: UserDefinedDataStreamFunction.FunctionType
    runtime_context: UserDefinedDataStreamFunction.RuntimeContext
    payload: bytes
    metric_enabled: bool
    key_type_info: TypeInfo
    profile_enabled: bool
    has_side_output: bool
    state_cache_size: int
    map_state_read_cache_size: int
    map_state_write_cache_size: int
    def __init__(self, function_type: _Optional[_Union[UserDefinedDataStreamFunction.FunctionType, str]] = ..., runtime_context: _Optional[_Union[UserDefinedDataStreamFunction.RuntimeContext, _Mapping]] = ..., payload: _Optional[bytes] = ..., metric_enabled: bool = ..., key_type_info: _Optional[_Union[TypeInfo, _Mapping]] = ..., profile_enabled: bool = ..., has_side_output: bool = ..., state_cache_size: _Optional[int] = ..., map_state_read_cache_size: _Optional[int] = ..., map_state_write_cache_size: _Optional[int] = ...) -> None: ...

class StateDescriptor(_message.Message):
    __slots__ = ("state_name", "state_ttl_config")
    class StateTTLConfig(_message.Message):
        __slots__ = ("update_type", "state_visibility", "ttl_time_characteristic", "ttl", "cleanup_strategies")
        class UpdateType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            Disabled: _ClassVar[StateDescriptor.StateTTLConfig.UpdateType]
            OnCreateAndWrite: _ClassVar[StateDescriptor.StateTTLConfig.UpdateType]
            OnReadAndWrite: _ClassVar[StateDescriptor.StateTTLConfig.UpdateType]
        Disabled: StateDescriptor.StateTTLConfig.UpdateType
        OnCreateAndWrite: StateDescriptor.StateTTLConfig.UpdateType
        OnReadAndWrite: StateDescriptor.StateTTLConfig.UpdateType
        class StateVisibility(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            ReturnExpiredIfNotCleanedUp: _ClassVar[StateDescriptor.StateTTLConfig.StateVisibility]
            NeverReturnExpired: _ClassVar[StateDescriptor.StateTTLConfig.StateVisibility]
        ReturnExpiredIfNotCleanedUp: StateDescriptor.StateTTLConfig.StateVisibility
        NeverReturnExpired: StateDescriptor.StateTTLConfig.StateVisibility
        class TtlTimeCharacteristic(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
            __slots__ = ()
            ProcessingTime: _ClassVar[StateDescriptor.StateTTLConfig.TtlTimeCharacteristic]
        ProcessingTime: StateDescriptor.StateTTLConfig.TtlTimeCharacteristic
        class CleanupStrategies(_message.Message):
            __slots__ = ("is_cleanup_in_background", "strategies")
            class Strategies(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = ()
                FULL_STATE_SCAN_SNAPSHOT: _ClassVar[StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies]
                INCREMENTAL_CLEANUP: _ClassVar[StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies]
                ROCKSDB_COMPACTION_FILTER: _ClassVar[StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies]
            FULL_STATE_SCAN_SNAPSHOT: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
            INCREMENTAL_CLEANUP: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
            ROCKSDB_COMPACTION_FILTER: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
            class EmptyCleanupStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
                __slots__ = ()
                EMPTY_STRATEGY: _ClassVar[StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy]
            EMPTY_STRATEGY: StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy
            class IncrementalCleanupStrategy(_message.Message):
                __slots__ = ("cleanup_size", "run_cleanup_for_every_record")
                CLEANUP_SIZE_FIELD_NUMBER: _ClassVar[int]
                RUN_CLEANUP_FOR_EVERY_RECORD_FIELD_NUMBER: _ClassVar[int]
                cleanup_size: int
                run_cleanup_for_every_record: bool
                def __init__(self, cleanup_size: _Optional[int] = ..., run_cleanup_for_every_record: bool = ...) -> None: ...
            class RocksdbCompactFilterCleanupStrategy(_message.Message):
                __slots__ = ("query_time_after_num_entries",)
                QUERY_TIME_AFTER_NUM_ENTRIES_FIELD_NUMBER: _ClassVar[int]
                query_time_after_num_entries: int
                def __init__(self, query_time_after_num_entries: _Optional[int] = ...) -> None: ...
            class MapStrategiesEntry(_message.Message):
                __slots__ = ("strategy", "empty_strategy", "incremental_cleanup_strategy", "rocksdb_compact_filter_cleanup_strategy")
                STRATEGY_FIELD_NUMBER: _ClassVar[int]
                EMPTY_STRATEGY_FIELD_NUMBER: _ClassVar[int]
                INCREMENTAL_CLEANUP_STRATEGY_FIELD_NUMBER: _ClassVar[int]
                ROCKSDB_COMPACT_FILTER_CLEANUP_STRATEGY_FIELD_NUMBER: _ClassVar[int]
                strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies
                empty_strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy
                incremental_cleanup_strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.IncrementalCleanupStrategy
                rocksdb_compact_filter_cleanup_strategy: StateDescriptor.StateTTLConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy
                def __init__(self, strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies, str]] = ..., empty_strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.EmptyCleanupStrategy, str]] = ..., incremental_cleanup_strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.IncrementalCleanupStrategy, _Mapping]] = ..., rocksdb_compact_filter_cleanup_strategy: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy, _Mapping]] = ...) -> None: ...
            IS_CLEANUP_IN_BACKGROUND_FIELD_NUMBER: _ClassVar[int]
            STRATEGIES_FIELD_NUMBER: _ClassVar[int]
            is_cleanup_in_background: bool
            strategies: _containers.RepeatedCompositeFieldContainer[StateDescriptor.StateTTLConfig.CleanupStrategies.MapStrategiesEntry]
            def __init__(self, is_cleanup_in_background: bool = ..., strategies: _Optional[_Iterable[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies.MapStrategiesEntry, _Mapping]]] = ...) -> None: ...
        UPDATE_TYPE_FIELD_NUMBER: _ClassVar[int]
        STATE_VISIBILITY_FIELD_NUMBER: _ClassVar[int]
        TTL_TIME_CHARACTERISTIC_FIELD_NUMBER: _ClassVar[int]
        TTL_FIELD_NUMBER: _ClassVar[int]
        CLEANUP_STRATEGIES_FIELD_NUMBER: _ClassVar[int]
        update_type: StateDescriptor.StateTTLConfig.UpdateType
        state_visibility: StateDescriptor.StateTTLConfig.StateVisibility
        ttl_time_characteristic: StateDescriptor.StateTTLConfig.TtlTimeCharacteristic
        ttl: int
        cleanup_strategies: StateDescriptor.StateTTLConfig.CleanupStrategies
        def __init__(self, update_type: _Optional[_Union[StateDescriptor.StateTTLConfig.UpdateType, str]] = ..., state_visibility: _Optional[_Union[StateDescriptor.StateTTLConfig.StateVisibility, str]] = ..., ttl_time_characteristic: _Optional[_Union[StateDescriptor.StateTTLConfig.TtlTimeCharacteristic, str]] = ..., ttl: _Optional[int] = ..., cleanup_strategies: _Optional[_Union[StateDescriptor.StateTTLConfig.CleanupStrategies, _Mapping]] = ...) -> None: ...
    STATE_NAME_FIELD_NUMBER: _ClassVar[int]
    STATE_TTL_CONFIG_FIELD_NUMBER: _ClassVar[int]
    state_name: str
    state_ttl_config: StateDescriptor.StateTTLConfig
    def __init__(self, state_name: _Optional[str] = ..., state_ttl_config: _Optional[_Union[StateDescriptor.StateTTLConfig, _Mapping]] = ...) -> None: ...

class CoderInfoDescriptor(_message.Message):
    __slots__ = ("flatten_row_type", "row_type", "arrow_type", "over_window_arrow_type", "raw_type", "mode", "separated_with_end_message")
    class Mode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SINGLE: _ClassVar[CoderInfoDescriptor.Mode]
        MULTIPLE: _ClassVar[CoderInfoDescriptor.Mode]
    SINGLE: CoderInfoDescriptor.Mode
    MULTIPLE: CoderInfoDescriptor.Mode
    class FlattenRowType(_message.Message):
        __slots__ = ("schema",)
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class RowType(_message.Message):
        __slots__ = ("schema",)
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class ArrowType(_message.Message):
        __slots__ = ("schema",)
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class OverWindowArrowType(_message.Message):
        __slots__ = ("schema",)
        SCHEMA_FIELD_NUMBER: _ClassVar[int]
        schema: Schema
        def __init__(self, schema: _Optional[_Union[Schema, _Mapping]] = ...) -> None: ...
    class RawType(_message.Message):
        __slots__ = ("type_info",)
        TYPE_INFO_FIELD_NUMBER: _ClassVar[int]
        type_info: TypeInfo
        def __init__(self, type_info: _Optional[_Union[TypeInfo, _Mapping]] = ...) -> None: ...
    FLATTEN_ROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    OVER_WINDOW_ARROW_TYPE_FIELD_NUMBER: _ClassVar[int]
    RAW_TYPE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    SEPARATED_WITH_END_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    flatten_row_type: CoderInfoDescriptor.FlattenRowType
    row_type: CoderInfoDescriptor.RowType
    arrow_type: CoderInfoDescriptor.ArrowType
    over_window_arrow_type: CoderInfoDescriptor.OverWindowArrowType
    raw_type: CoderInfoDescriptor.RawType
    mode: CoderInfoDescriptor.Mode
    separated_with_end_message: bool
    def __init__(self, flatten_row_type: _Optional[_Union[CoderInfoDescriptor.FlattenRowType, _Mapping]] = ..., row_type: _Optional[_Union[CoderInfoDescriptor.RowType, _Mapping]] = ..., arrow_type: _Optional[_Union[CoderInfoDescriptor.ArrowType, _Mapping]] = ..., over_window_arrow_type: _Optional[_Union[CoderInfoDescriptor.OverWindowArrowType, _Mapping]] = ..., raw_type: _Optional[_Union[CoderInfoDescriptor.RawType, _Mapping]] = ..., mode: _Optional[_Union[CoderInfoDescriptor.Mode, str]] = ..., separated_with_end_message: bool = ...) -> None: ...
