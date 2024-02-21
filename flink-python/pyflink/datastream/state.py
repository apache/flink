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
from abc import ABC, abstractmethod
from enum import Enum
from typing import TypeVar, Generic, Iterable, List, Iterator, Dict, Tuple, Optional

from pyflink.common.time import Time
from pyflink.common.typeinfo import TypeInformation, Types

__all__ = [
    'ValueStateDescriptor',
    'ValueState',
    'ListStateDescriptor',
    'ListState',
    'MapStateDescriptor',
    'MapState',
    'ReducingStateDescriptor',
    'ReducingState',
    'AggregatingStateDescriptor',
    'AggregatingState',
    'ReadOnlyBroadcastState',
    'BroadcastState',
    'StateTtlConfig',
    'OperatorStateStore',
]

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
IN = TypeVar('IN')
OUT = TypeVar('OUT')


class OperatorStateStore(ABC):
    """
    Interface for getting operator states. Currently, only :class:`~state.BroadcastState` is
    supported.
    .. versionadded:: 1.16.0
    """

    @abstractmethod
    def get_broadcast_state(self, state_descriptor: 'MapStateDescriptor') -> 'BroadcastState':
        """
        Fetches the :class:`~state.BroadcastState` described by :class:`~state.MapStateDescriptor`,
        which has read/write access to the broadcast operator state.
        """
        pass


class State(ABC):
    """
    Interface that different types of partitioned state must implement.
    """

    @abstractmethod
    def clear(self) -> None:
        """
        Removes the value mapped under the current key.
        """
        pass


class ValueState(State, Generic[T]):
    """
    :class:`State` interface for partitioned single-value state. The value can be retrieved or
    updated.

    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.
    """

    @abstractmethod
    def value(self) -> T:
        """
        Returns the current value for the state. When the state is not partitioned the returned
        value is the same for all inputs in a given operator instance. If state partitioning is
        applied, the value returned depends on the current operator input, as the operator
        maintains an independent state for each partition.
        """
        pass

    @abstractmethod
    def update(self, value: T) -> None:
        """
        Updates the operator state accessible by :func:`value` to the given value. The next time
        :func:`value` is called (for the same state partition) the returned state will represent
        the updated value. When a partitioned state is updated with null, the state for the current
        key will be removed and the default value is returned on the next access.
        """
        pass


class AppendingState(State, Generic[IN, OUT]):
    """
    Base interface for partitioned state that supports adding elements and inspecting the current
    state. Elements can either be kept in a buffer (list-like) or aggregated into one value.

    This state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state is only accessible by functions applied on a KeyedStream. The key is automatically
    supplied by the system, so the function always sees the value mapped to the key of the current
    element. That way, the system can handle stream and state partitioning consistently together.
    """

    @abstractmethod
    def get(self) -> OUT:
        """
        Returns the elements under the current key.
        """
        pass

    @abstractmethod
    def add(self, value: IN) -> None:
        """
        Adding the given value to the tail of this list state.
        """
        pass


class MergingState(AppendingState[IN, OUT]):
    """
    Extension of AppendingState that allows merging of state. That is, two instance of MergingState
    can be combined into a single instance that contains all the information of the two merged
    states.
    """
    pass


class ReducingState(MergingState[T, T]):
    """
    :class:`State` interface for reducing state. Elements can be added to the state, they will be
    combined using a reduce function. The current state can be inspected.

    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state is only accessible by functions applied on a KeyedStream. The key is automatically
    supplied by the system, so the function always sees the value mapped to the key of the current
    element. That way, the system can handle stream and state partitioning consistently together.
    """
    pass


class AggregatingState(MergingState[IN, OUT]):
    """
    :class:`State` interface for aggregating state, based on an
    :class:`~pyflink.datastream.functions.AggregateFunction`. Elements that are added to this type
    of state will be eagerly pre-aggregated using a given AggregateFunction.

    The state holds internally always the accumulator type of the AggregateFunction. When
    accessing the result of the state, the function's
    :func:`~pyflink.datastream.functions.AggregateFunction.get_result` method.

    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state is only accessible by functions applied on a KeyedStream. The key is automatically
    supplied by the system, so the function always sees the value mapped to the key of the current
    element. That way, the system can handle stream and state partitioning consistently together.
    """
    pass


class ListState(MergingState[T, Iterable[T]]):
    """
    :class:`State` interface for partitioned list state in Operations.
    The state is accessed and modified by user functions, and checkpointed consistently
    by the system as part of the distributed snapshots.

    Currently only keyed list state is supported.

    When it is a keyed list state, the state key is automatically supplied by the system, so the
    user function always sees the value mapped to the key of the current element. That way, the
    system can handle stream and state partitioning consistently together.
    """

    @abstractmethod
    def update(self, values: List[T]) -> None:
        """
        Updating existing values to the given list of values.
        """
        pass

    @abstractmethod
    def add_all(self, values: List[T]) -> None:
        """
        Adding the given values to the tail of this list state.
        """
        pass

    def __iter__(self) -> Iterator[T]:
        return iter(self.get())


class MapState(State, Generic[K, V]):
    """
    :class:`State` interface for partitioned key-value state. The key-value pair can be added,
    updated and retrieved.
    The state is accessed and modified by user functions, and checkpointed consistently by the
    system as part of the distributed snapshots.

    The state key is automatically supplied by the system, so the function always sees the value
    mapped to the key of the current element. That way, the system can handle stream and state
    partitioning consistently together.
    """

    @abstractmethod
    def get(self, key: K) -> V:
        """
        Returns the current value associated with the given key.
        """
        pass

    @abstractmethod
    def put(self, key: K, value: V) -> None:
        """
        Associates a new value with the given key.
        """
        pass

    @abstractmethod
    def put_all(self, dict_value: Dict[K, V]) -> None:
        """
        Copies all of the mappings from the given map into the state.
        """
        pass

    @abstractmethod
    def remove(self, key: K) -> None:
        """
        Deletes the mapping of the given key.
        """
        pass

    @abstractmethod
    def contains(self, key: K) -> bool:
        """
        Returns whether there exists the given mapping.
        """
        pass

    @abstractmethod
    def items(self) -> Iterable[Tuple[K, V]]:
        """
        Returns all the mappings in the state.
        """
        pass

    @abstractmethod
    def keys(self) -> Iterable[K]:
        """
        Returns all the keys in the state.
        """
        pass

    @abstractmethod
    def values(self) -> Iterable[V]:
        """
        Returns all the values in the state.
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Returns true if this state contains no key-value mappings, otherwise false.
        """
        pass

    def __getitem__(self, key: K) -> V:
        return self.get(key)

    def __setitem__(self, key: K, value: V) -> None:
        self.put(key, value)

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __contains__(self, key: K) -> bool:
        return self.contains(key)

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())


class ReadOnlyBroadcastState(State, Generic[K, V]):
    """
    A read-only view of the :class:`BroadcastState`.
    Although read-only, the user code should not modify the value returned by the :meth:`get` or the
    items returned by :meth:`items`, as this can lead to inconsistent states. The reason for this is
    that we do not create extra copies of the elements for performance reasons.
    """

    @abstractmethod
    def get(self, key: K) -> V:
        """
        Returns the current value associated with the given key.
        """
        pass

    @abstractmethod
    def contains(self, key: K) -> bool:
        """
        Returns whether there exists the given mapping.
        """
        pass

    @abstractmethod
    def items(self) -> Iterable[Tuple[K, V]]:
        """
        Returns all the mappings in the state.
        """
        pass

    @abstractmethod
    def keys(self) -> Iterable[K]:
        """
        Returns all the keys in the state.
        """
        pass

    @abstractmethod
    def values(self) -> Iterable[V]:
        """
        Returns all the values in the state.
        """
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Returns true if this state contains no key-value mappings, otherwise false.
        """
        pass

    def __getitem__(self, key: K) -> V:
        return self.get(key)

    def __contains__(self, key: K) -> bool:
        return self.contains(key)

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())


class BroadcastState(ReadOnlyBroadcastState[K, V]):
    """
    A type of state that can be created to store the state of a :class:`BroadcastStream`. This state
    assumes that the same elements are sent to all instances of an operator.
    CAUTION: the user has to guarantee that all task instances store the same elements in
    this type of state.
    Each operator instance individually maintains and stores elements in the broadcast state. The
    fact that the incoming stream is a broadcast one guarantees that all instances see all the
    elements. Upon recovery or re-scaling, the same state is given to each of the instances.
    To avoid hotspots, each task reads its previous partition, and if there are more tasks (scale up
    ), then the new instances read from the old instances in a round-robin fashion. This is why each
    instance has to guarantee that it stores the same elements as the rest. If not, upon recovery or
    rescaling you may have unpredictable redistribution of the partitions, thus unpredictable
    results.
    """

    @abstractmethod
    def put(self, key: K, value: V) -> None:
        """
        Associates a new value with the given key.
        """
        pass

    @abstractmethod
    def put_all(self, dict_value: Dict[K, V]) -> None:
        """
        Copies all of the mappings from the given map into the state.
        """
        pass

    @abstractmethod
    def remove(self, key: K) -> None:
        """
        Deletes the mapping of the given key.
        """
        pass

    def __setitem__(self, key: K, value: V) -> None:
        self.put(key, value)

    def __delitem__(self, key: K) -> None:
        self.remove(key)


class StateDescriptor(ABC):
    """
    Base class for state descriptors. A StateDescriptor is used for creating partitioned State in
    stateful operations.
    """

    def __init__(self, name: str, type_info: TypeInformation):
        """
        Constructor for StateDescriptor.

        :param name: The name of the state
        :param type_info: The type information of the value.
        """
        self.name = name
        self.type_info = type_info
        self._ttl_config = None  # type: Optional[StateTtlConfig]

    def get_name(self) -> str:
        """
        Get the name of the state.

        :return: The name of the state.
        """
        return self.name

    def enable_time_to_live(self, ttl_config: 'StateTtlConfig'):
        """
        Configures optional activation of state time-to-live (TTL).

        State user value will expire, become unavailable and be cleaned up in storage depending on
        configured StateTtlConfig.

        :param ttl_config: Configuration of state TTL
        """
        self._ttl_config = ttl_config


class ValueStateDescriptor(StateDescriptor):
    """
    StateDescriptor for ValueState. This can be used to create partitioned value state using
    RuntimeContext.get_state(ValueStateDescriptor).
    """

    def __init__(self, name: str, value_type_info: TypeInformation):
        """
        Constructor of the ValueStateDescriptor.

        :param name: The name of the state.
        :param value_type_info: the type information of the state.
        """
        super(ValueStateDescriptor, self).__init__(name, value_type_info)


class ListStateDescriptor(StateDescriptor):
    """
    StateDescriptor for ListState. This can be used to create state where the type is a list that
    can be appended and iterated over.
    """

    def __init__(self, name: str, elem_type_info: TypeInformation):
        """
        Constructor of the ListStateDescriptor.

        :param name: The name of the state.
        :param elem_type_info: the type information of the state element.
        """
        super(ListStateDescriptor, self).__init__(name, Types.LIST(elem_type_info))


class MapStateDescriptor(StateDescriptor):
    """
    StateDescriptor for MapState. This can be used to create state where the type is a map that can
    be updated and iterated over.
    """

    def __init__(self, name: str, key_type_info: TypeInformation, value_type_info: TypeInformation):
        """
        Constructor of the MapStateDescriptor.

        :param name: The name of the state.
        :param key_type_info: The type information of the key.
        :param value_type_info: the type information of the value.
        """
        super(MapStateDescriptor, self).__init__(name, Types.MAP(key_type_info, value_type_info))


class ReducingStateDescriptor(StateDescriptor):
    """
    StateDescriptor for ReducingState. This can be used to create partitioned reducing state using
    RuntimeContext.get_reducing_state(ReducingStateDescriptor).
    """

    def __init__(self,
                 name: str,
                 reduce_function,
                 type_info: TypeInformation):
        """
        Constructor of the ReducingStateDescriptor.

        :param name: The name of the state.
        :param reduce_function: The ReduceFunction used to aggregate the state.
        :param type_info: The type of the values in the state.
        """
        super(ReducingStateDescriptor, self).__init__(name, type_info)
        from pyflink.datastream.functions import ReduceFunction, ReduceFunctionWrapper
        if not isinstance(reduce_function, ReduceFunction):
            if callable(reduce_function):
                reduce_function = ReduceFunctionWrapper(reduce_function)  # type: ignore
            else:
                raise TypeError("The input must be a ReduceFunction or a callable function!")
        self._reduce_function = reduce_function

    def get_reduce_function(self):
        return self._reduce_function


class AggregatingStateDescriptor(StateDescriptor):
    """
    A StateDescriptor for AggregatingState.

    The type internally stored in the state is the type of the Accumulator of the
    :func:`~pyflink.datastream.functions.AggregateFunction`.
    """

    def __init__(self,
                 name: str,
                 agg_function,
                 state_type_info):
        super(AggregatingStateDescriptor, self).__init__(name, state_type_info)
        from pyflink.datastream.functions import AggregateFunction
        if not isinstance(agg_function, AggregateFunction):
            raise TypeError("The input must be a pyflink.datastream.functions.AggregateFunction!")
        self._agg_function = agg_function

    def get_agg_function(self):
        return self._agg_function


class StateTtlConfig(object):
    class UpdateType(Enum):
        """
        This option value configures when to update last access timestamp which prolongs state TTL.
        """

        Disabled = 0
        """
        TTL is disabled. State does not expire.
        """

        OnCreateAndWrite = 1
        """
        Last access timestamp is initialised when state is created and updated on every write
        operation.
        """

        OnReadAndWrite = 2
        """
        The same as OnCreateAndWrite but also updated on read.
        """

        def _to_proto(self):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            return getattr(StateDescriptor.StateTTLConfig.UpdateType, self.name)

        @staticmethod
        def _from_proto(proto):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            update_type_name = StateDescriptor.StateTTLConfig.UpdateType.Name(proto)
            return StateTtlConfig.UpdateType[update_type_name]

    class StateVisibility(Enum):
        """
        This option configures whether expired user value can be returned or not.
        """

        ReturnExpiredIfNotCleanedUp = 0
        """
        Return expired user value if it is not cleaned up yet.
        """

        NeverReturnExpired = 1
        """
        Never return expired user value.
        """

        def _to_proto(self):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            return getattr(StateDescriptor.StateTTLConfig.StateVisibility, self.name)

        @staticmethod
        def _from_proto(proto):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            state_visibility_name = StateDescriptor.StateTTLConfig.StateVisibility.Name(proto)
            return StateTtlConfig.StateVisibility[state_visibility_name]

    class TtlTimeCharacteristic(Enum):
        """
        This option configures time scale to use for ttl.
        """

        ProcessingTime = 0
        """
        Processing time
        """

        def _to_proto(self):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            return getattr(StateDescriptor.StateTTLConfig.TtlTimeCharacteristic, self.name)

        @staticmethod
        def _from_proto(proto):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            ttl_time_characteristic_name = \
                StateDescriptor.StateTTLConfig.TtlTimeCharacteristic.Name(proto)
            return StateTtlConfig.TtlTimeCharacteristic[ttl_time_characteristic_name]

    def __init__(self,
                 update_type: UpdateType,
                 state_visibility: StateVisibility,
                 ttl_time_characteristic: TtlTimeCharacteristic,
                 ttl: Time,
                 cleanup_strategies: 'StateTtlConfig.CleanupStrategies'):
        self._update_type = update_type
        self._state_visibility = state_visibility
        self._ttl_time_characteristic = ttl_time_characteristic
        self._ttl = ttl
        self._cleanup_strategies = cleanup_strategies

    @staticmethod
    def new_builder(ttl: Time):
        return StateTtlConfig.Builder(ttl)

    def get_update_type(self) -> 'StateTtlConfig.UpdateType':
        return self._update_type

    def get_state_visibility(self) -> 'StateTtlConfig.StateVisibility':
        return self._state_visibility

    def get_ttl(self) -> Time:
        return self._ttl

    def get_ttl_time_characteristic(self) -> 'StateTtlConfig.TtlTimeCharacteristic':
        return self._ttl_time_characteristic

    def is_enabled(self) -> bool:
        return self._update_type.value != StateTtlConfig.UpdateType.Disabled.value

    def get_cleanup_strategies(self) -> 'StateTtlConfig.CleanupStrategies':
        return self._cleanup_strategies

    def _to_proto(self):
        from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
        state_ttl_config = StateDescriptor.StateTTLConfig()
        state_ttl_config.update_type = self._update_type._to_proto()
        state_ttl_config.state_visibility = self._state_visibility._to_proto()
        state_ttl_config.ttl_time_characteristic = self._ttl_time_characteristic._to_proto()
        state_ttl_config.ttl = self._ttl.to_milliseconds()
        state_ttl_config.cleanup_strategies.CopyFrom(self._cleanup_strategies._to_proto())
        return state_ttl_config

    @staticmethod
    def _from_proto(proto):
        update_type = StateTtlConfig.UpdateType._from_proto(proto.update_type)
        state_visibility = StateTtlConfig.StateVisibility._from_proto(proto.state_visibility)
        ttl_time_characteristic = \
            StateTtlConfig.TtlTimeCharacteristic._from_proto(proto.ttl_time_characteristic)
        ttl = Time.milliseconds(proto.ttl)
        cleanup_strategies = StateTtlConfig.CleanupStrategies._from_proto(proto.cleanup_strategies)
        builder = StateTtlConfig.new_builder(ttl) \
            .set_update_type(update_type) \
            .set_state_visibility(state_visibility) \
            .set_ttl_time_characteristic(ttl_time_characteristic)
        builder._strategies = cleanup_strategies._strategies
        builder._is_cleanup_in_background = cleanup_strategies._is_cleanup_in_background
        return builder.build()

    def __repr__(self):
        return "StateTtlConfig<" \
               "update_type={}," \
               " state_visibility={}," \
               "ttl_time_characteristic ={}," \
               "ttl={}>".format(self._update_type,
                                self._state_visibility,
                                self._ttl_time_characteristic,
                                self._ttl)

    class Builder(object):
        """
        Builder for the StateTtlConfig.
        """

        def __init__(self, ttl: Time):
            self._ttl = ttl
            self._update_type = StateTtlConfig.UpdateType.OnCreateAndWrite
            self._state_visibility = StateTtlConfig.StateVisibility.NeverReturnExpired
            self._ttl_time_characteristic = StateTtlConfig.TtlTimeCharacteristic.ProcessingTime
            self._is_cleanup_in_background = True
            self._strategies = {}  # type: Dict

        def set_update_type(self,
                            update_type: 'StateTtlConfig.UpdateType') -> 'StateTtlConfig.Builder':
            """
            Sets the ttl update type.

            :param update_type: The ttl update type configures when to update last access timestamp
                which prolongs state TTL.
            """
            self._update_type = update_type
            return self

        def update_ttl_on_create_and_write(self) -> 'StateTtlConfig.Builder':
            return self.set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)

        def update_ttl_on_read_and_write(self) -> 'StateTtlConfig.Builder':
            return self.set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite)

        def set_state_visibility(
                self,
                state_visibility: 'StateTtlConfig.StateVisibility') -> 'StateTtlConfig.Builder':
            """
            Sets the state visibility.

            :param state_visibility: The state visibility configures whether expired user value can
                be returned or not.
            """

            self._state_visibility = state_visibility
            return self

        def return_expired_if_not_cleaned_up(self) -> 'StateTtlConfig.Builder':
            return self.set_state_visibility(
                StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)

        def never_return_expired(self) -> 'StateTtlConfig.Builder':
            return self.set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)

        def set_ttl_time_characteristic(
                self,
                ttl_time_characteristic: 'StateTtlConfig.TtlTimeCharacteristic') \
                -> 'StateTtlConfig.Builder':
            """
            Sets the time characteristic.

            :param ttl_time_characteristic: The time characteristic configures time scale to use for
                ttl.
            """
            self._ttl_time_characteristic = ttl_time_characteristic
            return self

        def use_processing_time(self) -> 'StateTtlConfig.Builder':
            return self.set_ttl_time_characteristic(
                StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)

        def cleanup_full_snapshot(self) -> 'StateTtlConfig.Builder':
            """
            Cleanup expired state in full snapshot on checkpoint.
            """
            self._strategies[
                StateTtlConfig.CleanupStrategies.Strategies.FULL_STATE_SCAN_SNAPSHOT] = \
                StateTtlConfig.CleanupStrategies.EMPTY_STRATEGY
            return self

        def cleanup_incrementally(self,
                                  cleanup_size: int,
                                  run_cleanup_for_every_record) -> 'StateTtlConfig.Builder':
            """
            Cleanup expired state incrementally cleanup local state.

            Upon every state access this cleanup strategy checks a bunch of state keys for
            expiration and cleans up expired ones. It keeps a lazy iterator through all keys with
            relaxed consistency if backend supports it. This way all keys should be regularly
            checked and cleaned eventually over time if any state is constantly being accessed.

            Additionally to the incremental cleanup upon state access, it can also run per every
            record. Caution: if there are a lot of registered states using this option, they all
            will be iterated for every record to check if there is something to cleanup.

            if no access happens to this state or no records are processed in case of
            run_cleanup_for_every_record, expired state will persist.

            Time spent for the incremental cleanup increases record processing latency.

            Note:

            At the moment incremental cleanup is implemented only for Heap state backend.
            Setting it for RocksDB will have no effect.

            Note:

            If heap state backend is used with synchronous snapshotting, the global iterator keeps a
            copy of all keys while iterating because of its specific implementation which does not
            support concurrent modifications. Enabling of this feature will increase memory
            consumption then. Asynchronous snapshotting does not have this problem.

            :param cleanup_size: max number of keys pulled from queue for clean up upon state touch
                for any key
            :param run_cleanup_for_every_record: run incremental cleanup per each processed record
            """
            self._strategies[StateTtlConfig.CleanupStrategies.Strategies.INCREMENTAL_CLEANUP] = \
                StateTtlConfig.CleanupStrategies.IncrementalCleanupStrategy(
                    cleanup_size, run_cleanup_for_every_record)
            return self

        def cleanup_in_rocksdb_compact_filter(
                self,
                query_time_after_num_entries,
                periodic_compaction_time=Time.days(30)) -> \
                'StateTtlConfig.Builder':
            """
            Cleanup expired state while Rocksdb compaction is running.

            RocksDB compaction filter will query current timestamp, used to check expiration, from
            Flink every time after processing {@code queryTimeAfterNumEntries} number of state
            entries. Updating the timestamp more often can improve cleanup speed but it decreases
            compaction performance because it uses JNI call from native code.

            Periodic compaction could speed up expired state entries cleanup, especially for state
            entries rarely accessed. Files older than this value will be picked up for compaction,
            and re-written to the same level as they were before. It makes sure a file goes through
            compaction filters periodically.

            :param query_time_after_num_entries:  number of state entries to process by compaction
                filter before updating current timestamp
            :param periodic_compaction_time:   periodic compaction which could
                speed up expired state cleanup. 0 means turning off periodic compaction.
            :return:
            """
            self._strategies[
                StateTtlConfig.CleanupStrategies.Strategies.ROCKSDB_COMPACTION_FILTER] = \
                StateTtlConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy(
                    query_time_after_num_entries, periodic_compaction_time)
            return self

        def disable_cleanup_in_background(self) -> 'StateTtlConfig.Builder':
            """
            Disable default cleanup of expired state in background (enabled by default).

            If some specific cleanup is configured, e.g. :func:`cleanup_incrementally` or
            :func:`cleanup_in_rocksdb_compact_filter`, this setting does not disable it.
            """
            self._is_cleanup_in_background = False
            return self

        def set_ttl(self, ttl: Time) -> 'StateTtlConfig.Builder':
            """
            Sets the ttl time.

            :param ttl: The ttl time.
            """
            self._ttl = ttl
            return self

        def build(self) -> 'StateTtlConfig':
            return StateTtlConfig(
                self._update_type,
                self._state_visibility,
                self._ttl_time_characteristic,
                self._ttl,
                StateTtlConfig.CleanupStrategies(self._strategies, self._is_cleanup_in_background)
            )

    class CleanupStrategies(object):
        """
        TTL cleanup strategies.

        This class configures when to cleanup expired state with TTL. By default, state is always
        cleaned up on explicit read access if found expired. Currently cleanup of state full
        snapshot can be additionally activated.
        """

        class Strategies(Enum):
            """
            Fixed strategies ordinals in strategies config field.
            """

            FULL_STATE_SCAN_SNAPSHOT = 0
            INCREMENTAL_CLEANUP = 1
            ROCKSDB_COMPACTION_FILTER = 2

            def _to_proto(self):
                from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
                return getattr(
                    StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies, self.name)

            @staticmethod
            def _from_proto(proto):
                from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
                strategies_name = \
                    StateDescriptor.StateTTLConfig.CleanupStrategies.Strategies.Name(proto)
                return StateTtlConfig.CleanupStrategies.Strategies[strategies_name]

        class CleanupStrategy(ABC):
            """
            Base interface for cleanup strategies configurations.
            """
            pass

        class EmptyCleanupStrategy(CleanupStrategy):
            pass

        class IncrementalCleanupStrategy(CleanupStrategy):
            """
            Configuration of cleanup strategy while taking the full snapshot.
            """

            def __init__(self, cleanup_size: int, run_cleanup_for_every_record: bool):
                self._cleanup_size = cleanup_size
                self._run_cleanup_for_every_record = run_cleanup_for_every_record

            def get_cleanup_size(self) -> int:
                return self._cleanup_size

            def run_cleanup_for_every_record(self) -> bool:
                return self._run_cleanup_for_every_record

        class RocksdbCompactFilterCleanupStrategy(CleanupStrategy):
            """
            Configuration of cleanup strategy using custom compaction filter in RocksDB.
            """

            def __init__(self,
                         query_time_after_num_entries: int,
                         periodic_compaction_time=Time.days(30)):
                self._query_time_after_num_entries = query_time_after_num_entries
                self._periodic_compaction_time = periodic_compaction_time

            def get_query_time_after_num_entries(self) -> int:
                return self._query_time_after_num_entries

            def get_periodic_compaction_time(self) -> Time:
                return self._periodic_compaction_time

        EMPTY_STRATEGY = EmptyCleanupStrategy()

        def __init__(self,
                     strategies: Dict[Strategies, CleanupStrategy],
                     is_cleanup_in_background: bool):
            self._strategies = strategies
            self._is_cleanup_in_background = is_cleanup_in_background

        def is_cleanup_in_background(self) -> bool:
            return self._is_cleanup_in_background

        def in_full_snapshot(self) -> bool:
            return (StateTtlConfig.CleanupStrategies.Strategies.FULL_STATE_SCAN_SNAPSHOT in
                    self._strategies)

        def get_incremental_cleanup_strategy(self) \
                -> 'StateTtlConfig.CleanupStrategies.IncrementalCleanupStrategy':
            if self._is_cleanup_in_background:
                default_strategy = \
                    StateTtlConfig.CleanupStrategies.IncrementalCleanupStrategy(5, False)
            else:
                default_strategy = None
            return self._strategies.get(  # type: ignore
                StateTtlConfig.CleanupStrategies.Strategies.INCREMENTAL_CLEANUP,
                default_strategy)

        def get_rocksdb_compact_filter_cleanup_strategy(self) \
                -> 'StateTtlConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy':
            if self._is_cleanup_in_background:
                default_strategy = \
                    StateTtlConfig.CleanupStrategies.RocksdbCompactFilterCleanupStrategy(1000)
            else:
                default_strategy = None
            return self._strategies.get(  # type: ignore
                StateTtlConfig.CleanupStrategies.Strategies.ROCKSDB_COMPACTION_FILTER,
                default_strategy)

        def _to_proto(self):
            from pyflink.fn_execution.flink_fn_execution_pb2 import StateDescriptor
            DescriptorCleanupStrategies = StateDescriptor.StateTTLConfig.CleanupStrategies
            CleanupStrategies = StateTtlConfig.CleanupStrategies

            cleanup_strategies = StateDescriptor.StateTTLConfig.CleanupStrategies()
            cleanup_strategies.is_cleanup_in_background = self._is_cleanup_in_background
            for k, v in self._strategies.items():
                cleanup_strategy = cleanup_strategies.strategies.add()
                cleanup_strategy.strategy = k._to_proto()
                if isinstance(v, CleanupStrategies.EmptyCleanupStrategy):
                    empty_strategy = DescriptorCleanupStrategies.EmptyCleanupStrategy.EMPTY_STRATEGY
                    cleanup_strategy.empty_strategy = empty_strategy
                elif isinstance(v, CleanupStrategies.IncrementalCleanupStrategy):
                    incremental_cleanup_strategy = \
                        DescriptorCleanupStrategies.IncrementalCleanupStrategy()
                    incremental_cleanup_strategy.cleanup_size = v._cleanup_size
                    incremental_cleanup_strategy.run_cleanup_for_every_record = \
                        v._run_cleanup_for_every_record
                    cleanup_strategy.incremental_cleanup_strategy.CopyFrom(
                        incremental_cleanup_strategy)
                elif isinstance(v, CleanupStrategies.RocksdbCompactFilterCleanupStrategy):
                    rocksdb_compact_filter_cleanup_strategy = \
                        DescriptorCleanupStrategies.RocksdbCompactFilterCleanupStrategy()
                    rocksdb_compact_filter_cleanup_strategy.query_time_after_num_entries = \
                        v._query_time_after_num_entries
                    cleanup_strategy.rocksdb_compact_filter_cleanup_strategy.CopyFrom(
                        rocksdb_compact_filter_cleanup_strategy)
            return cleanup_strategies

        @staticmethod
        def _from_proto(proto):
            CleanupStrategies = StateTtlConfig.CleanupStrategies

            strategies = {}
            is_cleanup_in_background = proto.is_cleanup_in_background
            for strategy_entry in proto.strategies:
                strategy = CleanupStrategies.Strategies._from_proto(strategy_entry.strategy)
                if strategy_entry.HasField('empty_strategy'):
                    strategies[strategy] = CleanupStrategies.EmptyCleanupStrategy
                elif strategy_entry.HasField('incremental_cleanup_strategy'):
                    incremental_cleanup_strategy = strategy_entry.incremental_cleanup_strategy
                    strategies[strategy] = CleanupStrategies.IncrementalCleanupStrategy(
                        incremental_cleanup_strategy.cleanup_size,
                        incremental_cleanup_strategy.run_cleanup_for_every_record)
                elif strategy_entry.HasField('rocksdb_compact_filter_cleanup_strategy'):
                    rocksdb_compact_filter_cleanup_strategy = \
                        strategy_entry.rocksdb_compact_filter_cleanup_strategy
                    strategies[strategy] = CleanupStrategies.RocksdbCompactFilterCleanupStrategy(
                        rocksdb_compact_filter_cleanup_strategy.query_time_after_num_entries)
            return CleanupStrategies(strategies, is_cleanup_in_background)
