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
import time
from abc import abstractmethod
from decimal import Decimal

from pyflink.common.constants import MAX_LONG_VALUE, MIN_LONG_VALUE
from pyflink.table import AggregateFunction, MapView, ListView


class AvgAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        # sum / count
        if accumulator[0] != 0:
            return accumulator[1] / accumulator[0]
        else:
            return None

    def create_accumulator(self):
        # [count, sum]
        return [0, 0]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] += 1
            accumulator[1] += args[0]

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] -= 1
            accumulator[1] -= args[0]

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            if acc[1] is not None:
                accumulator[0] += acc[0]
                accumulator[1] += acc[1]


class Count1AggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        accumulator[0] += 1

    def retract(self, accumulator, *args):
        accumulator[0] -= 1

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            accumulator[0] += acc[0]


class CountAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [0]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] += 1

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] -= 1

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            accumulator[0] += acc[0]


class FirstValueAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        # [first_value]
        return [None]

    def accumulate(self, accumulator, *args):
        if accumulator[0] is None and args[0] is not None:
            accumulator[0] = args[0]

    def retract(self, accumulator, *args):
        raise NotImplementedError("This function does not support retraction.")

    def merge(self, accumulator, accumulators):
        raise NotImplementedError("This function does not support merge.")


class FirstValueWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        # [first_value, first_order, value_to_order_map, order_to_value_map]
        return [None, None, MapView(), MapView()]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            prev_order = accumulator[1]
            value_to_order_map = accumulator[2]
            order_to_value_map = accumulator[3]

            # calculate the order of current value
            order = int(round(time.time() * 1000))
            if value in value_to_order_map:
                order_list = value_to_order_map[value]
            else:
                order_list = []
            order_list.append(order)
            value_to_order_map[value] = order_list

            if prev_order is None or prev_order > order:
                accumulator[0] = value
                accumulator[1] = order
            if order in order_to_value_map:
                value_list = order_to_value_map[order]
            else:
                value_list = []
            value_list.append(value)
            order_to_value_map[order] = value_list

    def retract(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            prev_value = accumulator[0]
            prev_order = accumulator[1]
            value_to_order_map = accumulator[2]
            order_to_value_map = accumulator[3]

            # calculate the order of current value
            if value in value_to_order_map and value_to_order_map[value]:
                order_list = value_to_order_map[value]
            else:
                # this data has not been accumulated
                return
            # get and remove current order in value_to_order_map
            order = order_list.pop(0)
            if order_list:
                value_to_order_map[value] = order_list
            else:
                del value_to_order_map[value]

            # remove current value in order_to_value_map
            if order in order_to_value_map:
                value_list = order_to_value_map[order]
            else:
                # this data has not been accumulated
                return
            if value in value_list:
                value_list.remove(value)
                if value_list:
                    order_to_value_map[order] = value_list
                else:
                    del order_to_value_map[order]

            if value == prev_value:
                start_key = prev_order
                next_key = MAX_LONG_VALUE
                for key in order_to_value_map:
                    if start_key <= key < next_key:
                        next_key = key

                if next_key != MAX_LONG_VALUE:
                    accumulator[0] = order_to_value_map[next_key][0]
                    accumulator[1] = next_key
                else:
                    accumulator[0] = None
                    accumulator[1] = None

    def merge(self, accumulator, accumulators):
        raise NotImplementedError("This function does not support merge.")


class LastValueAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        # [last_value]
        return [None]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] = args[0]

    def retract(self, accumulator, *args):
        raise NotImplementedError("This function does not support retraction.")

    def merge(self, accumulator, accumulators):
        raise NotImplementedError("This function does not support merge.")


class LastValueWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        # [last_value, last_order, value_to_order_map, order_to_value_mapl9]
        return [None, None, MapView(), MapView()]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            prev_order = accumulator[1]
            value_to_order_map = accumulator[2]
            order_to_value_map = accumulator[3]

            # calculate the order of current value
            order = int(time.time() * 1000)
            if value in value_to_order_map:
                order_list = value_to_order_map[value]
            else:
                order_list = []
            order_list.append(order)
            value_to_order_map[value] = order_list

            if prev_order is None or prev_order <= order:
                accumulator[0] = value
                accumulator[1] = order

            if order in order_to_value_map:
                value_list = order_to_value_map[order]
            else:
                value_list = []
            value_list.append(value)
            order_to_value_map[order] = value_list

    def retract(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            prev_value = accumulator[0]
            prev_order = accumulator[1]
            value_to_order_map = accumulator[2]
            order_to_value_map = accumulator[3]

            # calculate the order of current value
            if value in value_to_order_map and value_to_order_map[value]:
                order_list = value_to_order_map[value]
            else:
                # this data has not been accumulated
                return
            # get and remove current order in value_to_order_map
            order = order_list.pop(0)
            if order_list:
                value_to_order_map[value] = order_list
            else:
                del value_to_order_map[value]

            if order in order_to_value_map:
                value_list = order_to_value_map[order]
            else:
                return

            if value in value_list:
                value_list.remove(value)
                if value_list:
                    order_to_value_map[order] = value_list
                else:
                    del order_to_value_map[order]

            if value == prev_value:
                start_key = prev_order
                next_key = MIN_LONG_VALUE
                for key in order_to_value_map:
                    if start_key >= key > next_key:
                        next_key = key

                if next_key != MIN_LONG_VALUE:
                    values = order_to_value_map[next_key]
                    accumulator[0] = values[len(values) - 1]
                    accumulator[1] = next_key
                else:
                    accumulator[0] = None
                    accumulator[1] = None

    def merge(self, accumulator, accumulators):
        raise NotImplementedError("This function does not support merge.")


class ListAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        if accumulator[1]:
            return accumulator[0].join(accumulator[1])
        else:
            return None

    def create_accumulator(self):
        # delimiter, values
        return [',', []]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            if len(args) > 1:
                accumulator[0] = args[1]
            accumulator[1].append(args[0])

    def retract(self, accumulator, *args):
        raise NotImplementedError("This function does not support retraction.")


class ListAggWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        values = [i for i in accumulator[0]]
        if values:
            return ','.join(values)
        else:
            return None

    def create_accumulator(self):
        # [list, retract_list]
        return [ListView(), ListView()]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0].add(args[0])

    def retract(self, accumulator, *args):
        if args[0] is not None:
            values = [i for i in accumulator[0]]
            try:
                values.remove(args[0])
                accumulator[0].clear()
                accumulator[0].add_all(values)
            except ValueError:
                accumulator[1].add(args[0])

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            buffer = [e for e in acc[0]]
            retract_buffer = [e for e in acc[1]]

            if buffer or retract_buffer:
                for e in accumulator[0]:
                    buffer.append(e)
                for e in accumulator[1]:
                    retract_buffer.append(e)

                # merge list & retract list
                new_retract_buffer = []
                for e in retract_buffer:
                    if e in buffer:
                        buffer.remove(e)
                    else:
                        new_retract_buffer.append(e)

                accumulator[0].clear()
                accumulator[0].add_all(buffer)
                accumulator[1].clear()
                accumulator[1].add_all(new_retract_buffer)


class ListAggWsWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        values = [i for i in accumulator[0]]
        if values:
            return accumulator[2].join(values)
        else:
            return None

    def create_accumulator(self):
        # [list, retract_list, delimiter]
        return [ListView(), ListView(), ',']

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[2] = args[1]
            accumulator[0].add(args[0])

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[2] = args[1]
            values = [i for i in accumulator[0]]
            if args[0] in values:
                values.remove(args[0])
                accumulator[0].clear()
                accumulator[0].add_all(values)
            else:
                accumulator[1].add(args[0])

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            buffer = [e for e in acc[0]]
            retract_buffer = [e for e in acc[1]]

            if buffer or retract_buffer:
                accumulator[2] = acc[2]
                for e in accumulator[0]:
                    buffer.append(e)
                for e in accumulator[1]:
                    retract_buffer.append(e)

                # merge list & retract list
                new_retract_buffer = []
                for e in retract_buffer:
                    if e in buffer:
                        buffer.remove(e)
                    else:
                        new_retract_buffer.append(e)

                accumulator[0].clear()
                accumulator[0].add_all(buffer)
                accumulator[1].clear()
                accumulator[1].add_all(retract_buffer)


class MaxAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return [None]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            if accumulator[0] is None or args[0] > accumulator[0]:
                accumulator[0] = args[0]

    def retract(self, accumulator, *args):
        raise NotImplementedError("This function does not support retraction.")

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            if acc[0] is not None:
                if accumulator[0] is None or acc[0] > accumulator[0]:
                    accumulator[0] = acc[0]


class MaxWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        if accumulator[1] > 0:
            return accumulator[0]
        else:
            return None

    def create_accumulator(self):
        # [max, map_size, value_to_count_map]
        return [None, 0, MapView()]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            if accumulator[1] == 0 or accumulator[0] < value:
                accumulator[0] = value

            if value in accumulator[2]:
                count = accumulator[2][value]
            else:
                count = 0

            count += 1
            if count == 0:
                del accumulator[2][value]
            else:
                accumulator[2][value] = count
            if count == 1:
                accumulator[1] += 1

    def retract(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            if value in accumulator[2]:
                count = accumulator[2][value]
            else:
                count = 0

            count -= 1
            if count == 0:
                del accumulator[2][value]
                accumulator[1] -= 1

                if accumulator[1] == 0:
                    accumulator[0] = None
                    return

                if value == accumulator[0]:
                    self.update_max(accumulator)

    @staticmethod
    def update_max(acc):
        has_max = False
        for value in acc[2]:
            if not has_max or acc[0] < value:
                acc[0] = value
                has_max = True

        # The behavior of deleting expired data in the state backend is uncertain.
        # so `mapSize` data may exist, while `map` data may have been deleted
        # when both of them are expired.
        if not has_max:
            acc[0] = None
            # we should also override max value, because it may have an old value.
            acc[1] = 0

    def merge(self, acc, accumulators):
        need_update_max = False
        for a in accumulators:
            # set max element
            if acc[1] == 0 or (a[1] > 0 and a[0] is not None and acc[0] < a[0]):
                acc[0] = a[0]

            # merge the count for each key
            for value, count in a[2].items():
                if value in acc[2]:
                    this_count = acc[2][value]
                else:
                    this_count = 0
                merged_count = count + this_count
                if merged_count == 0:
                    # remove it when count is increased from -1 to 0
                    del acc[2][value]
                    # origin is > 0, and retract to 0
                    if this_count > 0:
                        acc[1] -= 1
                        if value == acc[0]:
                            need_update_max = True
                elif merged_count < 0:
                    acc[2][value] = merged_count
                    if this_count > 0:
                        # origin is > 0, and retract to < 0
                        acc[1] -= 1
                        if value == acc[0]:
                            need_update_max = True
                else:  # merged_count > 0
                    acc[2][value] = merged_count
                    if this_count <= 0:
                        # origin is <= 0, and accumulate to > 0
                        acc[1] += 1

        if need_update_max:
            self.update_max(acc)


class MinAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        # [min]
        return [None]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            if accumulator[0] is None or accumulator[0] > args[0]:
                accumulator[0] = args[0]

    def retract(self, accumulator, *args):
        raise NotImplementedError("This function does not support retraction.")

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            if acc[0] is not None:
                if accumulator[0] is None or accumulator[0] > acc[0]:
                    accumulator[0] = acc[0]


class MinWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        if accumulator[1] > 0:
            return accumulator[0]
        else:
            return None

    def create_accumulator(self):
        # [min, map_size, value_to_count_map]
        return [None, 0, MapView()]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            if accumulator[1] == 0 or accumulator[0] > value:
                accumulator[0] = value

            if value in accumulator[2]:
                count = accumulator[2][value]
            else:
                count = 0

            count += 1
            if count == 0:
                del accumulator[2][value]
            else:
                accumulator[2][value] = count
            if count == 1:
                accumulator[1] += 1

    def retract(self, accumulator, *args):
        if args[0] is not None:
            value = args[0]
            if value in accumulator[2]:
                count = accumulator[2][value]
            else:
                count = 0

            count -= 1
            if count == 0:
                del accumulator[2][value]
                accumulator[1] -= 1

                if accumulator[1] == 0:
                    accumulator[0] = None
                    return

                if value == accumulator[0]:
                    self.update_min(accumulator)

    @staticmethod
    def update_min(acc):
        has_max = False
        for value in acc[2]:
            if not has_max or acc[0] > value:
                acc[0] = value
                has_max = True

        # The behavior of deleting expired data in the state backend is uncertain.
        # so `mapSize` data may exist, while `map` data may have been deleted
        # when both of them are expired.
        if not has_max:
            acc[0] = None
            # we should also override min value, because it may have an old value.
            acc[1] = 0

    def merge(self, acc, accumulators):
        need_update_min = False
        for a in accumulators:
            # set min element
            if acc[1] == 0 or (a[1] > 0 and a[0] is not None and acc[0] > a[0]):
                acc[0] = a[0]

            # merge the count for each key
            for value, count in a[2].items():
                if value in acc[2]:
                    this_count = acc[2][value]
                else:
                    this_count = 0
                merged_count = count + this_count
                if merged_count == 0:
                    # remove it when count is increased from -1 to 0
                    del acc[2][value]
                    # origin is > 0, and retract to 0
                    if this_count > 0:
                        acc[1] -= 1
                        if value == acc[0]:
                            need_update_min = True
                elif merged_count < 0:
                    acc[2][value] = merged_count
                    if this_count > 0:
                        # origin is > 0, and retract to < 0
                        acc[1] -= 1
                        if value == acc[0]:
                            need_update_min = True
                else:  # merged_count > 0
                    acc[2][value] = merged_count
                    if this_count <= 0:
                        # origin is <= 0, and accumulate to > 0
                        acc[1] += 1

        if need_update_min:
            self.update_min(acc)


class Sum0AggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    @abstractmethod
    def create_accumulator(self):
        pass

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] += args[0]

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] -= args[0]

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            accumulator[0] += acc[0]


class IntSum0AggFunction(Sum0AggFunction):

    def create_accumulator(self):
        # [sum]
        return [0]


class FloatSum0AggFunction(Sum0AggFunction):

    def create_accumulator(self):
        # [sum]
        return [0.0]


class DecimalSum0AggFunction(Sum0AggFunction):

    def create_accumulator(self):
        # [sum]
        return [Decimal('0')]


class SumAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        # [sum]
        return [None]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            if accumulator[0] is None:
                accumulator[0] = args[0]
            else:
                accumulator[0] += args[0]

    def retract(self, accumulator, *args):
        raise NotImplementedError("This function does not support retraction.")

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            if acc[0] is not None:
                if accumulator[0] is None:
                    accumulator[0] = acc[0]
                else:
                    accumulator[0] += acc[0]


class SumWithRetractAggFunction(AggregateFunction):

    def get_value(self, accumulator):
        if accumulator[1] == 0:
            return None
        else:
            return accumulator[0]

    def create_accumulator(self):
        # [sum, count]
        return [0, 0]

    def accumulate(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] += args[0]
            accumulator[1] += 1

    def retract(self, accumulator, *args):
        if args[0] is not None:
            accumulator[0] -= args[0]
            accumulator[1] -= 1

    def merge(self, accumulator, accumulators):
        for acc in accumulators:
            if acc[0] is not None:
                accumulator[0] += acc[0]
                accumulator[1] += acc[1]
