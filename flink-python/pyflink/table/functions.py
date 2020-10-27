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
import sys
import time
from abc import abstractmethod
from decimal import Decimal

from pyflink.table import AggregateFunction, MapView

MAX_LONG_VALUE = sys.maxsize
MIN_LONG_VALUE = -MAX_LONG_VALUE - 1


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

            # get the order of current value if not given
            if len(args) == 1:
                order = int(time.time() * 1000)
                if value in value_to_order_map:
                    order_list = value_to_order_map[value]
                else:
                    order_list = []
                order_list.append(order)
                value_to_order_map[value] = order_list
            else:
                order = args[1]

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

            # get the order of current value if not given
            if len(args) == 1:
                if value in value_to_order_map and len(value_to_order_map[value]) > 0:
                    order_list = value_to_order_map[value]
                else:
                    # this data has not been accumulated
                    return
                # get and remove current order in value_to_order_map
                order = order_list.pop(0)
                if len(order_list) == 0:
                    del value_to_order_map[value]
                else:
                    value_to_order_map[value] = order_list
            else:
                order = args[1]

            # remove current value in order_to_value_map
            if order in order_to_value_map:
                value_list = order_to_value_map[order]
            else:
                # this data has not been accumulated
                return
            if value in value_list:
                value_list.remove(value)
                if len(value_list) == 0:
                    del order_to_value_map[order]
                else:
                    order_to_value_map[order] = value_list

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
