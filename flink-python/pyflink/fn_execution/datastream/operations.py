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
from abc import ABC

DATA_STREAM_STATELESS_FUNCTION_URN = "flink:transform:ds:stateless_function:v1"
DATA_STREAM_STATEFUL_FUNCTION_URN = "flink:transform:ds:stateful_function:v1"


class Operation(ABC):

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass


class OneInputOperation(ABC):

    def process_element(self, value):
        raise NotImplementedError


class TwoInputOperation(ABC):
    def process_element1(self, value):
        raise NotImplementedError

    def process_element2(self, value):
        raise NotImplementedError
