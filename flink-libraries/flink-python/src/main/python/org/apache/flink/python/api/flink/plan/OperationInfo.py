# ###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from flink.plan.Constants import WriteMode


class OperationInfo():
    def __init__(self, info=None):
        if info is None:
            self.parent = None
            self.other = None
            self.parent_set = None
            self.other_set = None
            self.identifier = None
            self.field = None
            self.order = None
            self.keys = None
            self.key1 = None
            self.key2 = None
            self.types = None
            self.operator = None
            self.uses_udf = False
            self.name = None
            self.delimiter_line = "\n"
            self.delimiter_field = ","
            self.write_mode = WriteMode.NO_OVERWRITE
            self.sinks = []
            self.children = []
            self.path = None
            self.count = 0
            self.values = []
            self.projections = []
            self.bcvars = []
            self.id = None
            self.to_err = False
            self.chained_info = None
        else:
            self.__dict__.update(info.__dict__)

