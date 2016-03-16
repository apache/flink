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


class RuntimeContext(object):
    def __init__(self, iterator, collector, subtask_index):
        self.iterator = iterator
        self.collector = collector
        self.broadcast_variables = dict()
        self.subtask_id = subtask_index

    def _add_broadcast_variable(self, name, var):
        self.broadcast_variables[name] = var

    def get_broadcast_variable(self, name):
        return self.broadcast_variables[name]

    def get_index_of_this_subtask(self):
        return self.subtask_id
