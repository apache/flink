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
__all__ = ['JobID']


class JobID(object):
    """
    Unique (at least statistically unique) identifier for a Flink Job. Jobs in Flink correspond
    to dataflow graphs.

    Jobs act simultaneously as sessions, because jobs can be created and submitted incrementally
    in different parts. Newer fragments of a graph can be attached to existing graphs, thereby
    extending the current data flow graphs.
    """

    def __init__(self, j_job_id):
        self._j_job_id = j_job_id

    def __str__(self):
        return self._j_job_id.toString()
