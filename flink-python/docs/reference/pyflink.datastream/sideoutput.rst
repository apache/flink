.. ################################################################################
     Licensed to the Apache Software Foundation (ASF) under one
     or more contributor license agreements.  See the NOTICE file
     distributed with this work for additional information
     regarding copyright ownership.  The ASF licenses this file
     to you under the Apache License, Version 2.0 (the
     "License"); you may not use this file except in compliance
     with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
    limitations under the License.
   ################################################################################


============
Side Outputs
============

OutputTag
---------

An :class:`OutputTag` is a typed and named tag to use for tagging side outputs of an operator.

Example:
::

    # Explicitly specify output type
    >>> info = OutputTag("late-data", Types.TUPLE([Types.STRING(), Types.LONG()]))
    # Implicitly wrap list to Types.ROW
    >>> info_row = OutputTag("row", [Types.STRING(), Types.LONG()])
    # Implicitly use pickle serialization
    >>> info_side = OutputTag("side")
    # ERROR: tag id cannot be empty string (extra requirement for Python API)
    >>> info_error = OutputTag("")
