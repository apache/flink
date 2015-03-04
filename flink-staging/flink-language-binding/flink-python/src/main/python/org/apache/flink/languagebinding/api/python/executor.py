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
import socket
import struct
#argv[1] = port

s = None
try:
    import dill
    port = int(sys.argv[1])

    s = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    s.connect((socket.gethostbyname("localhost"), port))

    size = struct.unpack(">i", s.recv(4, socket.MSG_WAITALL))[0]
    serialized_operator = s.recv(size, socket.MSG_WAITALL)

    size = struct.unpack(">i", s.recv(4, socket.MSG_WAITALL))[0]
    import_string = s.recv(size, socket.MSG_WAITALL).decode("utf-8")

    size = struct.unpack(">i", s.recv(4, socket.MSG_WAITALL))[0]
    input_file = s.recv(size, socket.MSG_WAITALL).decode("utf-8")

    size = struct.unpack(">i", s.recv(4, socket.MSG_WAITALL))[0]
    output_file = s.recv(size, socket.MSG_WAITALL).decode("utf-8")

    exec(import_string)
    
    operator = dill.loads(serialized_operator)
    operator._configure(input_file, output_file, s)
    operator._go()
    sys.stdout.flush()
    sys.stderr.flush()
except:
    sys.stdout.flush()
    sys.stderr.flush()
    s.send(struct.pack(">i", -2))
    raise