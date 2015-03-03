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


try:
    import dill
    port = int(sys.argv[1])

    s1 = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    s1.bind((socket.gethostbyname("localhost"), 0))
    s1.sendto(struct.pack(">i", s1.getsockname()[1]), (socket.gethostbyname("localhost"), port))

    size = struct.unpack(">i", s1.recv(4))[0]
    serialized_operator = s1.recv(size)

    size = struct.unpack(">i", s1.recv(4))[0]
    import_string = s1.recv(size).decode("utf-8")

    size = struct.unpack(">i", s1.recv(4))[0]
    input_file = s1.recv(size).decode("utf-8")

    size = struct.unpack(">i", s1.recv(4))[0]
    output_file = s1.recv(size).decode("utf-8")

    exec(import_string)
    
    operator = dill.loads(serialized_operator)
    operator._configure(input_file, output_file, port)
    operator._go()
    sys.stdout.flush()
    sys.stderr.flush()
except:
    sys.stdout.flush()
    sys.stderr.flush()
    s = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    s.bind((socket.gethostbyname("localhost"), 0))
    destination = (socket.gethostbyname("localhost"), int(sys.argv[1]))
    s.sendto(struct.pack(">i", -2), destination)
    raise