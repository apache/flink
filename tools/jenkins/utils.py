#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Common functions
#
# Note by AiHua Li


import subprocess
import tempfile
import traceback
import uuid
from threading import Timer


def timeout_callback(p):
    print 'exe time out call back'
    try:
        p.kill()
    except Exception as error:
        print error


def run_command(cmd, timeout=12000):
    script = '''#!/bin/sh
    %s
    '''%cmd
    script_file = tempfile.NamedTemporaryFile('wt')
    script_file.write(script)
    script_file.flush()
    outputFile = "/tmp/%s"%uuid.uuid1()
    stderrFile = "/tmp/%s"%uuid.uuid1()
    stdout = open(outputFile, 'wb')
    stderr = open(stderrFile, 'wb')
    cmd = ['bash', script_file.name]
    p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    my_timer = Timer(timeout, timeout_callback, [p])
    my_timer.start()
    try:
        p.communicate()
    except Exception, e:
        print(traceback.format_exc())
    finally:
        my_timer.cancel()
        stdout.flush()
        stderr.flush()
        stdout.close()
        stderr.close()
        logfile = open(outputFile, 'r')
        lines = logfile.readlines()
        output = "".join(lines)
        logfile.close()
        exit_code = p.returncode
        return exit_code, output
