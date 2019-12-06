#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

import subprocess
import traceback
from logger import logger
from threading import Timer


def timeout_callback(p):
    logger.warning('exe time out call back')
    try:
        p.kill()
    except Exception as error:
        logger.error(error)


def run_command(cmd, timeout=12000):
    logger.info("cmd:%s" % cmd)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    my_timer = Timer(timeout, timeout_callback, [p])
    my_timer.start()
    lines = []
    try:
        while True:
            buff = p.stdout.readline()
            line_buff = str(buff, encoding="utf-8")
            line = line_buff.split("\n")[0]
            logger.info(line)
            if line_buff == "" and not p.poll() is None:
                break
            else:
                lines.append(buff)
    except Exception as e:
        logger.error(traceback.format_exc())
        return False, "", ""
    finally:
        my_timer.cancel()
        exit_code = p.returncode
        if exit_code == 0:
            result = True
        else:
            result = Flase
        return result, "".join(lines)
