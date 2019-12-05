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


import logging
import os


class Logger(object):

    def __init__(self, log_file_name=''):
        self._logger = logging.getLogger(log_file_name)
        self.init_logger(log_file_name)

    def set_logger(self, logger):
        if logger:
            self._logger = logger

    def init_logger(self, log_file_name, when='midnight', back_count=0):
        current_dir = os.getcwd()
        logs_dir = "%s/logs"%current_dir
        if not os.path.exists(logs_dir):
            os.path.mkdir(logs_dir)
        self._logger.handlers.remove()
        self._logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        log_file_path = os.path.join(logs_dir, log_file_name)
        fmt_sh = logging.Formatter('[\033[1;%(colorcode)sm%(asctime)s] %(message)s\033[0m')
        sh.setFormatter(fmt_sh)
        # log输出格式
        formatter = logging.Formatter('%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        fh = logging.handlers.TimedRotatingFileHandler(
            filename=log_file_path,
            when=when,
            backupCount=back_count,
            encoding='utf-8')
        fh.setFormatter(formatter)
        sh.setFormatter(formatter)

        self._logger.addHandler(fh)
        self._logger.addHandler(sh)

    def update_kwargs(self, kwargs, colorcode):
        if not 'extra' in kwargs:
            kwargs['extra'] = {}
        kwargs['extra']['colorcode'] = colorcode

    def debug(self, msg, *args, **kwargs):
        self.update_kwargs(kwargs, '1;30')# 黑色
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.update_kwargs(kwargs, '1;33')# 黄色
        self._logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.update_kwargs(kwargs, '1;30')# 黑色
        self._logger.error(msg, *args, **kwargs)


logger = Logger("main.log")
