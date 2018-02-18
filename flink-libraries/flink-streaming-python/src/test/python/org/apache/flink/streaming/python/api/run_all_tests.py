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
import traceback
from glob import glob
from os.path import dirname, join, basename

excluded_tests = [
    'test_kafka09',
]


class Main:
    def __init__(self):
        pass

    def run(self, flink):
        tests = []

        current_dir = dirname(sys.modules[__name__].__file__)
        print("Working directory: {}".format(current_dir))

        if excluded_tests:
            print("Excluded tests: {}\n".format(excluded_tests))

        for x in glob(join(current_dir, 'test_*.py')):
            if not x.startswith('__'):
                test_module_name = basename(x)[:-3]
                if test_module_name not in excluded_tests:
                    tests.append(__import__(test_module_name, globals(), locals()))

        try:
            for test in tests:
                print("Submitting job ... '{}'".format(test.__name__))
                test.main(flink)
                print("Job completed ('{}')\n".format(test.__name__))
        except Exception as ex:
            print ("Test {} has failed\n".format(test.__name__))
            traceback.print_exc()


def main(flink):
    Main().run(flink)
