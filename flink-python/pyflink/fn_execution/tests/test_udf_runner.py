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
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest


@unittest.skipIf(os.name == 'nt', 'The Bash worker launcher is only used on Unix.')
class UdfRunnerTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='pyflink-udf-runner-')
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.runner = Path(__file__).resolve().parents[2] / 'bin' / 'pyflink-udf-runner.sh'
        self.boot_log = self.directory / 'flink-python-udf-boot.log'
        package = self.directory
        for name in ['pyflink', 'fn_execution', 'beam']:
            package /= name
            package.mkdir()
            (package / '__init__.py').touch()
        self.worker_module = package / 'beam_boot.py'

    def start_worker(self, script, boot_log_dir=None):
        self.worker_module.write_text('import os, signal, sys\n' + script + '\n')
        environment = dict(os.environ, python=sys.executable, FLINK_TESTING='0',
                           _PYTHON_WORKING_DIR='', PYTHONPATH=str(self.directory),
                           BOOT_LOG_DIR=str(boot_log_dir or self.directory))
        return subprocess.Popen(
            ['bash', str(self.runner), '--id', 'test-worker'],
            env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True)

    def run_worker(self, script, boot_log_dir=None):
        with self.start_worker(script, boot_log_dir) as process:
            try:
                output, _ = process.communicate(timeout=10)
                return subprocess.CompletedProcess(process.args, process.returncode, output)
            finally:
                process.kill()
                process.communicate(timeout=10)

    def test_sigkill_is_preserved(self):
        result = self.run_worker("print('worker-started', flush=True)\n"
                                 'os.kill(os.getpid(), signal.SIGKILL)')

        self.assertEqual(-signal.SIGKILL, result.returncode)
        self.assertIn('worker-started', result.stdout)
        self.assertIn('worker-started', self.boot_log.read_text())

    def test_nonzero_exit_is_preserved(self):
        result = self.run_worker("print('worker-failed')\nsys.exit(23)")

        self.assertEqual(23, result.returncode)
        self.assertEqual('worker-failed\n', result.stdout)
        self.assertEqual('worker-failed\n', self.boot_log.read_text())

    def test_success_preserves_worker_output(self):
        result = self.run_worker("print('worker-completed')")

        self.assertEqual(0, result.returncode)
        self.assertEqual('worker-completed\n', result.stdout)
        self.assertEqual('worker-completed\n', self.boot_log.read_text())

    def test_log_failure_does_not_override_worker_success(self):
        result = self.run_worker('sys.exit(0)', self.directory / 'missing-directory')

        self.assertEqual(0, result.returncode)

    def test_worker_failure_is_preserved_when_logging_also_fails(self):
        result = self.run_worker('sys.exit(23)', self.directory / 'missing-directory')

        self.assertEqual(23, result.returncode)

    def test_worker_replaces_launcher(self):
        result = self.run_worker('print(os.getppid())')

        self.assertEqual(0, result.returncode)
        self.assertEqual(str(os.getpid()), result.stdout.strip())

    def test_sigkill_is_preserved_during_cleanup(self):
        with self.start_worker('print(os.getpid(), flush=True)\nsignal.pause()') as process:
            try:
                ready, _, _ = select.select([process.stdout], [], [], 10)
                self.assertTrue(ready, 'Worker did not publish its PID.')
                worker_pid = int(process.stdout.readline())
                os.kill(worker_pid, signal.SIGKILL)
                process.terminate()
                process.communicate(timeout=10)

                self.assertEqual(worker_pid, process.pid)
                self.assertEqual(-signal.SIGKILL, process.returncode)
            finally:
                process.kill()
                process.communicate(timeout=10)


if __name__ == '__main__':
    unittest.main()
