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
import json
import os
import uuid

__all__ = ['DependencyManager']


class DependencyManager(object):
    """
    Utility class for dependency management. The dependencies will be registered at the distributed
    cache.
    """

    PYTHON_FILE_PREFIX = "python_file"
    PYTHON_REQUIREMENTS_FILE_PREFIX = "python_requirements_file"
    PYTHON_REQUIREMENTS_CACHE_PREFIX = "python_requirements_cache"
    PYTHON_ARCHIVE_PREFIX = "python_archive"

    PYTHON_FILES = "python.files"
    PYTHON_REQUIREMENTS_FILE = "python.requirements-file"
    PYTHON_REQUIREMENTS_CACHE = "python.requirements-cache"
    PYTHON_ARCHIVES = "python.archives"
    PYTHON_EXEC = "python.exec"

    # Environment variable names used to store the dependency settings specified via commandline
    # options.
    PYFLINK_PY_FILES = "PYFLINK_PY_FILES"
    PYFLINK_PY_REQUIREMENTS = "PYFLINK_PY_REQUIREMENTS"
    PYFLINK_PY_EXECUTABLE = "PYFLINK_PY_EXECUTABLE"
    PYFLINK_PY_ARCHIVES = "PYFLINK_PY_ARCHIVES"

    def __init__(self, config, j_env):
        self._config = config
        self._j_env = j_env
        self._python_file_map = dict()  # type: dict[str, str]
        self._archives_map = dict()  # type: dict[str, str]
        self._counter = -1

    def add_python_file(self, file_path):
        file_key = self._generate_unique_file_key(self.PYTHON_FILE_PREFIX)
        self._j_env.registerCachedFile(file_path, file_key)
        self._python_file_map[file_key] = os.path.basename(file_path)
        self._config.set_string(
            self.PYTHON_FILES, json.dumps(self._python_file_map))

    def set_python_requirements(self, requirements_file_path, requirements_cached_dir=None):
        self._remove_cached_file_if_exists(self.PYTHON_REQUIREMENTS_FILE)
        self._remove_cached_file_if_exists(self.PYTHON_REQUIREMENTS_CACHE)

        file_key = self._generate_unique_file_key(self.PYTHON_REQUIREMENTS_FILE_PREFIX)
        self._j_env.registerCachedFile(requirements_file_path, file_key)
        self._config.set_string(self.PYTHON_REQUIREMENTS_FILE, file_key)
        if requirements_cached_dir is not None:
            file_key = self._generate_unique_file_key(self.PYTHON_REQUIREMENTS_CACHE_PREFIX)
            self._j_env.registerCachedFile(requirements_cached_dir, file_key)
            self._config.set_string(self.PYTHON_REQUIREMENTS_CACHE, file_key)

    def add_python_archive(self, archive_path, target_dir=None):
        file_key = self._generate_unique_file_key(self.PYTHON_ARCHIVE_PREFIX)
        self._j_env.registerCachedFile(archive_path, file_key)
        if target_dir is not None:
            self._archives_map[file_key] = target_dir
        else:
            self._archives_map[file_key] = os.path.basename(archive_path)
        self._config.set_string(self.PYTHON_ARCHIVES, json.dumps(self._archives_map))

    def _generate_unique_file_key(self, config_key):
        self._counter += 1
        return "%s_%d_%s" % (config_key, self._counter, uuid.uuid4())

    def _remove_cached_file_if_exists(self, config_key):
        if self._config.contains_key(config_key):
            file_key = self._config.get_string(config_key, "")
            cached_files = self._j_env.getCachedFiles()
            for key_file_tuple in cached_files:
                if file_key == key_file_tuple.f0:
                    cached_files.remove(key_file_tuple)
                    break
            self._config.remove_config(config_key)

    def load_from_env(self, env):
        """
        Loads python dependency settings specified via command line options from the environment
        variable.
        """
        if self.PYFLINK_PY_FILES in env:
            py_files = env[self.PYFLINK_PY_FILES].split("\n")
            for file_path in py_files:
                self.add_python_file(file_path)

        if self.PYFLINK_PY_ARCHIVES in env:
            py_archives = env[self.PYFLINK_PY_ARCHIVES].split("\n")
            for i in range(0, len(py_archives), 2):
                self.add_python_archive(py_archives[i], py_archives[i + 1] or None)

        if self.PYFLINK_PY_REQUIREMENTS in env:
            requirements_file_path, requirements_cache_dir = \
                env[self.PYFLINK_PY_REQUIREMENTS].split("\n")
            requirements_cache_dir = requirements_cache_dir or None
            self.set_python_requirements(requirements_file_path, requirements_cache_dir)

        if self.PYFLINK_PY_EXECUTABLE in env:
            self._config.set_string(self.PYTHON_EXEC, env[self.PYFLINK_PY_EXECUTABLE])
