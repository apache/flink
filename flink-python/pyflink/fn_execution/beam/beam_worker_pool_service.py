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
import atexit
import functools
import logging
import os
import sys
import threading
import traceback

import grpc

# In order to remove confusing infos produced by beam.
logging.getLogger().setLevel(logging.WARNING)

from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import ProfilingOptions
from apache_beam.portability.api import beam_fn_api_pb2
from apache_beam.portability.api import beam_fn_api_pb2_grpc
from apache_beam.portability.api import endpoints_pb2
from apache_beam.portability.api.org.apache.beam.model.fn_execution.v1.beam_provision_api_pb2 \
    import GetProvisionInfoRequest
from apache_beam.portability.api.org.apache.beam.model.fn_execution.v1.beam_provision_api_pb2_grpc \
    import ProvisionServiceStub
from apache_beam.runners.worker import sdk_worker_main
from apache_beam.runners.worker.log_handler import FnApiLogRecordHandler
from apache_beam.runners.worker.sdk_worker import SdkHarness
from apache_beam.utils import thread_pool_executor, profiler
from google.protobuf import json_format

from pyflink.fn_execution.beam import beam_sdk_worker_main  # noqa # pylint: disable=unused-import

_LOGGER = logging.getLogger(__name__)


class BeamFnLoopbackWorkerPoolServicer(beam_fn_api_pb2_grpc.BeamFnExternalWorkerPoolServicer):
    """
    Worker pool entry point.

    The worker pool exposes an RPC service that is used in MiniCluster to start and stop the Python
    SDK workers.

    The worker pool uses child thread for parallelism
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    def __init__(self):
        self._parse_param_lock = threading.Lock()
        self._worker_address = None
        self._old_working_dir = None
        self._old_python_path = None
        self._ref_cnt = 0

    def start(self):
        if not self._worker_address:
            worker_server = grpc.server(
                thread_pool_executor.shared_unbounded_instance())
            worker_address = 'localhost:%s' % worker_server.add_insecure_port('[::]:0')
            beam_fn_api_pb2_grpc.add_BeamFnExternalWorkerPoolServicer_to_server(self, worker_server)
            worker_server.start()

            self._worker_address = worker_address
            atexit.register(functools.partial(worker_server.stop, 1))
        return self._worker_address

    def StartWorker(self,
                    start_worker_request: beam_fn_api_pb2.StartWorkerRequest,
                    unused_context):
        try:
            self._start_sdk_worker_main(start_worker_request)
            return beam_fn_api_pb2.StartWorkerResponse()
        except Exception:
            return beam_fn_api_pb2.StartWorkerResponse(error=traceback.format_exc())

    def StopWorker(self,
                   stop_worker_request: beam_fn_api_pb2.StopWorkerRequest,
                   unused_context):
        pass

    def _start_sdk_worker_main(self, start_worker_request: beam_fn_api_pb2.StartWorkerRequest):
        params = start_worker_request.params
        self._parse_param_lock.acquire()
        # The first thread to start is responsible for preparing all execution environment.
        if not self._ref_cnt:
            if 'PYTHONPATH' in params:
                self._old_python_path = sys.path[:]
                python_path_list = params['PYTHONPATH'].split(':')
                python_path_list.reverse()
                for path in python_path_list:
                    sys.path.insert(0, path)
            if '_PYTHON_WORKING_DIR' in params:
                self._old_working_dir = os.getcwd()
                os.chdir(params['_PYTHON_WORKING_DIR'])
            os.environ.update(params)
        self._ref_cnt += 1
        self._parse_param_lock.release()

        # read job information from provision stub
        metadata = [("worker_id", start_worker_request.worker_id)]
        provision_endpoint = start_worker_request.provision_endpoint.url
        with grpc.insecure_channel(provision_endpoint) as channel:
            client = ProvisionServiceStub(channel=channel)
            info = client.GetProvisionInfo(GetProvisionInfoRequest(), metadata=metadata).info
            options = json_format.MessageToJson(info.pipeline_options)
            logging_endpoint = info.logging_endpoint.url
            control_endpoint = info.control_endpoint.url

        try:
            logging_service_descriptor = endpoints_pb2.ApiServiceDescriptor(url=logging_endpoint)

            # Send all logs to the runner.
            fn_log_handler = FnApiLogRecordHandler(logging_service_descriptor)
            logging.getLogger().setLevel(logging.INFO)
            # Remove all the built-in log handles
            logging.getLogger().handlers = []
            logging.getLogger().addHandler(fn_log_handler)
            logging.info("Starting up Python worker in loopback mode.")
        except Exception:
            _LOGGER.error(
                "Failed to set up logging handler, continuing without.",
                exc_info=True)
            fn_log_handler = None

        sdk_pipeline_options = sdk_worker_main._parse_pipeline_options(options)

        _worker_id = start_worker_request.worker_id

        try:
            control_service_descriptor = endpoints_pb2.ApiServiceDescriptor(url=control_endpoint)
            status_service_descriptor = endpoints_pb2.ApiServiceDescriptor()

            experiments = sdk_pipeline_options.view_as(DebugOptions).experiments or []
            enable_heap_dump = 'enable_heap_dump' in experiments
            SdkHarness(
                control_address=control_service_descriptor.url,
                status_address=status_service_descriptor.url,
                worker_id=_worker_id,
                state_cache_size=sdk_worker_main._get_state_cache_size(experiments),
                data_buffer_time_limit_ms=sdk_worker_main._get_data_buffer_time_limit_ms(
                    experiments),
                profiler_factory=profiler.Profile.factory_from_options(
                    sdk_pipeline_options.view_as(ProfilingOptions)),
                enable_heap_dump=enable_heap_dump).run()
        except:  # pylint: disable=broad-except
            _LOGGER.exception('Python sdk harness failed: ')
            raise
        finally:
            self._parse_param_lock.acquire()
            self._ref_cnt -= 1
            # The last thread to exit is responsible for reverting working directory and sys.path.
            if self._ref_cnt == 0:
                if self._old_python_path is not None:
                    sys.path.clear()
                    for item in self._old_python_path:
                        sys.path.append(item)
                    self._old_python_path = None
                if self._old_working_dir is not None:
                    os.chdir(self._old_working_dir)
                    self._old_working_dir = None
            self._parse_param_lock.release()
            if fn_log_handler:
                fn_log_handler.close()
