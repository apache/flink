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


#
# This file serves a basic HTTP server to act as an artifact caching proxy for
# E2E test artifacts.
#

import http.server
import socketserver
import urllib.request
import shutil
import os
import hashlib
import signal
import argparse

CACHE_FOLDER = "./.cache/"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Local file cache proxy for e2e artifacts"
    )
    parser.add_argument("-p", "--port", default=7654, type=int)
    parser.add_argument("-d", "--cache-dir", default=".artifact_cache")
    return parser.parse_args()


def gracefull_kill(sig, stack):
    httpd.server_close()
    exit()


class CacheHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Trim leading '/' from path
        path = self.path[1:]
        hashed_path = (
            CACHE_FOLDER
            + "/"
            + hashlib.md5(path.encode("utf-8")).hexdigest()
            + ".cache"
        )

        if not os.path.exists(hashed_path):
            print(path + " not present in cache. Retrieving from source...")
            request = urllib.request.Request(path)
            try:
                response = urllib.request.urlopen(request)
                with open(hashed_path, "wb") as out:
                    shutil.copyfileobj(response, out, length=16 * 1024)

            except urllib.error.HTTPError as e:
                self.send_response(e.code)
                self.end_headers()
                return None
        else:
            print(path + " present in cache.")

        with open(hashed_path, "rb") as cached:
            self.send_response(200)
            self.end_headers()
            shutil.copyfileobj(cached, self.wfile)


args = parse_args()

CACHE_FOLDER = args.cache_dir

if not os.path.exists(CACHE_FOLDER):
    os.mkdir(CACHE_FOLDER)

for s in [signal.SIGINT, signal.SIGTERM]:
    signal.signal(s, gracefull_kill)

socketserver.TCPServer.allow_reuse_address = True
httpd = socketserver.TCPServer(("", args.port), CacheHandler)

httpd.serve_forever()
