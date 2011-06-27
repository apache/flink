#######################################################################################################################
#
# Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
#
#######################################################################################################################

JAVAINCLUDES = $(JAVA_HOME)/include
GPP = /usr/bin/g++

.PHONY: clean snappy

all: snappy
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -L snappy/.libs/ -c SnappyCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -L snappy/.libs/ -c SnappyDecompressor.c
	$(GPP) -Wall -fPIC -g -shared -static-libgcc -static-libstdc++ -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux snappy.o snappy-c.o snappy-sinksource.o snappy-stubs-internal.o SnappyCompressor.o SnappyDecompressor.o -o libsnappycompression.so.1.0

snappy:
	$(GPP) -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -O2 -fPIC -fvisibility=hidden -m64 -c snappy/snappy-sinksource.cc -o snappy-sinksource.o
	$(GPP) -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -O2 -fPIC -fvisibility=hidden -m64 -c snappy/snappy.cc -o snappy.o
	$(GPP) -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -O2 -fPIC -fvisibility=hidden -m64 -c snappy/snappy-c.cc -o snappy-c.o
	$(GPP) -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -O2 -fPIC -fvisibility=hidden -m64 -c snappy/snappy-stubs-internal.cc -o snappy-stubs-internal.o


clean:
	(cd snappy && make clean && cd ..)
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*
