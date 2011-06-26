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

.PHONY: clean snappy

all: snappy
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c SnappyCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c SnappyDecompressor.c
	$(LD) -shared -soname libsnappycompression.so.1 -o libsnappycompression.so.1.0 SnappyCompressor.o SnappyDecompressor.o snappy/.libs/snappy-c.o snappy/.libs/snappy.o

snappy:
	#./build-snappy.sh
	(cd snappy && ./configure && make && cd ..)

clean:
	(cd snappy && make clean && cd ..)
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*
