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

.PHONY: clean lzma

all: lzma

	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c LzmaCompressor.c
	$(CC) -Wall -fPIC -g -I $(JAVAINCLUDES) -I $(JAVAINCLUDES)/linux -c LzmaDecompressor.c
	$(LD) -shared -soname liblzmacompression.so.1 -o liblzmacompression.so.1.0 LzmaCompressor.o LzmaDecompressor.o LzmaDec.o LzmaEnc.o LzFind.o LzmaLib.o Alloc.o

lzma:
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzmaDec.c
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzmaEnc.c
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzFind.c
	#g++ -O2 -Wall -fPIC -g -I /opt/jdk1.6.0_18/include -I /opt/jdk1.6.0_18/include/linux -c -D_7ZIP_ST lzma/LzFindMt.c 
	$(CC) -O2 -Wall -fPIC -g -c lzma/LzmaLib.c
	$(CC) -O2 -Wall -fPIC -g -c lzma/Alloc.c

clean:
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*

