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
	$(LD) -shared -soname libzlibcompression.so.1 -o libzlibcompression.so.1.0 SnappyCompressor.o SnappyDecompressor.o adler32.o compress.o crc32.o deflate.o gzclose.o gzlib.o gzread.o gzwrite.o infback.o inffast.o inflate.o inftrees.o trees.o uncompr.o zutil.o

zlib:
	$(CC) -O2 -Wall -fPIC -g -c zlib/adler32.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/compress.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/crc32.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/deflate.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/gzclose.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/gzlib.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/gzread.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/gzwrite.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/uncompr.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/trees.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/zutil.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/inflate.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/infback.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/inftrees.c
	$(CC) -O2 -Wall -fPIC -g -c zlib/inffast.c


clean:
	rm -rf *.so
	rm -rf *.o
	rm -rf *.so.*
