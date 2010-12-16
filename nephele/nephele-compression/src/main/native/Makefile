#Makefile for compression

TARGET_DIR      := ../../../target
JAVA_BUILD_DIR 	= $(TARGET_DIR)/classes
TARGET          = $(JAVA_BUILD_DIR)/META-INF/lib

.PHONY: bzip2 lzma lzo quicklz zlib dir clean

all: dir bzip2 lzma lzo quicklz zlib clean

dir:
	mkdir -p $(TARGET)

bzip2:
	make -C bzip2
	cp bzip2/*.so* $(TARGET)
lzma:
	make -C lzma
	cp lzma/*.so* $(TARGET)
lzo:
	make -C lzo
	cp lzo/*.so* $(TARGET)

quicklz:
	make -C quicklz
	cp quicklz/*.so* $(TARGET)

zlib:
	make -C zlib
	cp zlib/*.so* $(TARGET)

clean:
	make -C bzip2 clean
	make -C lzma clean
	make -C lzo clean
	make -C quicklz clean
	make -C zlib clean
