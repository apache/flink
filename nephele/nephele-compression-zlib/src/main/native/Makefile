#Makefile for compression

TARGET_DIR      := ../../../target
JAVA_BUILD_DIR 	= $(TARGET_DIR)/classes
TARGET          = $(JAVA_BUILD_DIR)/META-INF/lib

.PHONY: zlib dir clean

all: dir zlib clean

dir:
	mkdir -p $(TARGET)

zlib:
	make -C zlib
	cp zlib/*.so* $(TARGET)

clean:
	make -C zlib clean
