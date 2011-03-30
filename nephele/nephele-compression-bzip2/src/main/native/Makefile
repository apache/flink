#Makefile for compression

TARGET_DIR      := ../../../target
JAVA_BUILD_DIR 	= $(TARGET_DIR)/classes
TARGET          = $(JAVA_BUILD_DIR)/META-INF/lib

.PHONY: bzip2 dir clean

all: dir bzip2 clean

dir:
	mkdir -p $(TARGET)

bzip2:
	make -C bzip2
	cp bzip2/*.so* $(TARGET)

clean:
	make -C bzip2 clean
