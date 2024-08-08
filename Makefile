# SRC_DIR = src
# BUILD_DIR = build/debug
# CC = g++-12 # the built-in g++ doesn't work for some reason, but what the heck i don't care..
# SRC_FILES = $(wildcard $(SRC_DIR)/*.cpp)

# OBJ_NAME = foo
# INCLUDE_PATHS = -Iinclude -I/opt/homebrew/include/ -I/opt/homebrew/include/SDL2 # second one is because SDL2_image has "#include "SDL2.h"", which needs SDL2.h to be directly within touch
# LIBRARY_PATHS = -L/opt/homebrew/lib -lavformat -lavcodec -lavutil -lavdevice -lsdl2 -lswscale
# # LIBRARY_PATHS = -L/opt/homebrew/Cellar/sdl2/2.0.22/lib/ -L/opt/homebrew/Cellar/sdl2_image/2.0.5/lib
# COMPILER_FLAG = -std=c++11 -Wall -O0 -g -v
# # LINKER_FLAGS = -lsdl2

# all:
# 	$(CC) $(COMPILER_FLAG) $(LINKER_FLAGS) $(INCLUDE_PATHS) $(LIBRARY_PATHS) $(SRC_FILES) -o $(BUILD_DIR)/$(OBJ_NAME)
	
# run:
# 	$(CC) $(COMPILER_FLAG) $(LINKER_FLAGS) $(INCLUDE_PATHS) $(LIBRARY_PATHS) $(SRC_FILES) -o $(BUILD_DIR)/$(OBJ_NAME)
# 	./build/debug/foo	# lets execute it


SRC_DIR=.
BUILD_DIR = build/debug
CC = gcc # the built-in g++ doesn't work for some reason, but what the heck i don't care..
# SRC_FILES = $(wildcard $(SRC_DIR)/*.c)
SRC_FILES = $(wildcard *.c)
$(info SRC_FILES=$(SRC_FILES))


OBJ_NAME = db
# LIBRARY_PATHS = -L/opt/homebrew/Cellar/sdl2/2.0.22/lib/ -L/opt/homebrew/Cellar/sdl2_image/2.0.5/lib
# COMPILER_FLAG = -std=c11 -Wall -O0 -g -v
# LINKER_FLAGS = -lsdl2

all:
	$(CC) $(COMPILER_FLAG) $(LINKER_FLAGS) $(SRC_FILES) -o $(BUILD_DIR)/$(OBJ_NAME)
	
run:
	$(CC) $(COMPILER_FLAG) $(LIBRARY_PATHS) $(SRC_FILES) -o $(BUILD_DIR)/$(OBJ_NAME)
	$(BUILD_DIR)/$(OBJ_NAME)	# lets execute it






#  makefile re-structuring
#  https://stackoverflow.com/questions/30573481/how-to-write-a-makefile-with-separate-source-and-header-directories
SRC_DIR := src
BIN_DIR := bin


