# Set homebrew paths
# HOMEBREW_PREFIX = /opt/homebrew
# PKG_CONFIG_PATH = $(HOMEBREW_PREFIX)/lib/pkgconfig
# PATH := $(HOMEBREW_PREFIX)/bin:$(PATH)

#Conda

CONDA_PREFIX = /scratch/sqb6440/miniconda3/envs/mapreduce
PKG_CONFIG_PATH = $(CONDA_PREFIX)/lib/pkgconfig
PATH := $(CONDA_PREFIX)/bin:$(PATH)

#Hadoop HDFS
HADOOP_HOME = /scratch/sqb6440/hadoop
HDFS_BIN = $(HADOOP_HOME)/bin/hdfs

LDFLAGS = -L$(CONDA_PREFIX)/lib \
          -lprotobuf -lgrpc++ -lgrpc++_reflection -lgpr -labsl_synchronization \
          -ldl -lpthread

CXX = g++
CPPFLAGS += -I$(CONDA_PREFIX)/include -I. -DHADOOP_HOME="\"$(HADOOP_HOME)\"" -DHDFS_BIN="\"$(HDFS_BIN)\""
CXXFLAGS += -std=c++17 

GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

SRC1 = $(wildcard ./mapreduce/*.cpp)
SRC2 = $(wildcard ./mapper/*.cpp)
SRC3 = $(wildcard ./reducer/*.cpp)
SRC4 = $(wildcard ./grpcclient/*.cpp)
SRC5 = $(wildcard ./util/*.cpp)

MAPREDUCE_OBJ = $(patsubst %.cpp,%.o,$(SRC1))
MAPPER_OBJ = $(patsubst %.cpp,%.o,$(SRC2))
REDUCER_OBJ = $(patsubst %.cpp,%.o,$(SRC3))
GRPC_OBJ = $(patsubst %.cpp,%.o,$(SRC4))
COMMON_OBJ = $(patsubst %.cpp,%.o,$(SRC5))

all: ./build/wordc_mapper ./build/wordc_reducer ./build/grep_mapper ./build/grep_reducer ./build/wordc_client ./build/grep_client

./build/wordc_client: mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(COMMON_OBJ)
	mkdir -p ./build
	$(CXX) $^ bin/driver.o bin/mr_wordc.o bin/tokenizer.o $(LDFLAGS) -o $@

./build/wordc_mapper: mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o $(MAPPER_OBJ) $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(COMMON_OBJ)
	mkdir -p ./build
	$(CXX) $^ bin/mr_wordc.o bin/tokenizer.o $(LDFLAGS) -o $@

./build/wordc_reducer: mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o $(REDUCER_OBJ) $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(COMMON_OBJ)
	mkdir -p ./build
	$(CXX) $^ bin/mr_wordc.o bin/tokenizer.o $(LDFLAGS) -o $@

./build/grep_client: mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(COMMON_OBJ)
	mkdir -p ./build
	$(CXX) $^ bin/driver.o bin/mr_grep.o bin/tokenizer.o $(LDFLAGS) -o $@

./build/grep_mapper: mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o $(MAPPER_OBJ) $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(COMMON_OBJ)
	mkdir -p ./build
	$(CXX) $^ bin/mr_grep.o bin/tokenizer.o $(LDFLAGS) -o $@

./build/grep_reducer: mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o $(REDUCER_OBJ) $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(COMMON_OBJ)
	mkdir -p ./build
	$(CXX) $^ bin/mr_grep.o bin/tokenizer.o $(LDFLAGS) -o $@


%.grpc.pb.cc: %.proto
	protoc --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

%.pb.cc: %.proto
	protoc --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h ./build/wordc_client ./build/wordc_mapper ./build/wordc_reducer \
	                         ./build/grep_client ./build/grep_mapper ./build/grep_reducer \
							 $(MAPREDUCE_OBJ) $(GRPC_OBJ) $(MAPPER_OBJ) $(REDUCER_OBJ) $(COMMON_OBJ) \
							 driver.o mapper.pb.o mapper.grpc.pb.o reducer.pb.o reducer.grpc.pb.o

# Object file targets
%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@