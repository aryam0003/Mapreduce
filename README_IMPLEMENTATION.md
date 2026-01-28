# MapReduce Framework Implementation

This is a complete implementation of a MapReduce framework with gRPC communication between mappers and reducers.

## Features Implemented

### 1. **mr_init Function**
- Allocates and initializes MapReduce framework instance
- Loads configuration from `config` file
- Sets up mapper and reducer addresses
- Initializes function pointers for map and reduce operations

### 2. **Configuration System**
- Reads from `config` file with format:
  ```
  MAPPER 4 mapper:5000
  REDUCER reducer:5001
  ```
- Supports multiple mappers and reducers
- Configurable port assignments

### 3. **gRPC Communication**
- **Mapper Service Client**: Implements `mapper_start_rpc` function
- **Reducer Service Client**: Implements `reducer_start_rpc` function
- Protocol buffer definitions in `mapper.proto` and `reducer.proto`
- Proper error handling and timeout management

### 4. **Threading Support**
- Multi-threaded mapper execution
- Reducer thread management
- Thread-safe communication between components

## File Structure

```
├── mapreduce/
│   ├── mapreduce.h          # Main framework header
│   └── mapreduce.cc         # Framework implementation
├── common/
│   ├── mapreduce_util.h     # Utility functions and structures
│   └── mapreduce_util.cc    # Utility implementations
├── grpcclient/
│   ├── mapper_service_client.h   # Mapper gRPC client header
│   ├── mapper_service_client.cc  # Mapper gRPC client implementation
│   ├── reducer_service_client.h  # Reducer gRPC client header
│   └── reducer_service_client.cc # Reducer gRPC client implementation
├── mapper.proto             # Mapper service definition
├── reducer.proto            # Reducer service definition
├── config                   # Configuration file
├── Makefile                 # Build configuration
└── build.sh                 # Build script
```

## Building the Framework

### Prerequisites
```bash
# Install Protocol Buffers
sudo apt-get install protobuf-compiler

# Install gRPC (if not already installed)
# Follow instructions at: https://grpc.io/docs/languages/cpp/quickstart/
```

### Build Commands
```bash
# Option 1: Use the build script
./build.sh

# Option 2: Use Makefile
make all

# Option 3: Manual compilation
protoc --cpp_out=. mapper.proto
protoc --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` mapper.proto
g++ -std=c++17 -I. -I/usr/local/include \
    mapreduce/mapreduce.cc \
    common/mapreduce_util.cc \
    grpcclient/mapper_service_client.cc \
    grpcclient/reducer_service_client.cc \
    mapper.pb.cc mapper.grpc.pb.cc \
    -lgrpc++ -lgrpc -lprotobuf -lpthread \
    -o mapreduce_framework
```

## Usage

### 1. Configure the Framework
Edit the `config` file:
```
MAPPER 4 mapper:5000
REDUCER reducer:5001
```

### 2. Initialize the Framework
```cpp
struct map_reduce *mr = mr_init("wordc", 4, "input.txt", "output.txt", "delimiters");
if (mr == NULL) {
    printf("Failed to initialize MapReduce framework\n");
    return -1;
}
```

### 3. Start MapReduce Operations
```cpp
int result = mr_start(mr, 1);  // 1 = enable barrier
if (result != 0) {
    printf("Failed to start MapReduce operations\n");
    return -1;
}
```

### 4. Wait for Completion
```cpp
int final_result = mr_finish(mr);
if (final_result == 0) {
    printf("MapReduce operations completed successfully\n");
} else {
    printf("MapReduce operations failed\n");
}
```

### 5. Cleanup
```cpp
mr_destroy(mr);
```

## gRPC Service Definitions

### Mapper Service (mapper.proto)
```protobuf
service Mapper {
    rpc mapper_start (MapperRequest) returns (MapperReply) {}
}

message MapperRequest {
    string application = 1;
    int32 threads = 2;
    string inpath = 3;
    string args = 4;
    int32 id = 5;
}

message MapperReply {
    int32 id = 1;
    int32 result = 2;
}
```

### Reducer Service (reducer.proto)
```protobuf
service Reducer {
    rpc reducer_start (ReducerRequest) returns (ReducerReply) {}
}

message ReducerRequest {
    string application = 1;
    string outpath = 2;
    int32 id = 3;
}

message ReducerReply {
    int32 id = 1;
    int32 result = 2;
}
```

## Key Implementation Details

### 1. **Memory Management**
- Proper allocation and deallocation of resources
- Error handling for memory allocation failures
- Cleanup in `mr_destroy` function

### 2. **Error Handling**
- Comprehensive error checking throughout the code
- Graceful failure handling with proper cleanup
- Detailed error messages for debugging

### 3. **Thread Safety**
- Thread-safe communication between mappers and reducers
- Proper synchronization mechanisms
- Barrier implementation for reducer coordination

### 4. **Configuration Management**
- Flexible configuration system
- Support for multiple mappers and reducers
- Easy port and address configuration

## Testing

The framework can be tested with the provided test cases:
```bash
# Run word count test
./mapreduce_framework wordc test/mr_wordc/Testcase1/input1 test/mr_wordc/Testcase1/output1.txt " "

# Run grep test
./mapreduce_framework grep test/mr_grep/Testcase1/input1 test/mr_grep/Testcase1/output1.txt "search_pattern"
```

## Future Enhancements

1. **HDFS Integration**: Complete HDFS file system implementation
2. **Load Balancing**: Dynamic load balancing across mappers
3. **Fault Tolerance**: Handle mapper/reducer failures
4. **Monitoring**: Add performance monitoring and metrics
5. **Security**: Add authentication and encryption for gRPC calls
