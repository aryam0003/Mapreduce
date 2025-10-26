#ifndef REDUCER_H
#define REDUCER_H

/* Header includes */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <fcntl.h>
#include <iostream>
#include "../mapreduce/mapreduce.h"
#include "../reducer.grpc.pb.h"

#include <grpcpp/grpcpp.h>
#include "../reducer.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using reducer::Reducer;
using reducer::ReducerServiceStartRequest;
using reducer::ReducerServiceStartReply;
using reducer::ReducerServiceFlushRequest;
using reducer::ReducerServiceFlushReply;

#define REDUCE_BUFFER_SIZE 1024000

struct reducer_struct {
    int id;
    struct map_reduce *mr;
    int barrier;
    char* outpath;
    int (*reduce_fnc)(struct map_reduce*, struct kvpair*, size_t num);
};

class ReducerServiceImplementation final : public Reducer::Service {

    Status start_reducer(
        ServerContext* context, 
        const ReducerServiceStartRequest* request, 
        ReducerServiceStartReply* reply
    ) override;

    Status flush(
        ServerContext* context, 
        const ReducerServiceFlushRequest* request, 
        ReducerServiceFlushReply* reply
    ) override;

    private:
        int32_t start_reducer_(struct map_reduce *mr, int id);
        int32_t receiver(struct map_reduce *mr, std::string buffer, int size, int id);
        reducer_struct* reducer_init(struct map_reduce *mr, int id);
        void reducer_destroy(struct reducer_struct *reduce_worker);
        struct map_reduce* mr;
};


#endif




