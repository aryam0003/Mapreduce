#ifndef REDUCER_SERVICE_CLIENT_H
#define REDUCER_SERVICE_CLIENT_H

#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "../reducer.grpc.pb.h"
#include "../util/mapreduce_util.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using reducer::Reducer;
using reducer::ReducerServiceStartRequest;
using reducer::ReducerServiceStartReply;
using reducer::ReducerServiceFlushRequest;
using reducer::ReducerServiceFlushReply;

class ReducerServiceClient {
    public:
        ReducerServiceClient(std::shared_ptr<Channel> channel) : stub_(Reducer::NewStub(channel)) {}
        int32_t start_reducer(map_reduce* mr, const char* outpath, int32_t id);
        int32_t flush_rpc(int32_t id, char* buffer, int32_t size);

    private:
        std::unique_ptr<Reducer::Stub> stub_;
};

// Global function that can be called from other services
int32_t start_reducer_rpc(map_reduce* mr, char* outpath, int32_t id, const std::string& address);
int32_t flush_rpc(int32_t id, char* buffer, int32_t size, const std::string& address);


#endif // REDUCER_SERVICE_CLIENT_H
