#ifndef MAPPER_SERVICE_CLIENT_H
#define MAPPER_SERVICE_CLIENT_H

#include <string>
#include <memory>
#include <grpcpp/grpcpp.h>
#include "../mapper.grpc.pb.h"
#include "../util/mapreduce_util.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using mapper::Mapper;
using mapper::MapperRequest;
using mapper::MapperReply;

class MapperServiceClient {
    public:
        MapperServiceClient(std::shared_ptr<Channel> channel) : stub_(Mapper::NewStub(channel)) {}
        int32_t start_mapper_rpc(map_reduce* mr, const char *chunk , uint32_t start_line, int id, const std::string& address);

    private:
        std::unique_ptr<Mapper::Stub> stub_;
};

// Global function that can be called from mapreduce.cc
int32_t start_mapper_rpc(map_reduce* mr, const char *chunk , uint32_t start_line, int id, const std::string& address);


#endif // MAPPER_SERVICE_CLIENT_H
