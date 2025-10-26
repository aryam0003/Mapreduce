#include "reducer_service_client.h"
#include <cstdint>
#include <iostream>
#include <string>
#include <inttypes.h>
#include <grpcpp/grpcpp.h>
#include "../reducer.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using reducer::Reducer;
using reducer::ReducerServiceStartRequest;
using reducer::ReducerServiceStartReply;
using reducer::ReducerServiceFlushRequest;
using reducer::ReducerServiceFlushReply;

int ReducerServiceClient::start_reducer(map_reduce* mr, const char* outpath, int32_t id) {
    ReducerServiceStartRequest request;
    ReducerServiceStartReply reply;
    ClientContext context;

    //prepare the request
    request.set_application(mr->application);
    request.set_threads(mr->numOfMappers); 
    request.set_outpath(outpath);
    // request.set_args(mr->helper_args);
    request.set_barrier(mr->barrierEnable);
    request.set_id(id);

    Status status = stub_->start_reducer(&context, request, &reply);

    if(status.ok()){
        return reply.result();
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return -1;
    }

}

int ReducerServiceClient::flush_rpc(int id, char* buffer, int size) {
    ReducerServiceFlushRequest request;
    ReducerServiceFlushReply reply;
    ClientContext context;

    request.set_id(id);
    request.set_buffer(buffer, size);
    request.set_size(size);

    Status status = stub_->flush(&context, request, &reply);

    if(status.ok()){
        return reply.successful();
    } else {
        return -1;
    }
}

int32_t start_reducer_rpc(struct map_reduce *mr, char* outpath, int32_t id, const std::string& address) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(150 * 1024 * 1024); // 150MB
    args.SetMaxSendMessageSize(150 * 1024 * 1024);    // 150MB
    
    ReducerServiceClient client(
        grpc::CreateCustomChannel(
            address, 
            grpc::InsecureChannelCredentials(),
            args
        )
    );

    int32_t response;
    response = client.start_reducer(mr, outpath, id);
    return response;
}

int32_t flush_rpc(int32_t id, char* buffer, int32_t size, const std::string& address) {
    //PRINT THE SIZE
    printf("size: %d\n", size);
    
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(150 * 1024 * 1024); // 150MB
    args.SetMaxSendMessageSize(150 * 1024 * 1024);    // 150MB
    
    ReducerServiceClient client(
        grpc::CreateCustomChannel(
            address, 
            grpc::InsecureChannelCredentials(),
            args
        )
    );
    int32_t response = client.flush_rpc(id, buffer, size);
    return response;
}