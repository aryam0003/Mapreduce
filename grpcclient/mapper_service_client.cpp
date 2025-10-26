#include "mapper_service_client.h"
#include <iostream>
#include <memory>
#include <string>

using mapper::Mapper;
using mapper::MapperRequest;
using mapper::MapperReply;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;


int32_t MapperServiceClient::start_mapper_rpc(map_reduce* mr, const char *chunk , uint32_t start_line, int id, const std::string& address) {
    
    // Prepare the request
    MapperRequest request;
    request.set_application(mr->application);
    request.set_threads(mr->numOfMapWorkerThreads);
    request.set_chunk(chunk);
    request.set_startline(std::to_string(start_line));
    request.set_args(mr->helper_args);
    request.set_id(id);
    
    // Prepare the reply
    MapperReply reply;
    ClientContext context;
    
    
    // Make the RPC call
    Status status = stub_->start_mapper(&context, request, &reply);
    
    // Check the status and return appropriate result
    if (status.ok()) {
        std::cout << "Mapper " << id << " RPC call successful. Result: " << reply.result() << std::endl;
        return  reply.result();
    } else {
        std::cout << "Mapper " << id << " RPC failed: " << status.error_code() 
                  << ": " << status.error_message() << std::endl;
        return -1;
    }
}

// Global function that can be called from mapreduce.cc
int32_t start_mapper_rpc(map_reduce* mr, const char *chunk , uint32_t start_line, int id, const std::string& address) {

    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(150 * 1024 * 1024); // 150MB
    args.SetMaxSendMessageSize(150 * 1024 * 1024);    // 150MB

    // Create a client for the mapper service
    MapperServiceClient client(
        grpc::CreateCustomChannel(
            address, 
            grpc::InsecureChannelCredentials(),
            args
        )
    );

    return client.start_mapper_rpc(mr, chunk, start_line, id, address);
}