#ifndef MAPPER_H
#define MAPPER_H

#include <vector>
#include <string>

#include "../mapreduce/mapreduce.h"
// #include "../tools/util.h"  // This file doesn't exist - tokenization is in mapreduce.h
#include "../grpcclient/reducer_service_client.h"

#include <grpcpp/grpcpp.h>
#include "../mapper.grpc.pb.h"
#include "../reducer.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using mapper::Mapper;
using mapper::MapperRequest;
using mapper::MapperReply;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using reducer::Reducer;
using reducer::ReducerServiceStartRequest;
using reducer::ReducerServiceStartReply;
using reducer::ReducerServiceFlushRequest;
using reducer::ReducerServiceFlushReply;


struct mapper_struct {
    int id;
    map_reduce *mr;
    int finished;
    std::vector<int> worker_func_status; // status of each worker function
};

struct worker_struct {
    int worker_id;
    mapper_struct *mapper_ptr;
    int worker_finished;
    char *chunk_content;  // Sub-chunk content for this worker thread
    uint32_t start_line;  // Starting line number for this worker's chunk
};


class MapperServiceImplementation final : public Mapper::Service {
public:
    Status start_mapper(
        ServerContext* context, 
        const MapperRequest* request, 
        MapperReply* reply
    ) override;

private:
    int32_t mapper_start_(struct map_reduce *mr, int32_t id);

    int32_t sender(mapper_struct* map_worker, int id);

    mapper_struct* mapper_init(struct map_reduce *mr, int32_t id);
    worker_struct* worker_init(struct mapper_struct *map_worker, int32_t worker_id, char *chunk_content, uint32_t start_line);
    void mapper_destroy(struct mapper_struct *map_worker);
    void worker_destroy(struct worker_struct *worker);
    int32_t flush_rpc(struct map_reduce *mr, int32_t id, std::string address);
    
    int32_t divide_chunk_into_worker_chunks(struct map_reduce *mr, int32_t id, 
                                       std::vector<std::string> &worker_chunks, 
                                       std::vector<uint32_t> &worker_start_lines);
};

#endif