/* Header includes */
#include "mapper.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <cstdlib> 
#include "../mapreduce/mapreduce.h"
#include "../util/mapreduce_util.h"
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


static void* worker_wrapper(void* worker) {
    // turn the argument to mapper_struct type
    worker_struct *worker_exec = (worker_struct*) worker;
    printf("mapper(%d) start map_wrapper\n", worker_exec->worker_id);

    // tokenize the chunk
    uint32_t numLineTokenized;
    std::vector<kvpair> tokens = tokenization(worker_exec->chunk_content, worker_exec->start_line, worker_exec->mapper_ptr->mr->helper_args, &numLineTokenized,
         worker_exec->mapper_ptr->mr->application);

    // Pass each token to the map function to map the key-value pairs
    for(int i = 0; i < tokens.size(); i++) {
        worker_exec->mapper_ptr->mr->map_fnc(worker_exec->mapper_ptr->mr, &tokens[i]);
    }
    // tell the sender this worker thread has finished
    // and tell the sender we put something inside the buffer to flush
    worker_exec->worker_finished = 1;
    worker_exec->mapper_ptr->worker_func_status[worker_exec->worker_id] = 1;
    // pthread_cond_signal(&worker_exec->mapper_ptr->mr->not_empty);
    return NULL;
}


Status MapperServiceImplementation::start_mapper(
    ServerContext* context, 
    const MapperRequest* request, 
    MapperReply* reply
) {
    // get the arguments from the request
    char *app = (char*) request->application().c_str();
    int n_threads = request->threads();
    char *chunk = (char*) request->chunk().c_str();
    uint32_t start_line = std::stoi(request->startline());
    char *args = (char*) request->args().c_str();
    int id_num = request->id();

    printf("mapper(%d) rpc call to mapper\n", id_num);

    // initialize mr_init with arguments
    map_reduce *mr = mr_init(app, n_threads, "", "", args);
    mr->file_chunks.push_back(chunk);
    mr->chunk_start_lines.push_back(start_line);

    // start mapper service
    mapper_start_(mr, id_num);
    int result = mr->map_func_status[id_num];
    // return the result to the caller
    reply->set_result(result);
    // destroy map_reduce instance
    mr_destroy(mr);

    printf("mapper(%d) rpc call returned\n", id_num);
    return Status::OK;
}

int32_t MapperServiceImplementation::divide_chunk_into_worker_chunks(struct map_reduce *mr, int32_t id, 
                                                               std::vector<std::string> &worker_chunks, 
                                                               std::vector<uint32_t> &worker_start_lines) {
    // Get the chunk content from the mapper struct
    std::string chunk_content = mr->file_chunks[0];
    
    // Count lines in the chunk
    size_t chunk_lines = 0;
    size_t pos = 0;
    while ((pos = chunk_content.find('\n', pos)) != std::string::npos) {
        chunk_lines++;
        pos++;
    }
    if (!chunk_content.empty() && chunk_content.back() != '\n') {
        chunk_lines++; // Last line without newline
    }
    
    // Calculate lines per worker thread
    size_t lines_per_worker = chunk_lines / mr->numOfMapWorkerThreads;
    size_t remaining_lines = chunk_lines % mr->numOfMapWorkerThreads;
    
    // Divide chunk into sub-chunks
    size_t current_pos = 0;
    size_t current_line = mr->chunk_start_lines[0];
    
    for(int i = 0; i < mr->numOfMapWorkerThreads; i++) {
        printf("mapper(%d) entered for loop\n", id);
        size_t worker_lines = lines_per_worker;
        if (i < remaining_lines) {
            worker_lines++; // Distribute remaining lines to first few workers
            printf("mapper(%d) distributed remaining lines to first few workers\n", id);
        }
        printf("mapper(%d) pushed back current line\n", id);
        worker_start_lines.push_back(current_line);
        printf("mapper(%d) pushed back current line to worker start lines\n", id);
        // Find the end position for this worker's chunk
        size_t end_pos = current_pos;
        size_t lines_found = 0;
        
        while (end_pos < chunk_content.length() && lines_found < worker_lines) {
            printf("mapper(%d) found line\n", id);
            if (chunk_content[end_pos] == '\n') {
                lines_found++;
            }
            end_pos++;
        }
        
        // Extract worker chunk content
        std::string worker_chunk = chunk_content.substr(current_pos, end_pos - current_pos);
        worker_chunks.push_back(worker_chunk);
        
        current_pos = end_pos;
        current_line += worker_lines;
    }
    return 0;
}

int32_t MapperServiceImplementation::mapper_start_(map_reduce *mr, int32_t id) {
    printf("mapper(%d) start running\n", id);
    // create worker threads for each mapper
    pthread_t *worker_threads = (pthread_t*)malloc(mr->numOfMapWorkerThreads * sizeof(pthread_t));
    if(worker_threads == NULL) {
        perror("Failed to allocate memory for map threads.\n");
        return -1;
    }
    
    // Divide the chunk into sub-chunks for each worker thread
    std::vector<std::string> worker_chunks;
    std::vector<uint32_t> worker_start_lines;
    if(divide_chunk_into_worker_chunks(mr, id, worker_chunks, worker_start_lines) == -1) {
        perror("Failed to divide chunk into worker chunks.\n");
        return -1;
    }
    
    // Track worker thread completion
    int completed_workers = 0;

    mapper_struct *map_worker = mapper_init(mr, id);
    
    for(int i = 0; i < mr->numOfMapWorkerThreads; i++) {
        // initialize worker struct for each worker thread
        worker_struct *worker = worker_init(map_worker, i, (char*)worker_chunks[i].c_str(), worker_start_lines[i]);
        if(worker == NULL) {
            perror("Failed to initialize worker.\n");
            free(worker_threads);
            return -1;
        }
        // create worker thread for each worker
        if(pthread_create(&worker_threads[i], NULL, &worker_wrapper, (void *)worker) != 0) {
            perror("Failed to create map thread.\n");
            free(worker_threads);
            return -1;
        }
    }
    
    // sender thread (flush) - continue until all worker threads are done
    int32_t sender_return = 0;
    int finished_workers = 0;
    
    while(finished_workers < mr->numOfMapWorkerThreads) {
        // send the buffer to reducer
        sender_return = sender(map_worker, id);
        // Check how many workers have finished
        for(int i = 0; i < mr->numOfMapWorkerThreads; i++) {
            if(map_worker->worker_func_status[i] == 1) {
                finished_workers++;
            }
        }

        // if sender_return is 1, the reducer has received the buffer for this mapper
        if(sender_return == 1) {
            printf("mapper(%d) sender_return is 1, break\n", id);
            break;
        }

    }
 
    // wait for all mapper worker threads to finish
    for(int i = 0; i < mr->numOfMapWorkerThreads; i++) {
        pthread_join(worker_threads[i], NULL);
        completed_workers++;
    }
    free(worker_threads);
    
    // Now that all worker threads are done, set the mapper status to finished
    if(completed_workers == mr->numOfMapWorkerThreads) {
        mr->map_func_status[id] = 1;
        map_worker->finished = 1;
        printf("All %d worker threads completed for mapper %d\n", completed_workers, id);
    }
    // flush the remaining buffer to reducer
    while(sender_return == 0) {
        printf("mapper(%d) flush remaining buffer to reducer\n", id);
        sender_return = flush_rpc(mr, id, mr->reducer_address);
    }
    // destroy mapper_struct instance
    mapper_destroy(map_worker);
    printf("mapper(%d) finish\n", id);
    return 0;
}

int32_t MapperServiceImplementation::sender(mapper_struct* map_worker, int id) {
    // lock so that it can synchronize with producer
    pthread_mutex_lock(&map_worker->mr->lock);
    int32_t ret = 0;
    // if no content in the buffer
    while(map_worker->mr->used_size <= 0) {
        // if map_finished, we used flush+rpc to send finish message to reducer
        if(map_worker->finished) {
            // keep flushing the buffer until reducer receive finish
            while(ret == 0) {
                ret = flush_rpc(map_worker->mr, id, map_worker->mr->reducer_address);  
            }
            // send this signal so that producer will not be stuck
            pthread_cond_signal(&map_worker->mr->not_full);
            pthread_mutex_unlock(&map_worker->mr->lock);
            return ret;
        }
        // if not finish, wait for producer to produce something
        pthread_cond_wait(&map_worker->mr->not_empty, &map_worker->mr->lock);
    }
    
    // now there is something in the buffer
    // RPC call to reducer
    if(map_worker->mr->used_size >= map_worker->mr->map_buffer_size * MR_BUFFER_THRESHOLD) {
        ret = flush_rpc(map_worker->mr, id, map_worker->mr->reducer_address);
    }

    pthread_cond_signal(&map_worker->mr->not_full);
    pthread_mutex_unlock(&map_worker->mr->lock);
    printf("mapper(%d) sender unlock\n", id);
    return ret;
}

mapper_struct* MapperServiceImplementation::mapper_init(map_reduce *mr, int32_t id) {
    printf("mapper(%d) initializing mapper\n", id);
    // create new mapper_struct instance
    mapper_struct *map_worker = (mapper_struct*) malloc(sizeof(mapper_struct));
    printf("mapper_struct allocated\n");
    if(map_worker == 0) {
        free(map_worker);
        return NULL;
    }
    // initialize local buffer for mapper
    mr->map_buffer = (char*) malloc(mr->map_buffer_size * sizeof(char));
    mr->used_size = 0;

    // initialize mapper's status
    // initialize worker_func_status vector
    new (&map_worker->worker_func_status) std::vector<int>();
    for(int i = 0; i < mr->numOfMapWorkerThreads; i++) {
        map_worker->worker_func_status.push_back(-1);
    }
    // initialize mapper struct
    map_worker->id          = id;
    map_worker->mr          = mr;
    map_worker->finished    = 0;
    printf("mapper(%d) done with initialization mapper_init\n", id);
    return map_worker;
}

worker_struct* MapperServiceImplementation::worker_init(mapper_struct *map_worker, int32_t worker_id, char *chunk_content, uint32_t start_line) {
    printf("mapper(%d) initializing worker worker_init (%d)\n", map_worker->id, worker_id);
    worker_struct *worker = (worker_struct*) malloc(sizeof(worker_struct));
    if(worker == NULL) {
        free(worker);
        return NULL;
    }
    worker->worker_id       = worker_id;
    worker->mapper_ptr      = map_worker;
    worker->worker_finished = 0;
    worker->chunk_content = (char*)malloc(strlen(chunk_content) + 1);
    if(worker->chunk_content == NULL) {
        free(worker);
        return NULL;
    }
    strcpy(worker->chunk_content, chunk_content);
    worker->start_line = start_line;
    return worker;
}

void MapperServiceImplementation::mapper_destroy(mapper_struct *map_worker) {
    printf("mapper(%d) destroyed\n", map_worker->id);
    // Destroy C++ objects
    map_worker->worker_func_status.~vector();
    if (map_worker->mr->map_buffer) {
        free(map_worker->mr->map_buffer);
        map_worker->mr->map_buffer = NULL;
    }
    free(map_worker);
}

void MapperServiceImplementation::worker_destroy(worker_struct *worker) {
    printf("mapper(%d) worker(%d) destroyed\n", worker->mapper_ptr->id, worker->worker_id);
    free(worker->chunk_content);
    free(worker);
}
int32_t MapperServiceImplementation::flush_rpc(map_reduce *mr, int32_t id, std::string address) {
    ReducerServiceClient client(
        grpc::CreateChannel(
            address, 
            grpc::InsecureChannelCredentials()
        )
    );
    int32_t response;
    response = client.flush_rpc(id, mr->map_buffer, mr->used_size);
    mr->used_size = 0;
    return response;
}

int32_t main(int32_t argc, char** argv) {
    std::string address("0.0.0.0:5000");
    MapperServiceImplementation service;

    ServerBuilder builder;
    
    // Configure message size limits
    builder.AddChannelArgument(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 150 * 1024 * 1024);
    builder.AddChannelArgument(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 150 * 1024 * 1024);

    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on port: " << address << std::endl;
    
    server->Wait();
    return 0;
}

