#include "reducer.h"
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
#include <grpcpp/grpcpp.h>

#include "../mapreduce/mapreduce.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using reducer::Reducer;
using reducer::ReducerServiceStartRequest;
using reducer::ReducerServiceStartReply;
using reducer::ReducerServiceFlushRequest;
using reducer::ReducerServiceFlushReply;

// #define REDUCE_BUFFER_SIZE 1024000

// check if all mapper has finished
static int32_t check_mapper_func_status(struct map_reduce *mr) {
    for(int i = 0; i < mr->numOfMappers; i++) {

        if(mr->map_func_status[i] != 1){ 
            return 0;
        } 

    }
    return 1;
}


reducer_struct* ReducerServiceImplementation::reducer_init(struct map_reduce *mr, int id) {
    
    // Create new reducer_struct instance
    reducer_struct *reduce_worker = (reducer_struct *)malloc(sizeof(reducer_struct));
    if (reduce_worker == NULL) {
        printf("Error: Failed to allocate memory for reducer_struct\n");
        return NULL;
    }
    
    // Initialize reducer_struct fields
    reduce_worker->id = id;
    reduce_worker->mr = mr;
    reduce_worker->barrier = mr->barrierEnable;
    
    // Allocate and copy output path
    reduce_worker->outpath = (char *)malloc(strlen(mr->outpath) + 1);
    if (reduce_worker->outpath == NULL) {
        printf("Error: Failed to allocate memory for output path\n");
        free(reduce_worker);
        return NULL;
    }
    strcpy(reduce_worker->outpath, mr->outpath);
    
    // Set the reduce function pointer
    reduce_worker->reduce_fnc = mr->reduce_fnc;
    
    // Allocate reduce buffer
    mr->reduce_buffer = (char *)malloc(mr->reduce_buffer_size * sizeof(char));
    if (mr->reduce_buffer == NULL) {
        printf("Error: Failed to allocate memory for reduce buffer\n");
        free(reduce_worker->outpath);
        free(reduce_worker);
        return NULL;
    }
    
    // Initialize map function status array
    // Initialize all mapper statuses to -1 (not completed)
    for (int i = 0; i < mr->numOfMappers; i++) {
        mr->map_func_status.push_back(0);
    }
    
    // Initialize reducer function status
    mr->reduce_func_status = -1;
    
    printf("Reducer %d initialized successfully\n", id);
    
    return reduce_worker;
}

void ReducerServiceImplementation::reducer_destroy(struct reducer_struct *reduce_worker) {
    // free the reducer worker's memory
    free(reduce_worker->outpath);
    free(reduce_worker);
}

int32_t ReducerServiceImplementation::receiver(struct map_reduce *mrstruct, std::string buffer, int size, int id) {

    pthread_mutex_lock(&mrstruct->lock);
    int ret = 0;
    // if size of received buffer = 0, the map function sending a finish signal.
    if(size == 0) {
        mrstruct->map_func_status[id] = 1;
        ret = 1;
        // signal reducer all mappers has finished
        if(check_mapper_func_status(mrstruct) != 0) {
            mrstruct->receiver_finished = 1;
            pthread_cond_signal(&mrstruct->map_finished);
            pthread_cond_signal(&mrstruct->not_empty);
            pthread_mutex_unlock(&mrstruct->lock);
            return ret;
        }
    }
    // otherwise, move the received buffer to reduce_buffer.
    else {
        // First check if the buffer is overflow
        // if it is full, wait for consumer to send signal
        // if size > reduce_buffer_size threshold print it
        if(size > mrstruct->reduce_buffer_size) {
            printf("buffer is overflow, size: %d, reduce_buffer_size: %d, buffer threshold: %d\n", size, mrstruct->reduce_buffer_size, MR_BUFFER_THRESHOLD);
        }
        while((mrstruct->used_size + size) >= mrstruct->reduce_buffer_size) {

            printf("buffer is full, wait for consumer to send signal\n");
            struct kvpair *kvset;
            size_t num; 
            mr_consume(mrstruct, kvset, &num, mrstruct->barrierEnable);
            
            pthread_cond_wait(&mrstruct->not_full, &mrstruct->lock);
        }
        // move the content from received buffer to reduce buffer
        memmove(&mrstruct->reduce_buffer[mrstruct->used_size], buffer.c_str(), size);
        mrstruct->used_size += size;
    }
    // tell the reducer thread the buffer is not empty (consumer)
    pthread_cond_signal(&mrstruct->not_empty);
    printf("flush successful\n");
    pthread_mutex_unlock(&mrstruct->lock);
    return ret;

}

int32_t ReducerServiceImplementation::start_reducer_(struct map_reduce *mr, int id) {
    struct reducer_struct *reduce_worker = reducer_init(mr, id);
    struct kvpair *kvset;
    size_t num;   
    // check_mapper_func_status return 1 if all the mapper
    // send a 0 to reducer
    // keep consuming until all mapper finish working
    while(!check_mapper_func_status(reduce_worker->mr)) {
    
        while(reduce_worker->mr->used_size == 0 && reduce_worker->mr->receiver_finished != 1) {
            usleep(1000);
        }
        mr_consume(reduce_worker->mr, kvset, &num, reduce_worker->barrier); 
    }
    mr_consume(reduce_worker->mr, kvset, &num, 0); 
    // give num 0 to tell application the end of reducer
    reduce_worker->reduce_fnc(reduce_worker->mr, kvset, 0);
    // reducefn status set to 1 (mark it as complete)
    reduce_worker->mr->reduce_func_status = 1;
    printf("reducer finish map_wrapper\n");

    reducer_destroy(reduce_worker);
    return 0;
}


Status ReducerServiceImplementation::start_reducer(
    ServerContext* context, 
    const ReducerServiceStartRequest* request, 
    ReducerServiceStartReply* reply
) {
    printf("call to reducer rpc\n");
    char *app = (char*) request->application().c_str();
    int n_threads = request->threads();
    char *out = (char*) request->outpath().c_str();
    int id_num = request->id();
    mr = mr_init(app, n_threads, "dummy.txt", out,  ".,:;");
    mr->barrierEnable = request->barrier();

    // start reducer
    start_reducer_(mr, id_num);
    int result = mr->reduce_func_status;
    reply->set_result(result);

    mr_destroy(mr);
    printf("return from reducer rpc\n");
    return Status::OK;
} 

Status ReducerServiceImplementation::flush(
    ServerContext* context, 
    const ReducerServiceFlushRequest* request, 
    ReducerServiceFlushReply* reply
) {
    // receive the buffer from mapper
    int received = receiver(mr, request->buffer(), request->size(), request->id());
    
    reply->set_successful(received);
    return Status::OK;
} 

int main(int argc, char** argv) {
    std::string address("0.0.0.0:5001");
    ReducerServiceImplementation service;

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


