/*
 * MapReduce utilities header file
 */




#ifndef MAPREDUCE_UTILS_H
#define MAPREDUCE_UTILS_H

#include <string>
#include <vector>
#include <inttypes.h>
#include "../mapreduce/mapreduce.h"
#include <fstream>
#include <sstream>

/**
 * Data structures for MapReduce communication and coordination
 */


//Structure to hold arguments for RPC calls between MapReduce components

struct mapper_grpc_args {
    map_reduce *mr;     
    char *chunk;       
    uint32_t start_line;      
    int32_t id;            
};

struct reducer_grpc_args {
    map_reduce *mr;     
    char *outpath;      
    int32_t id;            
};


int32_t read_config_file(std::string config_name, map_reduce* mr);

// implement split function
void split(const std::string &line, std::vector<std::string> &out);

#endif // MAPREDUCE_UTILS_H
