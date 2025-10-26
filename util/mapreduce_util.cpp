// MapReduce Utilities Implementation



#include "mapreduce_util.h"

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>



// Split a line by whitespace into tokens; clears output first
void split(const std::string &line, std::vector<std::string> &out) {

    out.clear();
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
        out.push_back(token);
    }
   
    return;
}

// Read configuration file
int32_t read_config_file(std::string config_name, map_reduce* mr) {
    std::fstream config_file;

    if (mr == NULL) {
        printf("ERROR: mr pointer is NULL!\n");
        return -1;
    }
    config_file.open(config_name, std::ios::in);

    if (!config_file) {
        return -1;
    }
    std::string line;
    std::vector<std::string> parsed_line;
    while (std::getline(config_file, line)) {
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        split(line, parsed_line);
        if (parsed_line.empty()) { 
            continue;
        }
        
        if (parsed_line[0] == "MAPPER") {
            if (parsed_line.size() >= 3) {
                int32_t count = std::stoi(parsed_line[1]);
                mr->numOfMappers = count;
                
                printf("Config: Setting up %d mappers\n", count);
                
                // Clear existing mapper addresses
                mr->mapper_addresses.clear();
                
                // Parse mapper addresses
                int expected_addresses = count;
                int provided_addresses = parsed_line.size() - 2; // Subtract "MAPPER" and count
                
                if (provided_addresses == 1) {
                    // Single address provided - use it for all mappers
                    std::string full_address = parsed_line[2];
                    printf("Config: Using single mapper address: %s\n", full_address.c_str());
                    for (int32_t i = 0; i < count; i++) {
                        mr->mapper_addresses.push_back(full_address);
                    }
                } else if (provided_addresses == expected_addresses) {
                    // Multiple addresses provided - use each one
                    printf("Config: Using distributed mapper addresses:\n");
                    for (int32_t i = 0; i < count; i++) {
                        std::string full_address = parsed_line[2 + i];
                        mr->mapper_addresses.push_back(full_address);
                        printf("  Mapper %d: %s\n", i, full_address.c_str());
                    }
                } else {
                    printf("ERROR: Expected %d mapper addresses, got %d\n", 
                           expected_addresses, provided_addresses);
                    return -1;
                }
            } else {
                printf("ERROR: MAPPER requires at least 3 arguments (MAPPER count address1 [address2 ...])\n");
                return -1;
            }
        } else if (parsed_line[0] == "REDUCER") {
            if (parsed_line.size() >= 2) {
                // Store full address for gRPC channel creation
                std::string full_address = parsed_line[1];
                mr->reducer_address = full_address;
                printf("Config: Reducer address: %s\n", full_address.c_str());
            } else {
                printf("ERROR: REDUCER requires at least 2 arguments (REDUCER address)\n");
                return -1;
            }
        } else if (parsed_line[0] == "MAPPER_BUFFER_SIZE") {
            if (parsed_line.size() >= 2) {
                int buffer_size = std::stoi(parsed_line[1]);
                mr->map_buffer_size = buffer_size;
                mr->reduce_buffer_size = buffer_size * (mr->numOfMappers + 1);
                printf("Config: Mapper buffer size: %zu, Reducer buffer size: %zu\n", 
                       mr->map_buffer_size, mr->reduce_buffer_size);
            } else {
                printf("ERROR: MAPPER_BUFFER_SIZE requires 2 arguments (MAPPER_BUFFER_SIZE size)\n");
                return -1;
            }
        } else {
            printf("WARNING: Unknown directive: %s\n", parsed_line[0].c_str());
        }
    }
    
    // Validate configuration
    if (mr->numOfMappers <= 0) {
        printf("ERROR: No valid MAPPER configuration found\n");
        return -1;
    }
    
    if (mr->reducer_address.empty()) {
        printf("ERROR: No valid REDUCER configuration found\n");
        return -1;
    }
    
    if (mr->map_buffer_size <= 0) {
        printf("WARNING: No MAPPER_BUFFER_SIZE specified, using default\n");
        mr->map_buffer_size = 1024000;
        mr->reduce_buffer_size = mr->map_buffer_size * (mr->numOfMappers + 1);
    }
    
    printf("Config: Successfully loaded configuration with %d mappers\n", mr->numOfMappers);
    return 0;
}
