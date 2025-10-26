
#include "mapreduce.h"
#include <string.h>
#include <stdlib.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include "../util/mapreduce_util.h"
#include "../grpcclient/mapper_service_client.h"
#include "../grpcclient/reducer_service_client.h"

// HDFS paths from Makefile
#ifndef HADOOP_HOME
#define HADOOP_HOME "/scratch/sqb6440/hadoop"
#endif
#ifndef HDFS_BIN
#define HDFS_BIN "/scratch/sqb6440/hadoop/bin/hdfs"
#endif

// Helper function declarations
int check_and_start_hdfs(void);
int load_input_to_hdfs(const char* input_path);
int divide_file_into_chunks(struct map_reduce *mr);

struct map_reduce *mr_init(const char *application, int threads, const char *inpath, const char *outpath, const char *helper_args) {
    printf("mr_init\n");
    printf("application: %s\n", application);
    printf("threads: %d\n", threads);
    printf("inpath: %s\n", inpath);
    printf("outpath: %s\n", outpath);
    printf("helper_args: %s\n", helper_args);
    // Allocate memory for the map_reduce structure
    struct map_reduce *mr = (struct map_reduce *)malloc(sizeof(struct map_reduce));
    if (mr == NULL) {
        printf("Error: Failed to allocate memory for map_reduce structure\n");
        free(mr);
        return NULL;
    }
    
    // Initialize all fields with provided parameters
    mr->application = (char *)malloc(strlen(application) + 1);
    if (mr->application == NULL) {
        printf("Error: Failed to allocate memory for application name\n");
        free(mr);
        return NULL;
    }
    strcpy(mr->application, application);
    
    mr->numOfMapWorkerThreads = threads;
    
    mr->inpath = (char *)malloc(strlen(inpath) + 1);
    if (mr->inpath == NULL) {
        printf("Error: Failed to allocate memory for input path\n");
        free(mr->application);
        free(mr);
        return NULL;
    }
    strcpy(mr->inpath, inpath);
    
    mr->outpath = (char *)malloc(strlen(outpath) + 1);
    if (mr->outpath == NULL) {
        printf("Error: Failed to allocate memory for output path\n");
        free(mr->application);
        free(mr->inpath);
        free(mr);
        return NULL;
    }
    strcpy(mr->outpath, outpath);
    
    mr->helper_args = (char *)malloc(strlen(helper_args) + 1);
    if (mr->helper_args == NULL) {
        printf("Error: Failed to allocate memory for helper args\n");
        free(mr->application);
        free(mr->inpath);
        free(mr->outpath);
        free(mr);
        return NULL;
    }
    strcpy(mr->helper_args, helper_args);

    // Initialize other fields
    mr->barrierEnable = 0;
    mr->genOutput = 0;
    mr->used_size = 0;
    mr->receiver_finished = 0;
    mr->reduce_func_status = -1;

    pthread_mutex_init(&mr->lock, NULL);
    pthread_cond_init(&mr->not_full, NULL);
    pthread_cond_init(&mr->not_empty, NULL);
    pthread_cond_init(&mr->map_finished, NULL);
 
    // Set function pointers to the application-provided callback functions
    // These will be resolved at link time based on which binary is linked
    mr->map_fnc = &map;      // Application's map callback function
    mr->reduce_fnc = &reduce; // Application's reduce callback function
    mr->reduce_buffer = NULL;
    mr->map_buffer = NULL;
    
    printf("Initialized framework for application: %s\n", application);

    // Initialize C++ objects before calling read_config
    new (&mr->mapper_addresses) std::vector<std::string>();
    new (&mr->reducer_address) std::string();
    new (&mr->file_chunks) std::vector<std::string>();
    new (&mr->chunk_start_lines) std::vector<uint32_t>();
    new (&mr->map_func_status) std::vector<int>();
    new (&mr->key_assigned_address) char*();


    if(read_config_file(std::string("config"), mr) == -1) {
        printf("Read Config File Failed\n");
        return NULL;
    }

    // Check and start HDFS if not running
    if (check_and_start_hdfs() != 0) {
        printf("Error: Failed to initialize HDFS\n");
        return NULL;
    }

    // Allocate mapper threads array
    mr->mapper_threads = (pthread_t *)malloc(mr->numOfMappers * sizeof(pthread_t));
    if (mr->mapper_threads == NULL) {
        printf("Error: Failed to allocate memory for mapper threads\n");
        free(mr->application);
        free(mr->inpath);
        free(mr->outpath);
        free(mr->helper_args);
        free(mr);
        return NULL;
    }

    // Initialize map function status vector
    for(int i = 0; i < mr->numOfMappers; i++) {
        mr->map_func_status.push_back(0);
    }

    printf("MapReduce framework initialized \n");
    
    return mr;
}

static void* reducer_wrapper(void* args) {
    // Reconstruct the Arguments
    struct reducer_grpc_args *reducer_args = (struct reducer_grpc_args *) args;

    // Call rpc and save 
    std::string address = reducer_args->mr->reducer_address;
    int32_t status = start_reducer_rpc(reducer_args->mr, reducer_args->outpath, reducer_args->id, address);
    
    if(status == -1) {
        printf("start_reducer_rpc() returns error\n");
    }
    reducer_args->mr->reduce_func_status = status;
    // free argument
    free(args);
    return NULL;
}



static void* mapper_wrapper(void* args) {
    // Reconstruct the Arguments
    struct mapper_grpc_args *mapper_args = (struct mapper_grpc_args *) args;
    // Call the map rpc and save the return value
    std::string address = mapper_args->mr->mapper_addresses[mapper_args->id];
    // print the chunk and start_line
    // for(int i = 0; i < mapper_args->mr->file_chunks.size(); i++) {
    int32_t ret = start_mapper_rpc(mapper_args->mr, mapper_args->chunk, mapper_args->start_line, mapper_args->id, address);
    // Check return value
    if(ret == -1) {
        printf("mapper_start_rpc() returns error\n");
    }
    mapper_args->mr->map_func_status[mapper_args->id] = ret;
    // }
    
    free(args);
    return NULL;
}

int mr_start(struct map_reduce *mr, const int barrierEnable) {

    // Initialize distributed file system and load input file on HDFS cluster
    printf("Initializing HDFS and loading input file...\n");
    if (load_input_to_hdfs(mr->inpath) != 0) {
        free(mr);
        return -1;
    }
    
    // Divide input file into chunks for each mapper
    if (divide_file_into_chunks(mr) != 0) {
        free(mr);
        return -1;
    }
    
    mr->barrierEnable = barrierEnable;
    printf("Starting MapReduce job with %d mapper threads, barrier: %s\n", 
           mr->numOfMappers, barrierEnable ? "enabled" : "disabled");

    // Start reducer thread first as mapper will talk to reducer to send the data
    reducer_grpc_args* reducer_args = (reducer_grpc_args*) malloc(sizeof(reducer_grpc_args));
    if(reducer_args == NULL) {
        printf("Failed to allocate reducer args memory\n");
        return -1;
    }
    reducer_args->mr = mr;


    // Allocate and copy output path
    reducer_args->outpath = (char *)malloc(strlen(mr->outpath) + 1);
    if(reducer_args->outpath == NULL) {
        printf("Failed to allocate reducer output path memory\n");
        free(reducer_args);
        return -1;
    }
    strcpy(reducer_args->outpath, mr->outpath);

    // Set reducer ID
    reducer_args->id = mr->numOfMappers;

    // Create reducer thread
    if(pthread_create(&mr->reducer_thread, NULL, &reducer_wrapper, (void *)reducer_args) != 0) {
        perror("Failed to create reducer thread");
        free(reducer_args->outpath);
        free(reducer_args);
        return -1;
    }

    // Start mapper threads
    for(int i = 0; i < mr->numOfMappers; i++) {
        mapper_grpc_args* mapper_args = (mapper_grpc_args*) malloc(sizeof(mapper_grpc_args));
        if(mapper_args == NULL) {
            printf("Failed to allocate mapper args memory for thread %d\n", i);
            return -1;
        }
        mapper_args->mr = mr;
        mapper_args->id = i;
        
        // Allocate and copy the chunk content
        mapper_args->chunk = (char*)malloc(mr->file_chunks[i].length() + 1);
        strcpy(mapper_args->chunk, mr->file_chunks[i].c_str());
        mapper_args->start_line = mr->chunk_start_lines[i];
        

        // Create mapper threads for each mapper
        if(pthread_create(&mr->mapper_threads[i], NULL, &mapper_wrapper, (void *)mapper_args) != 0) {
            perror("Failed to create mapper thread");
            free(mapper_args);
            return -1;
        }
        
        printf("Created mapper thread %d\n", i);
    }

    printf("All %d mapper threads and 1 reducer thread created successfully\n", mr->numOfMappers);
    return 0;
}



int mr_finish(struct map_reduce *mr) {
    printf("Waiting for all mapper threads to complete...\n");
    printf("mr_finish\n");
    
    // Wait for all mapper threads to complete
    for(int i = 0; i < mr->numOfMappers; i++) {
        if(pthread_join(mr->mapper_threads[i], NULL) != 0) {
            printf("Error joining mapper thread %d\n", i);
            return -1;
        }
        printf("Mapper thread %d completed\n", i);
    }
    
    printf("Waiting for reducer thread to complete...\n");
    
    // Wait for reducer thread to complete
    if(pthread_join(mr->reducer_thread, NULL) != 0) {
        printf("Error joining reducer thread\n");
        return -1;
    }
    
    printf("All threads completed successfully\n");
    
    // Check if any mapper or reducer failed
    for(int i = 0; i < mr->numOfMappers; i++) {
        if(mr->map_func_status[i] != 0) {
            printf("Mapper thread %d failed with status %d\n", i, mr->map_func_status[i]);
            return -1;
        }
    }
    
    if(mr->reduce_func_status != 0) {
        printf("Reducer thread failed with status %d\n", mr->reduce_func_status);
        return -1;
    }
    
    printf("MapReduce job completed successfully\n");
    return 0;
}

int mr_produce(struct map_reduce *mr, const struct kvpair *kv) {
     // Lock so that producer and sender can synchronize
     pthread_mutex_lock(&mr->lock);
    //  printf("mr_produce lock\n");
     // Use the kv_pair size to check if buffer overflow
     int kv_size = kv->keysz + kv->valuesz + 8;
     // Wait for the sender to send out content in the buffer to reducer
     while((mr->used_size + kv_size) >= mr->map_buffer_size * MR_BUFFER_THRESHOLD) {
        //flush
        if (mr->used_size <= mr->map_buffer_size * MR_BUFFER_THRESHOLD) {
            //call flush rpc and wait rpc return and return the return value
            flush_rpc(0, mr->map_buffer, mr->used_size, mr->reducer_address);
            mr->used_size = 0;
            break;
        }
        printf("mr_produce wait not_full\n");
        pthread_cond_wait(&mr->not_full, &mr->lock);
        printf("mr_produce wait not_full done\n");
     }
     // if not overflow, put the kvpair in the buffer
     // put from the place we last use
     memmove(&mr->map_buffer[mr->used_size], &kv->keysz, 4);
     mr->used_size +=  4;
     memmove(&mr->map_buffer[mr->used_size], kv->key, kv->keysz);
     mr->used_size += kv->keysz;
     memmove(&mr->map_buffer[mr->used_size], &kv->valuesz, 4);
     mr->used_size += 4;
     memmove(&mr->map_buffer[mr->used_size], kv->value, kv->valuesz);
     mr->used_size += kv->valuesz;
    //  printf("mr_produce used_size: %d\n", mr->used_size);
     // Finish copying kvpair to the memory
     // signal the sender the buffer is not empty
     pthread_cond_signal (&mr->not_empty);
    //  printf("mr_produce signal not_empty\n");
     // Unlock
     pthread_mutex_unlock(&mr->lock);
    //  printf("mr_produce unlock\n");
     // Success
     return 1;
}

int mr_consume(struct map_reduce *mr, struct kvpair *kvset, size_t *num, const int barrierEnable) {
    // print the used_size of the buffer
    // printf("consumer in mr_consume used_size: %d\n", mr->used_size);
    // // if used_size is 0, return 0
    // if(mr->used_size == 0) {
    //     if(mr->receiver_finished != 1) {
    //         printf("consumer wait for receiver to finish\n");
    //         return 0;
    //     }
    // }
    // lock so that consumer and receiver can synchronize
    pthread_mutex_lock(&mr->lock);
    printf("start consuming...\n");
    // if barrier flag enable

    // Check the size to make sure that
    // there is something in the buffer
    // for us to consume
    while(mr->used_size <= 0) {
        printf("consume buffer is empty\n");
        // if this is true
        // it means that all mappers finished
        if(mr->receiver_finished == 1) {
            printf("consumer receive finish status\n");
            pthread_mutex_unlock(&mr->lock);
            return 1;
        }
        // otherwise
        // Wait for signal from receiver
        pthread_cond_wait(&mr->not_empty, &mr->lock);
        printf("consumer wait for signal from receiver\n");
    }
    // we have to first know how many kvpairs
    // are in the buffer
    printf("consumer used_size: %d\n", mr->used_size);
    int offset = 0;
    (*num) = 0;
    printf("consumer offset: %d\n", offset);
    struct kvpair kv;
    printf("consumer kv.keysz: %d\n", kv.keysz);
    printf("consumer kv.valuesz: %d\n", kv.valuesz);
    while(offset != mr->used_size) {
        memcpy(&kv.keysz, &mr->reduce_buffer[offset], 4);
        offset += 4;
        offset += kv.keysz;
        memcpy(&kv.valuesz, &mr->reduce_buffer[offset], 4);
        offset += 4;
        offset += kv.valuesz;
        (*num)++;
    }
    //print the num
    printf("consumer num: %d\n", (int)*num);
    // Since we can not access the value in the buffer anymore
    // we have to allocate another space for kvpairs
    mr->key_assigned_address = (char*) malloc(mr->used_size * sizeof(char));
    memcpy(mr->key_assigned_address, mr->reduce_buffer, mr->used_size);

    // after knowing how many kvpairs are in the buffer
    // we can malloc an array of kvpairs at kvset
    // and let pointers point to the space we just assign
    offset = 0;
    kvset = (kvpair*) malloc((*num) * sizeof(kvpair));
    for(int i = 0; i < (*num); i++) {
        memcpy(&kvset[i].keysz, &mr->key_assigned_address[offset], 4);
        offset += 4;
        kvset[i].key = &(mr->key_assigned_address[offset]);
        offset += kvset[i].keysz;
        memcpy(&kvset[i].valuesz, &mr->key_assigned_address[offset], 4);
        offset += 4;
        kvset[i].value = &(mr->key_assigned_address[offset]);
        offset += kvset[i].valuesz;
    }

    // set to 0, so we can reuse the buffer space
    mr->used_size = 0;

    // since we consume the buffer
    // we can tell receiver the buffer
    // is not full
    pthread_cond_signal(&mr->not_full);

    // consumer have to wait for receiver
    // to get all the finish massages from
    // mappers.
    if(barrierEnable) {
        printf("barrier enable, wait for mappers to finish\n");
        pthread_cond_wait(&mr->map_finished, &mr->lock);
    }
    // so now we have kvpairs to ready to be processed
    printf("execute reduce function\n");
    mr->reduce_fnc(mr, kvset, *num);
    printf("reduce function return\n");
    // The kvpairs has been processed
    // we can free the memory
    free(mr->key_assigned_address);
    mr->key_assigned_address = NULL;  // Set to NULL after freeing
    free(kvset);

    
    pthread_mutex_unlock(&mr->lock);
    printf("finish consuming...\n");
    // Success
    return 1;
}

int mr_output(struct map_reduce *mr, char *writeBuffer, size_t bufferLength) {
    // Write the output to HDFS
    printf("Writing output to HDFS...\n");
    
    // Create temporary local file to write buffer content
    std::string temp_file = "/tmp/mr_output_" + std::string(mr->application) + "_" + std::to_string(getpid()) + ".tmp";
    FILE *temp_output = fopen(temp_file.c_str(), "w");
    if (temp_output == NULL) {
        printf("Error: Failed to create temporary output file\n");
        return -1;
    }
    
    // Write buffer to temporary file
    size_t written = fwrite(writeBuffer, 1, bufferLength, temp_output);
    fclose(temp_output);
    
    if (written != bufferLength) {
        printf("Error: Failed to write complete buffer to temporary file\n");
        unlink(temp_file.c_str()); // Clean up temp file
        return -1;
    }
    
    // Create HDFS output directory if it doesn't exist
    std::string create_dir_cmd = std::string(HDFS_BIN) + " dfs -mkdir -p /output";
    int result = system(create_dir_cmd.c_str());
    if (result != 0) {
        printf("Warning: Failed to create HDFS output directory (may already exist)\n");
    }
    
    // Upload temporary file to HDFS (replace if exists)
    std::string output_filename = std::string(mr->outpath);
    size_t last_slash = output_filename.find_last_of("/");
    if (last_slash != std::string::npos) {
        output_filename = output_filename.substr(last_slash + 1);
    }
    std::string hdfs_output_path = "/output/" + output_filename;
    
    // Check if output file already exists and remove it
    std::string check_cmd = std::string(HDFS_BIN) + " dfs -test -e " + hdfs_output_path + " > /dev/null 2>&1";
    result = system(check_cmd.c_str());
    if (result == 0) {
        printf("Output file %s already exists in HDFS, removing it...\n", hdfs_output_path.c_str());
        std::string remove_cmd = std::string(HDFS_BIN) + " dfs -rm " + hdfs_output_path;
        result = system(remove_cmd.c_str());
        if (result != 0) {
            printf("Warning: Failed to remove existing output file, will try to overwrite\n");
        }
    }
    
    // Upload temporary file to HDFS
    std::string upload_cmd = std::string(HDFS_BIN) + " dfs -put " + temp_file + " " + hdfs_output_path;
    printf("Executing: %s\n", upload_cmd.c_str());
    result = system(upload_cmd.c_str());
    
    // Clean up temporary file
    unlink(temp_file.c_str());
    
    if (result != 0) {
        printf("Error: Failed to upload output file to HDFS\n");
        return -1;
    }
    
    printf("Output successfully written to HDFS at %s\n", hdfs_output_path.c_str());
    return 0;
}

void mr_destroy(struct map_reduce *mr) {
    if (mr == NULL) return;
    
    // Free allocated memory
    printf("mr_destroy free allocated memory\n");
    if (mr->application) {
        printf("mr_destroy free application\n");
        free(mr->application);
    }   
    if (mr->inpath) {
        printf("mr_destroy free inpath\n");
        free(mr->inpath);
    }
    if (mr->outpath) {
        printf("mr_destroy free outpath\n");
        free(mr->outpath);
    }
    if (mr->helper_args) {
        printf("mr_destroy free helper_args\n");
        free(mr->helper_args);
    }
    if (mr->reduce_buffer) {
        printf("mr_destroy free reduce_buffer\n");
        free(mr->reduce_buffer);
    }
    if (mr->map_buffer) {
        printf("mr_destroy free map_buffer\n");
        free(mr->map_buffer);
    }
    if (mr->mapper_threads) {
        printf("mr_destroy free mapper_threads\n");
        free(mr->mapper_threads);
    }
    if (mr->key_assigned_address) {
        printf("mr_destroy free key_assigned_address\n");
        free(mr->key_assigned_address);
    }
    
    // Destroy C++ objects
    printf("mr_destroy destroy C++ objects\n");
    mr->mapper_addresses.~vector();
    mr->reducer_address.~basic_string();
    mr->file_chunks.~vector();
    mr->chunk_start_lines.~vector();
    mr->map_func_status.~vector();
    
    // Destroy synchronization primitives
    printf("mr_destroy destroy synchronization primitives\n");
    pthread_cond_destroy(&mr->not_full);
    pthread_cond_destroy(&mr->not_empty);
    pthread_mutex_destroy(&mr->lock);
    pthread_cond_destroy(&mr->map_finished);
    
    // Free the structure itself
    free(mr);
}

// Helper function to load input file to HDFS
int load_input_to_hdfs(const char* input_path) {
    printf("Loading input file %s to HDFS...\n", input_path);
    
    // Check if input file exists locally
    std::ifstream file(input_path);
    if (!file.good()) {
        printf("Error: Input file %s does not exist\n", input_path);
        return -1;
    }
    file.close();
    
    // Create HDFS input directory if it doesn't exist
    std::string create_dir_cmd = std::string(HDFS_BIN) + " dfs -mkdir -p /input";
    int result = system(create_dir_cmd.c_str());
    if (result != 0) {
        printf("Warning: Failed to create HDFS input directory (may already exist)\n");
    }
    
    // Upload file to HDFS (replace if exists)
    std::string input_filename = std::string(input_path);
    size_t last_slash = input_filename.find_last_of("/");
    if (last_slash != std::string::npos) {
        input_filename = input_filename.substr(last_slash + 1);
    }
    std::string hdfs_input_path = "/input/" + input_filename;
    
    // Check if file already exists and remove it
    std::string check_cmd = std::string(HDFS_BIN) + " dfs -test -e " + hdfs_input_path + " > /dev/null 2>&1";
    result = system(check_cmd.c_str());
    if (result == 0) {
        printf("File %s already exists in HDFS, removing it...\n", hdfs_input_path.c_str());
        std::string remove_cmd = std::string(HDFS_BIN) + " dfs -rm " + hdfs_input_path;
        result = system(remove_cmd.c_str());
        if (result != 0) {
            printf("Warning: Failed to remove existing file, will try to overwrite\n");
        }
    }
    
    // Upload file to HDFS
    std::string upload_cmd = std::string(HDFS_BIN) + " dfs -put " + std::string(input_path) + " " + hdfs_input_path;
    printf("Executing: %s\n", upload_cmd.c_str());
    result = system(upload_cmd.c_str());
    if (result != 0) {
        printf("Error: Failed to upload input file to HDFS\n");
        return -1;
    }
    
    printf("Input file successfully loaded to HDFS at %s\n", hdfs_input_path.c_str());
    return 0;
}

// Helper function to divide input file into chunks for each mapper
int divide_file_into_chunks(struct map_reduce *mr) {
    int numberOfChunks = mr->numOfMappers;
    printf("Dividing input file into %d chunks...\n", numberOfChunks);
    
    // Read the entire input file directly from local filesystem
    // (since we're on the same machine that has the file)
    std::ifstream file(mr->inpath);
    if (!file.good()) {
        printf("Error: Cannot open input file %s\n", mr->inpath);
        return -1;
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    file.close();
    
    // Count total lines
    size_t total_lines = 0;
    size_t pos = 0;

    while ((pos = content.find('\n', pos)) != std::string::npos) {
        total_lines++;
        pos++;
    }
    if (!content.empty() && content.back() != '\n') {
        total_lines++; // Last line without newline
    }
    
    // Calculate lines per chunk
    size_t lines_per_chunk = total_lines / numberOfChunks;
    size_t remaining_lines = total_lines % numberOfChunks;
    
    // Divide content into chunks
    size_t current_pos = 0;
    size_t current_line = 1;
    for (int i = 0; i < numberOfChunks; i++) {
        size_t chunk_lines = lines_per_chunk;
        if (i < remaining_lines) {
            chunk_lines++; // Distribute remaining lines to first few chunks
        }
        mr->chunk_start_lines.push_back(current_line);
        // Find the end position for this chunk
        size_t end_pos = current_pos;
        size_t lines_found = 0;
        while (end_pos < content.length() && lines_found < chunk_lines) {
            if (content[end_pos] == '\n') {
                lines_found++;
            }
            end_pos++;
        }
        // Extract chunk content
        std::string chunk = content.substr(current_pos, end_pos - current_pos);
        mr->file_chunks.push_back(chunk);
        
        current_pos = end_pos;
        current_line += chunk_lines;
    }

    printf("File successfully divided into %d chunks\n", mr->numOfMappers);
    return 0;
}

// Helper function to check and start HDFS if not running
int check_and_start_hdfs(void) {
    printf("Checking HDFS status...\n");
    
    // Check if HDFS is running by trying to list root directory
    std::string check_cmd = std::string(HDFS_BIN) + " dfs -ls / > /dev/null 2>&1";
    int result = system(check_cmd.c_str());
    
    if (result == 0) {
        printf("HDFS is already running\n");
        
        // Check if NameNode is in safe mode and wait for it to leave naturally
        std::string safe_mode_cmd = std::string(HDFS_BIN) + " dfsadmin -safemode get 2>&1";
        FILE* pipe = popen(safe_mode_cmd.c_str(), "r");
        if (pipe) {
            char buffer[256];
            std::string output;
            while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
                output += buffer;
            }
            pclose(pipe);
            
            if (output.find("Safe mode is ON") != std::string::npos) {
                printf("NameNode is in safe mode, waiting for it to leave naturally...\n");
                
                // Wait for safe mode to be turned off naturally (up to 60 seconds)
                for (int i = 0; i < 60; i++) {
                    sleep(1);
                    
                    FILE* check_pipe = popen(safe_mode_cmd.c_str(), "r");
                    if (check_pipe) {
                        char check_buffer[256];
                        std::string check_output;
                        while (fgets(check_buffer, sizeof(check_buffer), check_pipe) != NULL) {
                            check_output += check_buffer;
                        }
                        pclose(check_pipe);
                        
                        if (check_output.find("Safe mode is OFF") != std::string::npos) {
                            printf("NameNode has left safe mode naturally\n");
                            return 0;
                        }
                    }
                    
                    if (i % 10 == 0 && i > 0) {
                        printf("Still waiting for safe mode to turn off... (%d/60 seconds)\n", i);
                    }
                }
                
                printf("Warning: NameNode still in safe mode after 60 seconds, but continuing...\n");
            } else {
                printf("NameNode is not in safe mode\n");
            }
        }
        return 0;
    }
    
    printf("HDFS is not running, attempting to start...\n");
    
    // Start HDFS services
    std::string start_dfs_cmd = std::string(HADOOP_HOME) + "/sbin/start-dfs.sh";
    printf("Executing: %s\n", start_dfs_cmd.c_str());
    result = system(start_dfs_cmd.c_str());
    
    if (result != 0) {
        printf("Error: Failed to start HDFS services\n");
        return -1;
    }
    
    // Wait for services to start and leave safe mode naturally
    printf("Waiting for HDFS services to start and leave safe mode naturally...\n");
    for (int i = 0; i < 60; i++) { // Wait up to 60 seconds
        sleep(1);
        
        // Check if HDFS is responding
        result = system(check_cmd.c_str());
        if (result == 0) {
            // Check if still in safe mode
            std::string safe_mode_cmd = std::string(HDFS_BIN) + " dfsadmin -safemode get 2>&1";
            FILE* pipe = popen(safe_mode_cmd.c_str(), "r");
            if (pipe) {
                char buffer[256];
                std::string output;
                while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
                    output += buffer;
                }
                pclose(pipe);
                
                if (output.find("Safe mode is OFF") != std::string::npos) {
                    printf("HDFS successfully started and left safe mode naturally\n");
                    return 0;
                }
            }
        }
        
        if (i % 10 == 0 && i > 0) {
            printf("Waiting for HDFS to be ready... (%d/60 seconds)\n", i);
        }
    }
    
    printf("Warning: HDFS may still be in safe mode after 60 seconds, but continuing...\n");
    return 0;
}


