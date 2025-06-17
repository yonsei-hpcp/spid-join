#include <iostream>
#include <cstdio>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/types.h>

#include <unistd.h>
#include <iomanip>

#include <fstream>
#include "pidjoin.hpp"

using namespace pidjoin;

void TestSPidJoin(join_design_t join_design)
{
    // To profile the latency of SPID-Join
    struct timespec start;

    int ratio = join_design.rs_ratio;
    int zipf_factor = join_design.zipf_factor;
    int number_of_ranks = join_design.rank_num;

    // Synthetic benchmark
    if (!join_design.tpch_example)
    {
        int64_t num_tuples = join_design.input_size;

        kv_pair_t* r_table = (kv_pair_t*) malloc(sizeof(kv_pair_t) * num_tuples + 4096);
        kv_pair_t* s_table = (kv_pair_t*) malloc(sizeof(kv_pair_t) * (ratio * num_tuples) + 4096);

        // Setting up synthetic values for Zipf = 0.0
        for (int64_t i = 0; i < num_tuples; i++)
        {
            r_table[i].lvalue = i + 1;
            r_table[i].rvalue = i + 3;
        }
        for (int64_t i = 0; i < (num_tuples * ratio); i++)
        {
            s_table[i].lvalue = (i / ratio) + 1;
            s_table[i].rvalue = (i / ratio) + 3;
        }

        if (zipf_factor != 0)
        {
            // Should load a file containing data with various zipf factors
        }
        
        // To validate the result of SPID-Join
        ResultBuffers_t result_buffer;

        // Join Setup
        JoinInstance join_instance(number_of_ranks, join_design.rank_set, number_of_ranks / join_design.rank_set);
        join_instance.LoadColumn((void*)r_table, num_tuples, "left");
        join_instance.LoadColumn((void*)s_table, (num_tuples * ratio), "right");
        join_instance.PrepareJoin(join_design);
        
        // For profiling the latency of join execution
        clock_gettime(CLOCK_MONOTONIC, &start);

        join_instance.StartJoin(join_design);
        result_buffer = join_instance.FinishJoin();

        // Profiled Result
        join_instance.RecordLogs(start, 0);
        
        free(r_table);
        free(s_table);
    }
}

int main(int argc, char** argv)
{   
    // Default Setting
    int bankgroup = 8;

    join_design_t join_design;
    join_design.primary_key = "order";
    join_design.foreign_key = "l";

    char opt;

    if (argc < 2)
    {
        exit(0);
    }
    else
    {
        while ((opt = getopt(argc, argv, "s:r:z:t:R:B:h:p:f:")) != -1)
        {
            switch(opt)
            {
            case 's':
                join_design.input_size = atoi(optarg);
                break;
            case 'r':
                join_design.rank_num = atoi(optarg);
                break;
            case 'z':
                join_design.zipf_factor = atoi(optarg);
                break;
            case 't':
                join_design.rs_ratio = atoi(optarg);
                break;
            case 'R':
                join_design.rank_set = atoi(optarg);
                break;
            case 'B':
                join_design.bank_set = atoi(optarg);
                break;
            }
        }
    }

    TestSPidJoin(join_design);
    return 0;
}