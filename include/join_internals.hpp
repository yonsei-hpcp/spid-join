#pragma once

#include "idpHandler.hpp"
#include "json/json.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <thread>
#include <numa.h>
#include <list>
#include <queue>
#include <semaphore.h>

#define MAX_RANK_GROUPS 16
#define NUM_NUMA_NODE 2
#define NUM_THREAD_PER_NODE 8

#define QUERY_PLAN_EXECUTE_PLAN 0
#define QUERY_PLAN_DEPRECATED 1
#define QUERY_PLAN_BANDWIDTH_TEST 2

namespace pidjoin
{
    enum job_priority_func_opt_t
    {
        RANK_FIRST,
        CH_LOAD_BAL,
        JOB_FIRST,
        DEFAULT
    };

    class JoinOperator;

    typedef struct lock_
    {
        pthread_mutex_t mutex;
        pthread_cond_t cond;
        int job_counter; // number of jobs finished - Comment
        lock_(void)
        {
            job_counter = 0;
            pthread_mutex_init(&mutex, NULL);
            pthread_cond_init(&cond, NULL);
        }
    } lock_;

    typedef struct GlobalBuffer_t
    {
        void *aligned_data;
        pthread_mutex_t lock;
        pthread_cond_t cond;
        bool allocated = false;
        size_t alloced_size;
    } GlobalBuffer_t;

    typedef struct TimeStamp_t // - Comment
    {
        TimeStamp_t(int id, std::string name) : rank_id{id}, start_time{0}, end_time{0}, node_name{name}, inputs{""} {}
        TimeStamp_t(int id, Json::Value &query_node, std::string suffix) : rank_id{id}, start_time{0}, end_time{0}
        {
            if (query_node.isMember("operator"))
                node_name = query_node["operator"].asString();
            else
                node_name = "";
            node_name += suffix;
            if (query_node.isMember("input"))
                inputs = query_node["input"].asString();
            else if (query_node.isMember("inputs"))
            {
                for (int i = 0; i < query_node["inputs"].size(); i++)
                {
                    if (i != 0)
                        inputs += ",";
                    inputs += query_node["inputs"][i].asString();
                }
            }
            else
                inputs = "";
        }
        int rank_id;
        struct timespec start_time;
        struct timespec end_time;
        std::string node_name;
        std::string inputs;
    } TimeStamp_t;

#define STAT_TYPE_RNS 0
#define STAT_TYPE_JOIN 1

    typedef struct stat_info_t
    {
        int STAT_TYPE;
        // JOIN
        int miss_count;
        int num_tuples;
        int hit_count;
        // RNS
        int64_t xfer_byte;
        int64_t max_xfer_byte; // max DPU->DPU TX data in byte for each source rank - Comment
        int64_t xfer_packet_num;
        int packet_size;
        // UPMEM shuffle
        int64_t upmem_tx_byte;
    } stat_info_t;

    typedef struct load_imbalance_info_t
    {
        int64_t* r_effective_elem_count_per_idp = nullptr;
        int64_t* r_allocated_elem_count_per_idp = nullptr;
        int64_t* s_effective_elem_count_per_idp = nullptr;
        int64_t* s_allocated_elem_count_per_idp = nullptr;
        int64_t* r_inter_rank_elem_count = nullptr;
        int64_t* s_inter_rank_elem_count = nullptr;
    } load_imbalance_info_t;
     

    typedef std::map<std::string, stat_info_t> StatLogs_t;


    class JoinInstance
    {
        friend class JoinOperator;
        friend class IDPHandler;

    private:
        pthread_mutex_t instance_lock;
        int rank_group_id = 0;
        int64_t lefthand_elem_cnt;
        int64_t righthand_elem_cnt;
        int scale_factor;
        int packet_size;
        float zipf_factor;
        std::string join_algorithm;

        load_imbalance_info_t imbalance_info;

        pthread_mutex_t thread_sync_mutex;
        pthread_cond_t thread_sync_cond;
        int sync_value;

        /* Ranks allocated into this Query*/
        std::vector<StatLogs_t> rankwiseStatLogs;

        // file to print latency breakdown of a query - Comment
        FILE *timeline_ptr;
        pthread_mutex_t timeline_mutex;

        /* Rotation and Copy */
        pthread_mutex_t rotate_and_stream_mutex;
        sem_t rotate_and_stream_semaphore;

        pthread_mutex_t job_done_mutex;
        std::map<int, bool> job_done_map;

        int m_xfer_worker_num; // total thread num doing rank to rank transfer - Comment

        /* Thread Wait Line */
        pthread_mutex_t thread_queue_mutex;
        std::vector<std::pair<int, lock_ *>> thread_queue_line; // each rank thread(Producer) is aligned in this vector with the order of priority - Comment
        std::vector<int> rns_rank_thread_orders;
        int curr_queued;

        /* Join Operator */
        JoinOperator *join_operator;
        IDPHandler *idp_handler;

        /* Temp Buffer */
        pthread_mutex_t global_buffer_mutex;
        std::unordered_map<std::string, GlobalBuffer_t *> global_buffer_map;

        /* Rankwise Buffer */
        pthread_mutex_t rankwise_buff_mutex;
        std::unordered_map<std::string, RankwiseMemoryBankBuffPair_t> rankwise_buff_map;
        std::unordered_map<std::string, RankwiseMemoryBankBufferPair_t> rankwise_buffer_map;

        GlobalBuffer_t *GetOrAllocateGlobalBuffer(int64_t size_byte, const char *name, bool do_memset);
        GlobalBuffer_t *GetOrAllocateGlobalBuffer(int64_t size_byte, const char *name);
        GlobalBuffer_t *AllocateGlobalBuffer(int64_t size_byte, const char *name, bool do_memset);
        GlobalBuffer_t *AllocateGlobalBuffer(int64_t size_byte, const char *name);
        GlobalBuffer_t *GetGlobalBuffer(const char *name);
        GlobalBuffer_t *WaitForGlobalBuffer(const char *name);
        void RemoveGlobalBuffer(const char *name);

        RankwiseMemoryBankBufferPair_t *AllocateMemoryBankBuffers(int64_t size_byte, int num_rank, const char *name);
        RankwiseMemoryBankBufferPair_t *AllocateEmptyMemoryBankBuffers(int num_rank, const char *name);
        RankwiseMemoryBankBufferPair_t *AllocateMemoryBankBuffersRankwise(int64_t size_byte, int rank_id, const char *name);
        RankwiseMemoryBankBufferPair_t *AllocateEmptyMemoryBankBuffersRankwise(int rank_id, const char *name);
        void RemoveMemoryBankBuffers(const char *name);
        RankwiseMemoryBankBufferPair_t *GetMemoryBankBuffers(const char *name);
        RankwiseMemoryBankBufferPair_t *GetMemoryBankBuffers(std::string &name);
        int WaitJobDoneQueueLineMicrobench(int index, int num_ranks, int packet_size);
        RankwiseMemoryBankBuffPair_t *AllocateEmptyMemoryBankBuffs(int num_rank, const char *name);
        RankwiseMemoryBankBuffPair_t *GetMemoryBankBuffs(const char *name);

        // deprecated
        // /* CPU partitioning */
        // PRO::shared_t cpu_part_shared;

        /* Log - Comment */
        struct timespec m_timer_init;          // querry time measure start
        std::vector<TimeStamp_t> m_timeStamps; // per rank

        std::pair<void *, int> GetSharedObj(std::string &name);
        void SetSharedObj(std::string &name, void *obj_ptr, int obj_type);

        pthread_mutex_t *GetSharedObjMutex(std::string &name);
        void GetSharedObjLock();
        void ReleaseSharedObjLock();

        void CreateRNSLog(int rank_id, int64_t xfer_byte, int64_t max_xfer_byte, int64_t xfer_packet_num, int64_t upmem_tx_byte, int packet_size);
        void CreateJoinLog(int rank_id, int32_t num_tuples, int32_t hit_count, int32_t miss_count);

        /* Thread Wait Line */
        void ResetQueueLine();
        // Returns its order
        int ReadyQueueLine(int rank_id, lock_ *lock);
        int ReadyQueueLineFixedPriority(int rank_id, lock_ *lock);
        int WaitJobDoneQueueLine(int index, GlobalBuffer_t *my_accmuls, int num_ranks, int packet_size);
        void WakeUpQueueLine(int index);
        bool UpdateJobStatus(int index);

        int GetJobStatus(int index);
        std::pair<int, lock_ *> GetQueueElemByIndex(int index);
        std::pair<int, lock_ *> GetQueueElemByRankID(int rank_id);

        int tot_jobs_per_rank; // Number of jobs a rank has to wait accounting both send and receive. Same value across all ranks - Comment

        void AddJobIntoPriorityQueue(rotate_n_stream_job_t *job);

        void CollectLog(std::vector<TimeStamp_t> &timeStamps);
        void RecordLogs(); // print time information to a timeline file - Comment

        

        static void InitRnSJob(
            rotate_n_stream_job_t *job,
            float job_priority,
            int src_rank,
            int dst_rank,
            int mram_src_offset,
            int mram_dst_offset,
            int job_type,
            int prefetch_distance,
            float packet_num_2_copy,
            void *host_buffer,
            IDPHandler *idp_handler);

        static void InitRnSJob(
            rotate_n_stream_job_t *job,
            float job_priority,
            int src_rank,
            int dst_rank,
            int mram_src_offset,
            int mram_dst_offset,
            int job_type,
            int prefetch_distance,
            float packet_num_2_copy,
            int bankchunk_offset,
            int num_repeat,
            void *host_buffer,
            IDPHandler *idp_handler);

        static void InitXferJob(
            rotate_n_stream_job_t *job,
            bool store_data,
            int dst_rank,
            int mram_dst_offset,
            void *host_buffer,
            int64_t xfer_bytes,
            IDPHandler *idp_handler,
            pthread_mutex_t *mutex_ptr,
            pthread_cond_t *cond_ptr,
            int *running_jobs_per_rank);
        
        static void InitXferChipwiseBroadcastJob(
            rotate_n_stream_job_t *job,
            int src_rank,
            int dst_rank,
            int mram_src_offset,
            int mram_dst_offset,
            int64_t xfer_bytes_to_broadcast_per_idp,
            int job_type,
            IDPHandler* idp_handler);

        static void InitMultiRankXferChipwiseBroadcastJob(
            rotate_n_stream_job_t *job,
            int src_rank,
            std::vector<int>& dst_ranks,
            int mram_src_offset,
            int mram_dst_offset,
            int64_t xfer_bytes_to_broadcast_per_idp,
            int job_type,
            IDPHandler* idp_handler);
        // Result Buffers
        ResultBuffers_t result_bufferpool;

    public:
        // num_workers_per_rank: currently only working for UPMEM lib. Should pass to job_queue and modify void xeon_sp_do_rnc(RNS_Job_Queue_t* job_queue)
        JoinInstance();
        JoinInstance(int num_rank_allocated_);
        [[deprecated]] JoinInstance(int num_rank_allocated_, int rank_group_id);
        JoinInstance(int num_rank_allocated_, int num_rankgroup, int num_rank_in_rankgroup);
        ~JoinInstance();

        // Number of ranks
        int num_rank_allocated;
        int num_rankgroup;
        int num_rank_in_rankgroup;
        void SetRankAllocated(int num_rank_allocated_);

        /* Query Execution */
        void LoadColumn(void *data_ptr, int64_t num_tuples, std::string col_name);
        void ExecuteJoin(std::string join_type, join_design_t join_design);
        void AddJobToEndMap(rotate_n_stream_job_t *ended_job);

        void PrepareJoin(join_design_t join_design);
        void StartJoin(join_design_t join_design);
        ResultBuffers_t FinishJoin();
        void RecordLogs(struct timespec start, int rank_group);
    };

    class JoinOperator
    {
        friend class JoinInstance;

    private:
        // function for USG
        void WaitUSGJobFinish(const int &num_jobs, pthread_mutex_t &mutex, pthread_cond_t &cond);

    public:
        JoinOperator(JoinInstance *join_instance_)
        {
            join_instance = join_instance_;
        }
        ~JoinOperator() {}
        JoinInstance *join_instance;
        std::string join_alg;

        // DPU Functions
        void Execute_PACKETWISE_NPHJ_BUILD(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_PACKETWISE_NPHJ_PROBE(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_LOCAL_HASH_PARTITIONING(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_GLOBAL_HASH_PARTITIONING(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_PHJ_BUILD_HASH_TABLE(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_PHJ_PROBE_HASH_TABLE_INNER(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_PACKETWISE_LOCAL_HASH_PARTITIONING(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_NESTED_LOOP_JOIN(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_GLB_PARTITION_COUNT(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_GLB_PARTITION_PACKET(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_GLB_CHIPWISE_PARTITION_COUNT(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_GLB_CHIPWISE_PARTITION_PACKET(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_BG_BROADCAST_COUNT(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_BG_BROADCAST_ALIGN(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_MPSM_JOIN_LOCAL_PARTITIONING(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_MPSM_JOIN_SORT_PROBE(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_FINISH_JOIN(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        // Host Functions
        void Execute_HOST_FUNC_INVALIDATE_STACKNODE(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_HOST_FUNC_ROTATE_AND_STREAM(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_HOST_FUNC_SEND_DATA_OPT(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_HOST_FUNC_CHIPWISE_BROADCAST(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_HOST_FUNC_RECV_DATA_OPT(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_HOST_FUNC_BROADCAST_DATA(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        // Host Functions Pipelined
        void Execute_MT_SAFE_HOST_FUNC_CALCULATE_PAGE_HISTOGRAM(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_MT_SAFE_HOST_FUNC_CALCULATE_BANKGROUP_HISTOGRAM(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_MT_SAFE_HOST_FUNC_CHIPWISE_CALCULATE_PAGE_HISTOGRAM(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        // Compound Functions
        void Execute_COMPOUND_FUNC_RNS_JOIN(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node, std::vector<TimeStamp_t> *perThreadTimeStamps);
        void Execute_COMPOUND_FUNC_IDP_JOIN(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node, std::vector<TimeStamp_t> *perThreadTimeStamps);
        void Execute_COMPOUND_FUNC_GLB_PARTITION(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_COMPOUND_FUNC_GLB_CHIPWISE_PARTITION(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        void Execute_COMPOUND_FUNC_BG_BROADCAST(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);
        // Control Functions
        void Execute_CONTROL_FUNC_SYNC_THREADS(IDPHandler *idp_handler, int rank_id, DPUKernelParams_t *params, Json::Value *op_node);

        void ExecuteJoin(IDPHandler *idp_handler, join_design_t join_design);
        void PrepareJoin(IDPHandler *idp_handler, join_design_t join_design);
        void StartJoin(IDPHandler *idp_handler, join_design_t join_design);
        void FinishJoin(IDPHandler *idp_handler);
    };

    // utils
    int UpdateSharedHistogramFixedPriority(GlobalBuffer_t *hist_buff, int64_t value, int num_ranks, int my_rank_id);
    int UpdateRankGroupAwareSharedHistogramFixedPriority(GlobalBuffer_t *hist_buff, int64_t value, int num_ranks, int my_rank_id);
    int job_priority_func(IDPHandler *idp_handler, int src_rank, int target_rank, job_priority_func_opt_t option);
}
