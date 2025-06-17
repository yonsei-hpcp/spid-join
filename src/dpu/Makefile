DPU_DIR := src
HOST_DIR := host
BUILDDIR ?= bin
NR_TASKLETS ?= 16
BL ?= 10
NR_DPUS ?= 1
ENERGY ?= 0

GLB_PARTITION_COUNT := ${BUILDDIR}/glb_partition_count
SRC_GLB_PARTITION_COUNT := ${DPU_DIR}/join/glb_partition_count.c ${DPU_DIR}/hash.c

GLB_PARTITION_PACKET := ${BUILDDIR}/glb_partition_packet
SRC_GLB_PARTITION_PACKET := ${DPU_DIR}/join/glb_partition_packet.c ${DPU_DIR}/hash.c

GLB_CHIPWISE_PARTITION_COUNT := ${BUILDDIR}/glb_chipwise_partition_count
SRC_GLB_CHIPWISE_PARTITION_COUNT := ${DPU_DIR}/join/glb_chipwise_partition_count.c ${DPU_DIR}/hash.c

GLB_CHIPWISE_PARTITION_PACKET := ${BUILDDIR}/glb_chipwise_partition_packet
SRC_GLB_CHIPWISE_PARTITION_PACKET := ${DPU_DIR}/join/glb_chipwise_partition_packet.c ${DPU_DIR}/hash.c

BG_BROADCAST_COUNT := ${BUILDDIR}/bg_broadcast_count
SRC_BG_BROADCAST_COUNT := ${DPU_DIR}/join/bg_broadcast_count.c ${DPU_DIR}/hash.c

BG_BROADCAST_ALIGN := ${BUILDDIR}/bg_broadcast_align
SRC_BG_BROADCAST_ALIGN := ${DPU_DIR}/join/bg_broadcast_align.c ${DPU_DIR}/hash.c

DEBUGGER := ${BUILDDIR}/debugger
SRC_DEBUGGER := ${DPU_DIR}/debugger.c

A2A_SRJ_GLOBAL_PARTITION := ${BUILDDIR}/a2a_srj_global_partition
A2A_SRJ_LOCAL_PARTITION := ${BUILDDIR}/a2a_srj_local_partition
A2A_PACKETWISE_SRJ_LOCAL_PARTITION := ${BUILDDIR}/a2a_packetwise_srj_local_partition
A2A_PACKETWISE_SRJ_GLOBAL_PARTITION := ${BUILDDIR}/a2a_packetwise_srj_global_partition

SRC_A2A_SRJ_GLOBAL_PARTITION = ${DPU_DIR}/join/a2a_srj_global_partition.c ${DPU_DIR}/hash.c
SRC_A2A_SRJ_LOCAL_PARTITION = ${DPU_DIR}/join/a2a_srj_local_partition.c ${DPU_DIR}/hash.c
SRC_A2A_PACKETWISE_SRJ_LOCAL_PARTITION = ${DPU_DIR}/join/a2a_packetwise_srj_local_partition.c ${DPU_DIR}/hash.c
SRC_A2A_PACKETWISE_SRJ_GLOBAL_PARTITION = ${DPU_DIR}/join/a2a_packetwise_srj_global_partition.c ${DPU_DIR}/hash.c

# Hash Table Build Probe

A2A_SRJ_BUILD_LINEAR_PROBE := ${BUILDDIR}/a2a_srj_build_linear_probe
A2A_SRJ_PROBE_INNER_LINEAR_PROBE := ${BUILDDIR}/a2a_srj_probe_inner_mt_linear_probe
A2A_SRJ_PROBE := ${BUILDDIR}/a2a_srj_probe

SRC_A2A_SRJ_BUILD_LINEAR_PROBE = ${DPU_DIR}/join/a2a_srj_build.c ${DPU_DIR}/hash.c
SRC_A2A_SRJ_PROBE_INNER_LINEAR_PROBE = ${DPU_DIR}/join/a2a_srj_probe_inner_mt.c ${DPU_DIR}/hash.c
SRC_A2A_SRJ_PROBE = ${DPU_DIR}/join/a2a_srj_probe.c

# Nested Loop Join
NESTED_LOOP_JOIN := ${BUILDDIR}/nested_loop_join
SRC_NESTED_LOOP_JOIN := ${DPU_DIR}/join/nested_loop_join.c

NO_PARTITIONED_BUILD = ${BUILDDIR}/packetwise_no_partitioned_hash_join_build
NO_PARTITIONED_PROBE_INNER = ${BUILDDIR}/packetwise_no_partitioned_hash_join_probe

SRC_NO_PARTITIONED_BUILD = ${DPU_DIR}/join/packetwise_no_partitioned_hash_join_build.c ${DPU_DIR}/hash.c
SRC_NO_PARTITIONED_PROBE_INNER = ${DPU_DIR}/join/packetwise_no_partitioned_hash_join_probe.c ${DPU_DIR}/hash.c

# Sort Merge Join

MPSM_JOIN_LOCAL_PARTITION := ${BUILDDIR}/mpsm_join_local_partition
MPSM_JOIN_SORT_PROBE := ${BUILDDIR}/mpsm_join_sort_probe

SRC_MPSM_JOIN_LOCAL_PARTITION := ${DPU_DIR}/join/mpsm_join_local_partition.c
SRC_MPSM_JOIN_SORT_PROBE := ${DPU_DIR}/join/mpsm_join_sort_probe.c

FINISH_JOIN := ${BUILDDIR}/finish_join

SRC_FINISH_JOIN := ${DPU_DIR}/join/finish_join.c
##############################

COMMON_INCLUDES := ./include

.PHONY: all clean

__dirs := $(shell mkdir -p ${BUILDDIR})

COMMON_FLAGS := -Wall -Wextra -g -I${COMMON_INCLUDES}
DPU_FLAGS := ${COMMON_FLAGS} -O3 -DDEBUG -DSTACK_SIZE_DEFAULT=256 -DCRC -DVALIDATION

all: \
$(A2A_SRJ_GLOBAL_PARTITION) \
$(NO_PARTITIONED_BUILD)\
$(NO_PARTITIONED_PROBE_INNER)\
$(A2A_SRJ_LOCAL_PARTITION)  \
$(A2A_SRJ_BUILD_LINEAR_PROBE)  \
$(A2A_SRJ_PROBE_INNER_LINEAR_PROBE) \
$(A2A_PACKETWISE_SRJ_LOCAL_PARTITION)  \
${FINISH_JOIN} \
$(GLB_PARTITION_COUNT) \
$(GLB_PARTITION_PACKET) \
$(GLB_CHIPWISE_PARTITION_COUNT) \
$(GLB_CHIPWISE_PARTITION_PACKET) \
$(BG_BROADCAST_COUNT) \
$(BG_BROADCAST_ALIGN) \
$(MPSM_JOIN_LOCAL_PARTITION) \
$(MPSM_JOIN_SORT_PROBE) \
$(NESTED_LOOP_JOIN) \


${CONF}:
	$(RM) $(call conf_filename,*,*)
	touch ${CONF}

${MPSM_JOIN_PARTITION}: ${SRC_MPSM_JOIN_PARTITION} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_MPSM_JOIN_PARTITION}

${MPSM_JOIN_LOCAL_PARTITION}: ${SRC_MPSM_JOIN_LOCAL_PARTITION} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_MPSM_JOIN_LOCAL_PARTITION}
${MPSM_JOIN_SORT_PROBE}: ${SRC_MPSM_JOIN_SORT_PROBE} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_MPSM_JOIN_SORT_PROBE}

${FINISH_JOIN}: ${SRC_FINISH_JOIN} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_FINISH_JOIN}
######################################################

${NO_PARTITIONED_BUILD}: ${SRC_NO_PARTITIONED_BUILD} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_NO_PARTITIONED_BUILD}

${NO_PARTITIONED_PROBE_INNER}: ${SRC_NO_PARTITIONED_PROBE_INNER} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_NO_PARTITIONED_PROBE_INNER}

${DEBUGGER}: ${SRC_DEBUGGER} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_DEBUGGER} 

######################################################
${GLB_PARTITION_COUNT}: ${SRC_GLB_PARTITION_COUNT} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_GLB_PARTITION_COUNT} 

${GLB_PARTITION_PACKET}: ${SRC_GLB_PARTITION_PACKET} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_GLB_PARTITION_PACKET} 

${GLB_CHIPWISE_PARTITION_COUNT}: ${SRC_GLB_CHIPWISE_PARTITION_COUNT} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_GLB_CHIPWISE_PARTITION_COUNT} 

${GLB_CHIPWISE_PARTITION_PACKET}: ${SRC_GLB_CHIPWISE_PARTITION_PACKET} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_GLB_CHIPWISE_PARTITION_PACKET} 

${A2A_SRJ_GLOBAL_PARTITION}: ${SRC_A2A_SRJ_GLOBAL_PARTITION} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_A2A_SRJ_GLOBAL_PARTITION} 

${A2A_SRJ_GLOBAL_PARTITION_PREV}: ${SRC_A2A_SRJ_GLOBAL_PARTITION_PREV} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=${NR_TASKLETS} -o $@ ${SRC_A2A_SRJ_GLOBAL_PARTITION_PREV}

${A2A_SRJ_LOCAL_PARTITION}: ${SRC_A2A_SRJ_LOCAL_PARTITION} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_A2A_SRJ_LOCAL_PARTITION} 

${A2A_PACKETWISE_SRJ_LOCAL_PARTITION}: ${SRC_A2A_PACKETWISE_SRJ_LOCAL_PARTITION} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_A2A_PACKETWISE_SRJ_LOCAL_PARTITION} 

${A2A_PACKETWISE_SRJ_GLOBAL_PARTITION}: ${SRC_A2A_PACKETWISE_SRJ_GLOBAL_PARTITION} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_A2A_PACKETWISE_SRJ_GLOBAL_PARTITION} 

${NESTED_LOOP_JOIN}: ${SRC_NESTED_LOOP_JOIN} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_NESTED_LOOP_JOIN}

######################################################

${BG_BROADCAST_COUNT}: ${SRC_BG_BROADCAST_COUNT} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_BG_BROADCAST_COUNT} 

${BG_BROADCAST_ALIGN}: ${SRC_BG_BROADCAST_ALIGN} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_BG_BROADCAST_ALIGN}

######################################################

${A2A_SRJ_BUILD_LINEAR_PROBE}: ${SRC_A2A_SRJ_BUILD_LINEAR_PROBE} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -DLINEAR_PROBE -o $@ ${SRC_A2A_SRJ_BUILD_LINEAR_PROBE} 

${A2A_SRJ_PROBE}: ${SRC_A2A_SRJ_PROBE} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -o $@ ${SRC_A2A_SRJ_PROBE}

${A2A_SRJ_PROBE_INNER_LINEAR_PROBE}: ${SRC_A2A_SRJ_PROBE_INNER_LINEAR_PROBE} ${COMMON_INCLUDES} ${CONF}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} -DNR_TASKLETS=12 -DLINEAR_PROBE -o $@ ${SRC_A2A_SRJ_PROBE_INNER_LINEAR_PROBE}

######################################################

clean:
	$(RM) -r $(BUILDDIR)

