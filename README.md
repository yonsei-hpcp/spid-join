# SPID-Join \[SIGMOD '25\]

This repository contains the source code for [SPID-Join \[SIGMOD '25\]](https://doi.org/10.1145/3698827), a skew-resistant processing-in-DIMM join algorithm exploiting the bank- and rank-level parallelisms of DIMMs.
Please cite the following paper if you utilize SPID-Join in your research.

```bibtex
@article{lee2025spidjoin,
  author  = {Suhyun Lee and Chaemin Lim and Jinwoo Choi and Heelim Choi and Chan Lee and Yongjun Park and Kwanghyun Park and Hanjun Kim and Youngsok Kim},
  title   = {{SPID-Join: A Skew-resistant Processing-in-DIMM Join Algorithm Exploiting the Bank- and Rank-level Parallelisms of DIMMs}},
  journal = {Proceedings of the ACM on Management of Data (PACMMOD)},
  volume  = {2},
  number  = {6},
  year    = {2024},
}
```

## System Configuration

- Intel Xeon Gold 5222 CPU
- 1 DDR4 channel with 2 64-GB DDR4-2400 DIMMs
- 4 DDR4 channels, each with two UPMEM DIMMs
- Ubuntu 18.04 (x64)

## Prerequisites

- g++
- python3.6
- matplotlib
- numpy
- Pandas
- Scipy
- The driver for UPMEM SDK (version 2021.3, available from the [UPMEM website](https://sdk.upmem.com/).)

## Directories

- `src/`
    - The source codes for libspidjoin.so
- `test/`
    - The test srcs for SPID-Join
- `upmem-2021.3.0-Linux-x86_64/`
    - The modified UPMEM SDK version 2021.3 for SPID-Join

## Download the UPMEM SDK
```
# firstly download upmem sdk then,
cp -r {your upmem sdk dir}/lib {your spid-join dir}/upmem-2021.3.0-Linux-x86_64/;
cp -r {your upmem sdk dir}/share {your spid-join dir}/upmem-2021.3.0-Linux-x86_64/;
```

## Environment Setup
```
cd {your spid-join dir};
source ./scripts/upmem_env.sh
```

## Build UPMEM SDK
```
cd {your spid-join dir};
cd upmem-2021.3.0-Linux-x86_64/src/backends/;
./load.sh
```

## Build SPID-Join Library
```
cd {your spid-join dir};
make lib -j
```

## Build tests for SPID-Join
```
cd {your spid-join dir};
make test -j
```

## Run tests for SPID-Join

### Required Arguments
- `-s`
    - Input size of table R [The number of tuples]
- `-r`    
    - The number of ranks to use
- `-z`    
    - Zipf factor to use; should pass the argument as Zipf factor * 100 (e.g., if you want to test zipf factor 1.0; -z 100)
- `-t`    
    - R-S Ratio to test; (e.g., if you want to test R:S=1:2; -t 2)
- `-R`    
    - The number of rank set to use
- `-B`    
    - The number of bank set to use
