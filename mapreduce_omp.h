#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <queue>
#include <iterator>
#include <omp.h>
#include <functional>

using namespace std;

map<string,int> mapreduce_omp(int num_threads,int mpi_size, int mpi_rank);
