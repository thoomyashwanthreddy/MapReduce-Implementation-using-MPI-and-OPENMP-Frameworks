# MapReduce-Implementation-using-MPI-and-OPENMP-Frameworks
An approach to implement MapReduce framework on MPI platform. In this approach, inter-process communication is achieved using MPI communication functions.
There are some limitations when solving problems of both data intensive and computing intensive in Traditional MapReduce model.
The message passing of MapReduce model is realized through the low layer of the distributed file system.
It stores all information in disk, and then reads them from the disk while required. 
        
        In order to overcome the limitations of Traditional MapReduce approach, MPI (Message Passing Interface) based MapReduce model is proposed. 
In this project I used MPI communication for cluster level and OpenMp on node level. In this, the master processor takes an input file from the list of files provided as
input &amp; the other processors (workers) wait for the task from the master node. Once the data is reduced by the workers is sent back to the Master and then
final reduced data is printed to the stdout.
