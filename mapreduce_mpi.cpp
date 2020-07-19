//Author: Yashwanth Reddy Thoom
//Date: May 05,2020
//Filename: mapreduce_omp.cpp
//Description:
//Main function that performs mapreduce to count the number of occurences of each unique word in a list of files
//listed in list.txt. Every process here calls mapreduce_omp.cpp and divides the files amongst the processes. The function 
//finally prints the output (count of each uniqe word) onto the stdout.

//Input: Nothing ( No Command line arguments)
//Output: Printed on terminal

#include "mapreduce_omp.h"
#include <mpi.h>
#include <cstring>
#define numthread 1

//Main Function
int main(int argc, char** argv)
{
  using namespace std;
  int myrank, mpi_size, interval_calculated;
  int i, j;
  double time1, time2, time3;  // for timing measurements

  vector<queue<pair<string,int>>> local_queues; // vector for storing the data to be processed(Here pair is used to store twoheterogenous values)
  
  int* word_count_proc; 
  int* final_word_count_proc; 

  //MPI initialization
  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
  {    /* Printing the processor on specific server*/
		    char name[MPI_MAX_PROCESSOR_NAME];
			int length;
			MPI_Get_processor_name(name, &length);
			printf("Processor %d is on %s\n",myrank,name);

  }
  MPI_Comm_size(MPI_COMM_WORLD,&mpi_size);
 // MPI_Status ireq[128];
  MPI_Status status;	// To check the status of the request
  time1 = -1*MPI_Wtime();  
  
  //Dynamic memory allocation for the buffers used to store the data
  word_count_proc = (int*)malloc(mpi_size*sizeof(int));
  final_word_count_proc = (int*)malloc(mpi_size*sizeof(int));

  //Input map is used to get the output of the read file using the threads.
  map<string,int> input_map;
  map<string,int> finalMap; //final output is stored in this map
  
  int num_threads = numthread; //Number of threads
 
 
  //OpenMP function mapreduce_omp is being called to process the input_data
  input_map = mapreduce_omp(num_threads,mpi_size,myrank);

  if(myrank == 0)
	  cout << "When Rank is '0' the Number of Nodes: "<<mpi_size<<endl;

  //Create empty queues and push data into a vector
  for(int i = 0 ; i < mpi_size; i++)
  {
    local_queues.push_back(queue<pair<string,int>>());
  }
  
  hash<string> hash_fn;			//hash function
  size_t hash_out;
  
  //use hash function to determine which word goes in to the process queue
  for(map<string,int>::iterator it = input_map.begin(); it != input_map.end(); ++it)
  {
      string word = it->first;
      int count_of_word = it->second;
      hash_out = hash_fn(word); // calculate hash key
      int identifier = hash_out % mpi_size; //hash index
      local_queues[identifier].push(make_pair(word, count_of_word));//store the pairs in the local queues using hash index
  }

  //total numbers of words to be sent to other processes 
  int total_send_count = 0;
  
  for(int i = 0 ; i < mpi_size; i++)
  {
    word_count_proc[i] = local_queues[i].size();
    if(i!=myrank)
        total_send_count+=local_queues[i].size();
  }

  /*MPI Reduce All( combines values from all processors and distibite the result back to all processes.)*/
  MPI_Allreduce(word_count_proc, final_word_count_proc, mpi_size, MPI_INT, MPI_SUM,MPI_COMM_WORLD);

  MPI_Barrier(MPI_COMM_WORLD); 
  
  free(word_count_proc);
  free(final_word_count_proc); //deallocate the memory using free command

 MPI_Barrier(MPI_COMM_WORLD);
 
  /*Now every process knows how many words to receive.So,set a flag for sending and receiving all words, once set then exit loop
   Suppose, if mpi rank is x, it will receive x times and then send to other processors. */
  
  //for each queue, get a word and its count.
  for(int j=0;j<myrank;j++)
  {
         MPI_Status status; 
         int queue_size;
         int char_length;
         char *word_array;
      
         MPI_Recv(&queue_size, 1, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
                  
         while(queue_size-- >0)
         {         
           //receive word length
           MPI_Recv(&char_length, 1, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
		   
           //allocating memory to new word_array             
           word_array = (char *)malloc(sizeof(char)*char_length);
		   
           //receive the words
           MPI_Recv(word_array, char_length, MPI_CHAR, j, 1, MPI_COMM_WORLD, &status);
		   
           int word_count;
		   
           //Convert Char array to string
           string word_string(word_array);
		   
           //receive word count
           MPI_Recv(&word_count, 1, MPI_INT, j, 2, MPI_COMM_WORLD, &status);
           
           // ADD IT TO THE MAP
            map<string,int>::iterator keyfind = finalMap.find(word_string);
			
            if (keyfind == finalMap.end())
            {
                finalMap.insert(make_pair(word_string,word_count));
            }
                   
            //Insert
            else
            {
                (keyfind->second) = (keyfind->second) + word_count;
            }

 	          free(word_array);
            
        }
   }
               
         
  for(int i = 0; i < mpi_size ; i++)
  {
      if(i!=myrank)
      {
         int queue_size = local_queues[i].size();	//initializating the queue_size
      
         MPI_Send(&queue_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
         
         while(!local_queues[i].empty())
         {
			
            pair<string, int> word_count_pair = local_queues[i].front();   //Stroing the data from queue into pairs
			
            string word = word_count_pair.first;      
			
            int count = word_count_pair.second;
            
            //To send String as c-string along with length
            const char* temp_word = word.c_str();
			
            int char_length = strlen(temp_word) + 1;
            
            MPI_Send(&char_length, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                        
            MPI_Send((void*)temp_word, char_length, MPI_CHAR, i, 1, MPI_COMM_WORLD);
            
            //send word count
            MPI_Send(&count, 1, MPI_INT, i, 2, MPI_COMM_WORLD);
			
            local_queues[i].pop();           
         }
        
      }
      else //if myrank == i
      {
      
         while(!local_queues[i].empty())
         {
            
            string word_string = local_queues[i].front().first;
            int word_count = local_queues[i].front().second;
			
            // ADD IT TO THE MAP
            map<string,int>::iterator keyfind = finalMap.find(word_string);
            if (keyfind == finalMap.end())
            {
                finalMap.insert(make_pair(word_string,word_count));
            }
                   
            //Insert
            else
            {
                (keyfind->second) = (keyfind->second) + word_count;
            }
            
            local_queues[i].pop(); //popping data from local_queues
         }
      }      
  }
  
  //Final Reduction
  for(int j=myrank+1;j < mpi_size;j++)
  {
                             
         MPI_Status status; 
  
         int queue_size;
         int char_length;
      
         MPI_Recv(&queue_size, 1, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
         
         while(queue_size-- >0)
         {         
           //receive word length
           MPI_Recv(&char_length, 1, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
     
           char *word_array;		   
           //allocating memory to new word_array 
           word_array = (char *)malloc(sizeof(char)*char_length);
		   
           //receive the word
           MPI_Recv(word_array, char_length, MPI_CHAR, j, 1, MPI_COMM_WORLD, &status);
             
           int word_count;
           
           //Convert Char array to string
           string word_string(word_array);
                 
           //receive word count
           MPI_Recv(&word_count, 1, MPI_INT, j, 2, MPI_COMM_WORLD, &status);
             
            // ADD IT TO THE MAP
            map<string,int>::iterator keyfind = finalMap.find(word_string);
            if (keyfind == finalMap.end())
            {
                finalMap.insert(make_pair(word_string,word_count));
            }
                   
            //Insert
            else
            {
                (keyfind->second) = (keyfind->second) + word_count;
            }
		
	    free(word_array);
     }
  }
  
  
  MPI_Barrier(MPI_COMM_WORLD); //Blocks until all processes in the communicator have reached this routine
  
  time2 = time1 + MPI_Wtime();

  if(myrank ==0)
  {
	cout<<"Total MPI time: "<<time2<<endl;
	/*for (auto it = finalMap.cbegin(); it != finalMap.cend(); ++it) {
        std::cout << "{" << (*it).first << ": " << (*it).second << "}\n";
    }*/
	map<string, int>::iterator itr; 
    cout << "\nThe finalMap is : \n"; 
    cout << "\tKEY\tELEMENT\n"; 
    for (itr = finalMap.begin(); itr != finalMap.end(); ++itr) { 
        cout << '\t' << itr->first 
             << '\t' << itr->second << '\n'; 
    } 
    cout << endl; 
	fflush(stdout); //if there are any null pointer streams are flushed
  }
  
  MPI_Finalize(); //Terminate the MPI_Environment
  
  return 0;
 } 

