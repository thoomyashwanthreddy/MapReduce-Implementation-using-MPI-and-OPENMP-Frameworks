//Author: Yashwanth Reddy Thoom
//Description:
//Has a function that takes performs a mapreduce operation to get a frequency count on the words in a subset of files 
//listed in list.txt. The subset is determined by mpi_rank (rank of the calling process) and mpi_size. 
//Input: It takes 3 inputs. i) Number of threads, ii) mpi_size (no: of processes in the group) and iii) mpi_rank (rank of the calling process)
//Output: Output of omp_mapreduce. Counts of all the unique words in the read files.

#include "mapreduce_omp.h"
#include <iostream> 
#include <iterator> 
#include <map> 


std::map<std::string,int> mapreduce_omp(int num_threads, int mpi_size, int mpi_rank)
{
	using namespace std;
	
    cout<<"mpi size is "<<mpi_size<<" mpi rank is "<<mpi_rank<<endl;
	
    map<string,int> finalMap; //final output
    map<string,int> wordMap; 
	
    bool allPushed = false; // Falg to check if all items have been pushed into a queue
    //input stream
    ifstream file;
    //vector to store list of files
    vector<string> fileList;
   
    queue<string> wordStream; //This is the queue where readers add read data
    //open a file
    file.open("list.txt");
    string fileName;
    
    //To make sure that only one (even or odd) thread will print the output
    int evencout = 0;
    int oddcout = 0;
    
    //Stuff to be added inside main, but before the start of parallelization
    //array of queues for the reducers
    vector<queue<pair<string,int>>> reducerQueues; // vector of queues
	
    int num_e_threads;
    int number_of_threads = num_threads;
    omp_set_dynamic(0);  //disables dynamic adjustment of the number of threads
    omp_set_num_threads(number_of_threads);		//set number of threads
    
    
    #pragma omp parallel  //explicitly instructs the compiler to parallelize block of code.
    {
        if(mpi_rank == 0)
        {
            if(omp_get_thread_num() == 0)
            {
                cout << "Number of threads: "<< omp_get_num_threads()<<endl;
            }
        }
        #pragma omp critical
        {
            num_e_threads = (omp_get_num_threads() +1) /2; //Number of threads with even indices
        }
    }
    
   // cout<<"num e threads "<<num_e_threads<<endl;
    
    //Lock for reducer queues
    omp_lock_t reducerQueues_lock[num_e_threads];
    //Lock for word outputs
    omp_lock_t wordStream_lock;
    //Lock for word outputs initialization
    omp_init_lock(&wordStream_lock);
    //Lock for word outputs
    omp_lock_t finalMap_lock;
    //Lock for word outputs initialization
    omp_init_lock(&finalMap_lock);
    
    for(int j=0; j<num_e_threads; j++)
    {
        reducerQueues.push_back(queue<pair<string,int>>()); // add a queue to the vector
        //Lock for reducer queues initialization
        omp_init_lock(&reducerQueues_lock[j]);
    }
    
    //datasent for reducers to keep track of which mappers have finished sending data
    unsigned short datasent=0;
    // cout<<" num of thread: "<<number_of_threads<<endl;
    // cout<<"num e threads :"<<num_e_threads<<endl;
	
    //build a list of input files
    while(getline(file, fileName))
    {
        fileList.push_back(fileName);
        cout<<fileName<<endl;
    }
    
    if(mpi_rank == 0 && omp_get_thread_num() == 0)
    cout << "Total files read: "<<fileList.size()<<endl;
    file.close();
	
    int filenum = fileList.size()-1;
	
    double finaltime;
    finaltime = -omp_get_wtime();
    
	/*list to be shared among all the threads in shared region*/
    #pragma omp parallel shared(filenum, wordStream, evencout, oddcout) private(wordMap)
    {
        double readmaptime;
        readmaptime = -omp_get_wtime();
        //cout<<"Num_Per_threads: "<<omp_get_num_threads()<<endl;
		
        while(1)
        {
			//if even threads consider them as Reader Threads (later considered as Reducers)
            if(omp_get_thread_num()%2 ==0) // Reads the data and insert into work_queue
            {
                 //cout<<"My thread num is "<<omp_get_thread_num()<<" proc num is "<< mpi_rank << endl;
                //cout<<"file num:"<<filenum<< ": "<< omp_get_thread_num()<<endl;
                if(filenum < 0)
                {
                    #pragma omp critical
                    {
                        allPushed = true;
                       // cout <<omp_get_thread_num()<< "-> set all Pushed" <<endl;
                    }
                    break;
                }
                ifstream file;
                string word;
                int fileOpen = 0;
				
                #pragma omp critical
                {
                    //change for MPI
                    if(filenum >= 0 && (filenum-- % mpi_size == mpi_rank))
                    {
                        filenum++;
                        cout<<"filenum is "<<filenum + 1<<" my rank is "<<mpi_rank<<endl;
                        cout<<"Opening file: "<<fileList[filenum]<<endl;
                        file.open(fileList[filenum]);
                        if(!file.good())
                        {
                            cout<<fileList[filenum]<<" -> File is wrong"<<endl;
                        }
                        
                        filenum--;
                        fileOpen = 1;
                    }
                }
                if(fileOpen)
                {
                    while ( file >> word )
                    {
                        //cout<<word<<endl;
                        omp_set_lock(&wordStream_lock);
                        wordStream.push(word);
                        omp_unset_lock(&wordStream_lock);
                    }
                    file.close();
                    if(filenum < 0)
                    {
                        #pragma omp critical
                        {
                            allPushed = true;
                          //  cout <<omp_get_thread_num()<< "-> set all Pushed" <<endl;
                        }
                    }
                }
                if(number_of_threads == 1)
                {
                    while(filenum>=0)
                    {
                        cout<<"Opening file: "<<fileList[filenum]<<endl;
                        file.open(fileList[filenum]);
                        if(!file.good())
                        {
                            cout<<fileList[filenum]<<" -> File is wrong"<<endl;
                        }
                        // //cout<<"file num:"<<filenum<<endl;
                        filenum--;
                        fileOpen = 1;
                        while ( file >> word )
                        {
                            omp_set_lock(&wordStream_lock);
                            wordStream.push(word);
                            omp_unset_lock(&wordStream_lock);
                        }
                        file.close();
                    }
                    allPushed = true;
                }
            }
            
            //Odd Thread Execution Mappers(these execute parallel in Reader threads)
            if(number_of_threads == 1 || (omp_get_thread_num()%2!=0))
            {
                 string w;
                int newword = 0;
                omp_set_lock(&wordStream_lock);
                if(wordStream.empty() && allPushed)
                {
                    omp_unset_lock(&wordStream_lock);
                    break;
                }
                if(!wordStream.empty())
                {
                    newword = 1;
                    w = wordStream.front();
                    wordStream.pop();
                }
                omp_unset_lock(&wordStream_lock);
                if(newword && number_of_threads>1)
                {
                    map<string,int>::iterator keyfind = wordMap.find(w);
                    if (keyfind == wordMap.end())
                    {
                        wordMap.insert(make_pair(w,1));
                    }
                    //Insert
                    else
                    {
                        (keyfind->second)++;
                    }
                }
                else if(newword && number_of_threads == 1)
                {
                    map<string,int>::iterator keyfind1t = wordMap.find(w);
                    if (keyfind1t == wordMap.end())
                    {
                        wordMap.insert(make_pair(w,1));
                    }
                    //Insert
                    else
                    {
                        (keyfind1t->second)++;
                    }
                    omp_set_lock(&wordStream_lock);
                    while(!wordStream.empty())
                    {
                        w = wordStream.front();
                        wordStream.pop();
                        keyfind1t = wordMap.find(w);
                        if (keyfind1t == wordMap.end())
                        {
                            wordMap.insert(make_pair(w,1));
                        }
                        //Insert
                        else
                        {
                            (keyfind1t->second)++;
                        }
                    }
                    omp_unset_lock(&wordStream_lock);
                    //wordstream empty
                    break;
                }
            }
        }
        readmaptime += omp_get_wtime();
        
        #pragma omp critical  /*identifies a section of code that must be executed by a single thread at a time*/
        {
            if(number_of_threads != 1)
            {
                if(omp_get_thread_num()%2 == 0 && evencout++ < 1)
                {
                    cout<< "Reading time is: "<<readmaptime<<endl;
                }
                else if (omp_get_thread_num()%2 == 1 && oddcout++ < 1)
                {
                    cout<< "Mapping time is: "<<readmaptime<<endl;
                }
            }
            else
            {
                cout<< "Read-Map time is: "<<readmaptime<<endl;
            }
        }
		
        fflush(stdout); //if there are any null pointer streams those are flushed
		
        #pragma omp barrier /* threads in paralel region will not execute beyond the barrier*/
        omp_destroy_lock(&wordStream_lock);
        //Even numbers are reducers and Odd numbers are mappers
        //Done with reading and mapping. Now, moving onto reducing and writing
        if(omp_get_thread_num()%2 ==1 || number_of_threads == 1) // Mappers
        {
            int count_word, identifier;
            string word;
            hash<string> hash_fn;
            size_t hash_out;
            for(map<string,int>::iterator words_it = wordMap.begin(); words_it!=wordMap.end(); words_it++)
            {
                count_word = words_it->second;
                word = words_it->first;
                hash_out= hash_fn(word);
                identifier = hash_out % num_e_threads;	//hashing index
                if(number_of_threads == 0)
                identifier = 0;
                omp_set_lock(&reducerQueues_lock[identifier]);	//set lock
                reducerQueues[identifier].push(make_pair(word,count_word)); // push an element into queue for reducers
                omp_unset_lock(&reducerQueues_lock[identifier]);	//remove lock
            }
            #pragma omp critical
            {
                datasent++;
            }
        }
        //reducers - > even threads
        if(omp_get_thread_num() % 2 == 0)
        {
            map<string, int> word_count_redux;
            while(!reducerQueues[omp_get_thread_num()/2].empty() || (datasent!=num_e_threads))
            {
                omp_set_lock(&reducerQueues_lock[omp_get_thread_num()/2]);
                if(!reducerQueues[omp_get_thread_num()/2].empty())
                {
                    pair<string, int> word_count_pair = reducerQueues[omp_get_thread_num()/2].front();
                    reducerQueues[omp_get_thread_num()/2].pop();
                    omp_unset_lock(&reducerQueues_lock[omp_get_thread_num()/2]);
                    map<string,int>::iterator keyfound = word_count_redux.find(word_count_pair.first);
                    if (keyfound == word_count_redux.end())
                    {
                        word_count_redux.insert(make_pair(word_count_pair.first,word_count_pair.second));
                    }
                    //Insert
                    else
                    {
                        (keyfound->second)+=word_count_pair.second;
                    }
                }
                else
                {
                    omp_unset_lock(&reducerQueues_lock[omp_get_thread_num()/2]);
                }
            }
            //iterate word_count_redux
            for(map<string, int>::iterator iter = word_count_redux.begin(); iter!=word_count_redux.end(); iter++)
            {
                //omp get lock
                omp_set_lock(&finalMap_lock);
                {
                    finalMap.insert(make_pair(iter->first,iter->second));
                }
                omp_unset_lock(&finalMap_lock);
            }
	
	ofstream outfile;
   outfile.open("output.txt");
    if(!outfile)
    {
        cout << "Error in opening a file"<<endl;           //file does not exist and cannot be created.
    }
	
	//stroring the output to the output.txt file
   for(map<string, int>::iterator iter = word_count_redux.begin(); iter!=word_count_redux.end(); iter++)
    {
        outfile<<iter->first<<"|"<<iter->second;
        outfile<<"\n";
    }
   outfile.close();			
	
        }
    }

    for(int i=0;i<num_e_threads;i++)
    omp_destroy_lock(&reducerQueues_lock[i]);
    omp_destroy_lock(&finalMap_lock);
    //omp parallel
    finaltime += omp_get_wtime();
    cout << "Total OMP Time: "<<finaltime<<endl;
    return finalMap;
}
