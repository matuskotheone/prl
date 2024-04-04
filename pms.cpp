/*  
 *  Pipeline Merge Sort using openMPI
 *  Author: Matus Gazdik xgazdi04
 *  Date: 2024-04-04
 *  Description:
 *  This program reads 8bit numbers from file numbers prints them before sort and then sorts them while printing them in
 *  separate lines. The program uses openMPI to create a pipeline of workers that sort the numbers. First the worker
 *  with rank 0 reads the numbers from file and prints them. Then the same worker sends the numbers to next worker with
 *  a tag signalizing which queue to use. The workers continue the sorting and merging in a row until the last worket
 *  which merges the numbers while printing them. For comunication between workers is used MPI_Send and MPI_Recv. and
 *  the length of the numbers is broadcasted to all workers after reading the file.
 *  This program was tested on a virtual machine running fedora 33 with openMPI 4.0.5 max number of wokers i could get
 *  before error was 6.
*/


#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <sstream>
#include <queue>
#include <unistd.h>

#include <mpi.h>

// Name of the file with numbers to sort
const std::string FILENAME = "numbers";

// function to load numbers from file 
void load_numbers(std::vector<int>& numbers) {

    std::ifstream file(FILENAME, std::ios::binary); // Open the file in binary mode

    if (!file.is_open()) {
        std::cerr << "Could not open file " << FILENAME << std::endl;
        return;
    }

    unsigned char number; 

    while (file.read(reinterpret_cast<char*>(&number), sizeof(number))) { // Read the file byte by byte
        numbers.push_back(static_cast<int>(number)); // Convert unsigned char to int and push into vector
    }

    file.close();
}

// function to print numbers
void print_numbers(const std::vector<int>& numbers) {
    for (int number : numbers) {
        std::cout << number << " ";
                fflush(stdout);
    }
    std::cout << std::endl;
                fflush(stdout);
}

// the behavior of the first input it sends the numbers to the first worker while alternating between queues to send to 
void input_processor(std::vector<int>& numbers, int size) {
    int currenq = 0; // start sending to queue 0
    for (int i = 0; i < numbers.size(); i++) {
        MPI_Send(&numbers[i], 1, MPI_INT, 1, currenq, MPI_COMM_WORLD);
        currenq = ++currenq % 2;
    }
}

// the behavior of the output processor it is the processor with the highest rank has 2 queues as well as workers but
// instead of sending the numbers it prints them
// the arguments are the size of the world, the rank of the processor and the number of numbers to be recieved
void output_processor(int size, int rank, int num_numbers) {
    std::queue<int> q1;
    std::queue<int> q2;
    bool working = false; // flag to signal if the starting condition is met
    int recieved = 0; // counter for recieved numbers


    
    // the loop will continue until all numbers are recieved
    while (!working || !q1.empty() || !q2.empty() || recieved < num_numbers)
    {
        int number; // variable to store the recieved number
        MPI_Status status; // variable to store the status of the recieved message

        if (recieved < num_numbers) {
            MPI_Recv(&number, 1, MPI_INT, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status); // recieving a number from the previous worker
            int rank = status.MPI_TAG; // getting the tag to know which queue to push the number to

            if (rank == 0) {
                q1.push(number); // push the number to the first queue
            }
            else {
                q2.push(number); // push the number to the second queue
            }
            recieved++; // increment the recieved counter
        }

        if (working || ((q1.size() >= pow(2, rank-1) && q2.size() >= 1) || (q2.size() >= pow(2, rank-1) && q1.size() >= 1))) { // check if the starting condition is met
            working = true; // set the working flag to true
        }

        if (working) { // can be printing
            if (!q1.empty() && !q2.empty()) { // if both queues are not empty
                int n1 = q1.front(); // get the first number from the first queue
                int n2 = q2.front(); // get the first number from the second queue

                if (n1 < n2) { // compare the numbers
                    q1.pop(); // pop the number from the first queue
                    std::cout << n1 << std::endl; // print the number
                    fflush(stdout); // flush the output
                } 
                else {
                    q2.pop(); // pop the number from the second queue
                    std::cout << n2 << std::endl; // print the number
                    fflush(stdout); // flush the output
                }
            } 
            else if (!q1.empty()) { // this condition should only be met when we are printing the last number
                int n1 = q1.front();
                q1.pop();
                std::cout << n1 << std::endl;
            } 
            else if (!q2.empty()) {
                int n2 = q2.front();
                q2.pop();
                std::cout << n2 << std::endl;
                fflush(stdout);
            }
        }
    }
}


// function to send the number to the next worker
// the arguments are the rank of worker sending the number, the queue from which to sent the number, the counter of
// numbers to be increased, the recieve queue index to be recalculated with each sent number and the frequency of changing that is calculated at the start of the worker
void worker_send(int rank, std::queue<int>& queue, int& toIncrease, int& recieve_queue, int change_queue_freq) {
    int num = queue.front();// reads the numeer
    queue.pop(); // pops the number
    MPI_Send(&num, 1, MPI_INT, rank + 1, recieve_queue / change_queue_freq, MPI_COMM_WORLD); // sends the number to the next worker
    recieve_queue = (recieve_queue + 1) % (2 * change_queue_freq); // recalculates the recieve queue index
    toIncrease++; // increments the counter of numbers
}


void worker(int rank, int size, int num_numbers) {
    int recieved = 0; // counter for recieved numbers
    std::queue<int> q1; // first queue
    std::queue<int> q2; // second queue
    bool working = false; // flag to signal if the starting condition is met
    int change_queue_freq = (pow(2, rank)); // frequency of changing the recieve queue the workers change recieve queue every 2^rank numbers
    int recieve_queue = 0; // index of the recieve queue

    int numInFirst = 0; // counter for numbers that were sent to the first queue in current cluster
    int numInSecond = 0; // counter for numbers that were sent to the second queue in current cluster
    int cluster = pow(2, rank-1); // size of the cluster 


    while (!working || !q1.empty() || !q2.empty() || recieved < num_numbers){ // loop until all numbers are recieved
        int number; // variable to store the recieved number
        MPI_Status status; // variable to store the status of the recieved message


        if (recieved < num_numbers) { // if there are still numbers to be recieved

            MPI_Recv(&number, 1, MPI_INT, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status); // recieve the number from the previous worker
            recieved++; // increment the recieved counter

            int queue = status.MPI_TAG; // get the tag to know which queue to push the number to

            if (queue == 0) { 
                q1.push(number); // push the number to the first queue
            }
            else {
                q2.push(number); // push the number to the second queue
            }
        } 


        if (working || ((q1.size() >= pow(2, rank-1) && q2.size() >= 1) || (q2.size() >= pow(2, rank-1) && q1.size() >= 1))) { // check if the starting condition is met
            working = true; // set the working flag to true
        }


        if (working) {
            if (!q1.empty() && !q2.empty()) { // if both queues are not empty
                // reads the numbers 
                int n1 = q1.front(); 
                int n2 = q2.front();
                


                if (numInFirst == cluster && numInSecond == cluster) { // if the cluster is done 
                    numInFirst = 0;
                    numInSecond = 0;
                }

                // this happens if we sent too much numbers from one queue so we have to send from the other queue
                // despite which number is smaller
                if ((numInFirst == cluster && numInSecond < cluster) || (numInSecond == cluster && numInFirst < cluster)) { 
                    if (numInFirst > numInSecond) {
                        worker_send(rank, q2, numInSecond, recieve_queue, change_queue_freq);
                    } else {
                        worker_send(rank, q1, numInFirst, recieve_queue, change_queue_freq);
                    }
                    continue;
                }

                // compare the numbers and send the smaller one
                if (n1 < n2) {
                    worker_send(rank, q1, numInFirst, recieve_queue, change_queue_freq);
                } else {
                    worker_send(rank, q2, numInSecond, recieve_queue, change_queue_freq);
                }
            } 
            // thiese conditions are for the last number
            else if (!q1.empty()) {
                worker_send(rank, q1, numInFirst, recieve_queue, change_queue_freq);
            } 
            else if (!q2.empty()) {
                worker_send(rank, q2, numInSecond, recieve_queue, change_queue_freq);
            }
        }
    }
}

// all processors will call this function to do their work and it splits the work based on the rank
void work(std::vector<int>& numbers, int rank, int size, int num_numbers) {
    // rank 0 is the input processor
    if (rank == 0) {
        input_processor(numbers, size);
    }
    // rank size - 1 is the output processor
    else if (rank == size - 1) {
        output_processor(size, rank, num_numbers);
    }
    // all other ranks are workers
    else {
        worker(rank, size, num_numbers);
    }
}

int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);

    // Get the rank and size of the world
    int rank, size, numbers_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Vector to store the numbers
    std::vector<int> numbers;


    // Rank 0 reads the numbers from file prints them and broadcasts the size of the numbers
    if (rank == 0) {
        load_numbers(numbers);
        print_numbers(numbers);
        numbers_size = numbers.size();
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }
    else {
        // other rank wait for the broadcast
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }

    // call the work function
    work(numbers, rank, size, numbers_size);


    // Finalize MPI
    MPI_Finalize();
}
