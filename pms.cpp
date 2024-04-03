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

const std::string FILENAME = "numbers";

void print_queue(std::queue<int> q)
{
  while (!q.empty())
  {
    std::cout << q.front() << " ";
    q.pop();
  }
  std::cout << std::endl;
}


void load_numbers(std::vector<int>& numbers) {

    std::ifstream file(FILENAME, std::ios::binary); // Open the file in binary mode

    if (!file.is_open()) {
        std::cerr << "Could not open file " << FILENAME << std::endl;
        return;
    }

    unsigned char number; 

    while (file.read(reinterpret_cast<char*>(&number), sizeof(number))) {
        numbers.push_back(static_cast<int>(number)); // Convert unsigned char to int and push into vector
    }

    file.close();
}

void print_numbers(const std::vector<int>& numbers) {
    for (int number : numbers) {
        std::cout << number << " ";
                fflush(stdout);
    }
    std::cout << std::endl;
                fflush(stdout);
}

void input_processor(std::vector<int>& numbers, int size) {
    int currenq = 0; // start sending to queue 0
    for (int i = 0; i < numbers.size(); i++) {
        MPI_Send(&numbers[i], 1, MPI_INT, 1, currenq, MPI_COMM_WORLD);
        currenq = ++currenq % 2;
    }
}

void output_processor(int size, int rank, int num_numbers) {
    std::queue<int> q1;
    std::queue<int> q2;
    bool working = false;
    int recieved = 0;


    while (!working || !q1.empty() || !q2.empty() || recieved < num_numbers)
    {
        //std::cout << "OUTPUT Working" << std::endl;
        //std::cout << "OUTPUT Recieved: " << recieved << " Num numbers: " << num_numbers << std::endl;
        int number;
        MPI_Status status;

        if (recieved < num_numbers) {
            //std::cout << "OUTPUT Waiting for number from rank: " << rank - 1 << std::endl;
            MPI_Recv(&number, 1, MPI_INT, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            //std::cout << "OUTPUT Received number: " << number << " from rank: " << rank - 1 << std::endl;

            int rank = status.MPI_TAG;

            if (rank == 0) {
                //std::cout << "OUTPUT Recieved number: " << number << " Queue: " << rank << std::endl;
                //fflush(stdout);
                q1.push(number);
            }
            else {
                //std::cout << "OUTPUT Recieved number: " << number << " Queue: " << rank << std::endl;
                fflush(stdout);
                q2.push(number);
            }
            recieved++;
        }

        if (working || ((q1.size() >= pow(2, rank-1) && q2.size() >= 1) || (q2.size() >= pow(2, rank-1) && q1.size() >= 1))) {
            working = true;
        }

        if (working) {
            if (!q1.empty() && !q2.empty()) {
                int n1 = q1.front();
                int n2 = q2.front();

                if (n1 < n2) {
                    q1.pop();
                    std::cout << n1 << std::endl;
                fflush(stdout);

                } else {
                    q2.pop();
                    std::cout << n2 << std::endl;
                fflush(stdout);
                }
            } else if (!q1.empty()) {
                int n1 = q1.front();
                q1.pop();
                std::cout << n1 << std::endl;
            } else if (!q2.empty()) {
                int n2 = q2.front();
                q2.pop();
                std::cout << n2 << std::endl;
                fflush(stdout);
            }
        }
    }
}


void worker_send(int rank, std::queue<int>& queue, int& toIncrease, int& recieve_queue, int change_queue_freq) {
    int num = queue.front();
    queue.pop();
    MPI_Send(&num, 1, MPI_INT, rank + 1, recieve_queue / change_queue_freq, MPI_COMM_WORLD);
    recieve_queue = (recieve_queue + 1) % (2 * change_queue_freq);
    toIncrease++;
}


void worker(int rank, int size, int num_numbers) {
    int recieved = 0;
    std::queue<int> q1;
    std::queue<int> q2;
    bool working = false;
    int change_queue_freq = (pow(2, rank));
    int recieve_queue = 0;

    int numInFirst = 0;
    int numInSecond = 0;
    int cluster = pow(2, rank-1);


    while (!working || !q1.empty() || !q2.empty() || recieved < num_numbers){
        int number;
        MPI_Status status;


        if (recieved < num_numbers) {

            MPI_Recv(&number, 1, MPI_INT, rank - 1, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            recieved++;

            int queue = status.MPI_TAG;

            if (queue == 0) {
                //std::cout << "Rank: " << rank << " Recieved number: " << number << " Queue: " << queue << std::endl;
                //fflush(stdout);
                q1.push(number);
            }
            else {
                //std::cout << "Rank: " << rank << " Recieved number: " << number << " Queue: " << queue << std::endl;
                //fflush(stdout);
                q2.push(number);
            }
        } 


        if (working || ((q1.size() >= pow(2, rank-1) && q2.size() >= 1) || (q2.size() >= pow(2, rank-1) && q1.size() >= 1))) {
            //std::cout << "Rank: " << rank << " Working queue1: ";
            //print_queue(q1);
            //std::cout << " queue2: ";
            //print_queue(q2);
            //std::cout << std::endl;
            working = true;
        }


        if (working) {
            if (!q1.empty() && !q2.empty()) {
                int n1 = q1.front();
                int n2 = q2.front();
                

                //std::cout << "Rank: " << rank << " numInFirst: " << numInFirst << " numInSecond: " << numInSecond << std::endl;
                //fflush(stdout);

                if (numInFirst == cluster && numInSecond == cluster) {
                    numInFirst = 0;
                    numInSecond = 0;
                }

                if ((numInFirst == cluster && numInSecond < cluster) || (numInSecond == cluster && numInFirst < cluster)) {
                    if (numInFirst > numInSecond) {
                        worker_send(rank, q2, numInSecond, recieve_queue, change_queue_freq);
                    } else {
                        worker_send(rank, q1, numInFirst, recieve_queue, change_queue_freq);
                    }
                    continue;
                }

                if (n1 < n2) {
                    worker_send(rank, q1, numInFirst, recieve_queue, change_queue_freq);
                } else {
                    worker_send(rank, q2, numInSecond, recieve_queue, change_queue_freq);
                }
            } 
            else if (!q1.empty()) {
                //std::cout<<"Rank: "<<rank<<" Sending from q1 for retard reason"<<std::endl;
                //fflush(stdout);
                worker_send(rank, q1, numInFirst, recieve_queue, change_queue_freq);
            } 
            else if (!q2.empty()) {
                //std::cout<<"Rank: "<<rank<<" Sending from q2 for retard reason"<<std::endl;
                //fflush(stdout);
                worker_send(rank, q2, numInSecond, recieve_queue, change_queue_freq);
            }
        }
    }
}

void work(std::vector<int>& numbers, int rank, int size, int num_numbers) {
    if (rank == 0) {
        input_processor(numbers, size);
    }
    else if (rank == size - 1) {
        output_processor(size, rank, num_numbers);
    }
    else {
        worker(rank, size, num_numbers);
    }
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int rank, size, numbers_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::vector<int> numbers;


    if (rank == 0) {
        load_numbers(numbers);
        print_numbers(numbers);
        numbers_size = numbers.size();
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }
    else {
        MPI_Bcast(&numbers_size, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }

    work(numbers, rank, size, numbers_size);


    MPI_Finalize();
}
