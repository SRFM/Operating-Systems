/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/


/*  Thanasis Dimosiaris
    Serafim Antoniou

    !Please note that the current program is an extention to the one we wrote last year for the same lab
*/

#define _POSIX_C_SOURCE 1

#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <sys/time.h>
#include <math.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>



#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define MAX_QUEUE_SIZE            10  
#define CONSUMER_THREADS          10                                                                   // INPUT


// INITIALIZATION OF THE STATISTICS VARIABLES
long total_waiting_time = 0;
long total_service_time = 0;
int completed_requests = 0; 

// THE COUNTER AND THE TERMINATION FLAG
int terminate = 0;
int get_counter = 0;

// INITIALIZATION OF THE PTHREAD CONDITIONS
pthread_cond_t non_full_queue = PTHREAD_COND_INITIALIZER;    // SIGNAL FOR IF THE QUEUE IS NOT FULL
pthread_cond_t non_empty_queue = PTHREAD_COND_INITIALIZER;  // SIGNAL FOR IF THERE IS SOMETHING IN THE QUEUE

// INITIALIZATION OF THE MUTEXES
pthread_mutex_t queue_producer_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queue_consumer_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queue_size_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t db_put_mutex = PTHREAD_MUTEX_INITIALIZER; 
pthread_mutex_t get_counter_mutex = PTHREAD_MUTEX_INITIALIZER;      

// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;
// Definition of the database.
KISSDB *db = NULL;

pthread_t consumer_threads[CONSUMER_THREADS];

// QUEUE
// THE STRUCT NEEDED FOR THE QUEUE                                                                      // INPUT

struct queue_element
{
  int socketfd;
  struct timeval timev;
};

int QUEUE_HEAD = 0; // WITH WHICH YOU EXTRACT ELEMENTS FROM THE QUEUE
int QUEUE_TAIL = 0; // WITH WHICH YOU ADD ELEMENTS FROM THE QUEUE
int QUEUE_SIZE = 0;

// THE QUEUE:                                                                                           // INPUT
struct queue_element queue[MAX_QUEUE_SIZE];

// THE PROCESS THAT IS CALLED WHEN AN ELEMENT IS ADDED TO THE QUEUE

void pushed_element(){
  pthread_mutex_lock(&queue_size_mutex);
  QUEUE_TAIL++;
  QUEUE_TAIL = (QUEUE_TAIL % (int)MAX_QUEUE_SIZE);
  QUEUE_SIZE++;
  pthread_mutex_unlock(&queue_size_mutex);
}

// THE PROCESS THAT IS CALLED WHEN AN ELEMENT IS EXTRACTED FROM THE QUEUE

void popped_element(){
  pthread_mutex_lock(&queue_size_mutex);
  QUEUE_HEAD++;
  QUEUE_HEAD = (QUEUE_HEAD % (int)MAX_QUEUE_SIZE);
  QUEUE_SIZE--;
  pthread_mutex_unlock(&queue_size_mutex);
}

void termination(){
  terminate = 1;
  int i;
  for(i = 0; i < CONSUMER_THREADS; i++){
    pthread_cond_signal(&non_empty_queue);
  }

  // PRINT THE STATS  
  printf("\n---------STATS-----------\n\n");
  printf("Completed Requests: %d \n", completed_requests);
  printf("Average waiting time: %ld\n", total_waiting_time/(long)completed_requests);
  printf("Average service time: %ld\n----------------------------------\n", total_service_time/(long)completed_requests);


  for(i = 0; i < CONSUMER_THREADS; i++){
    pthread_join(consumer_threads[i],NULL);
  }
  KISSDB_close(db);
  printf("done\n");
  exit(1);
  return;
}
/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

/*
 * @name process_request - Process a client request.
 *
 * @return
 */
void process_request() {
  
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
  int numbytes = 0;
  Request *request = NULL;
  int new_fd;

  // NEED VARIABLES TO STORE THE IN QUEUE TIME AND THE SERVICE TIME OF EACH REQUEST WHICH I'LL ADD TO THE OVERALL VARIABLE     // INPUT
  struct timeval got_it_from_queue_time_temp, service_time_temp, entry_time;
  sigset_t set;
  sigemptyset(&set);

  sigaddset(&set, SIGSTOP);
// Block signal SIGUSR1 in this thread
  pthread_sigmask(SIG_BLOCK, &set, NULL); 

  while(1){
    // GET THE REQUEST FROM THE QUEUE
    
    pthread_mutex_lock(&queue_consumer_mutex);
    
    printf("The consumer thread starts\n");
    while(QUEUE_SIZE == 0){                  // THE QUEUE IS EMPTY
      pthread_cond_wait(&non_empty_queue, &queue_consumer_mutex);
      if(terminate){
        printf("Got a message to terminate\n");
        return;
      }
    }
   
    new_fd = queue[QUEUE_HEAD].socketfd;  // THE SOCKETFD WE'RE GOING TO USE
    entry_time = queue[QUEUE_HEAD].timev;     // THE TIME THE ELEMENT ENTERED THE QUEUE

    popped_element(); 
    gettimeofday(&got_it_from_queue_time_temp, NULL);

    pthread_cond_signal(&non_full_queue);
    pthread_mutex_unlock(&queue_consumer_mutex);
    
    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(new_fd, request_str, BUF_SIZE);
    printf("%s\n", request_str);

    
    // parse the request.
      if (numbytes) {
        request = parse_request(request_str);
        if (request) {
          switch (request->operation) {
            case GET:
              pthread_mutex_lock(&get_counter_mutex);
              get_counter++;
              pthread_mutex_unlock(&get_counter_mutex);
              // Read the given key from the database.
              if (KISSDB_get(db, request->key, request->value))
                sprintf(response_str, "GET ERROR\n");
              else{
                sprintf(response_str, "GET OK: %s\n", request->value);
                printf("+++GET OK: %s\n", request->value);
              }
              pthread_mutex_lock(&get_counter_mutex);
              get_counter--;
              pthread_mutex_unlock(&get_counter_mutex);
              break;
            case PUT:
              pthread_mutex_lock(&db_put_mutex);
              while(get_counter>0)
                // DO NOTHING, WAIT FOR THE GETTERS TO TERMINATE
              pthread_mutex_lock(&get_counter_mutex);
              // Write the given key/value pair to the database.
              if (KISSDB_put(db, request->key, request->value)){ 
                sprintf(response_str, "PUT ERROR\n");
              }else{
                sprintf(response_str, "PUT OK\n");
                printf("+++PUT OK\n");
              }
              pthread_mutex_unlock(&get_counter_mutex);
              pthread_mutex_unlock(&db_put_mutex);
              break;
            default:
              // Unsupported operation.
              sprintf(response_str, "UNKNOWN OPERATION\n");     // SAME ERROR WITH LAST YEAR
          } 
        }else{
          sprintf(response_str, "FORMAT ERROR\n");
        }
      }else{
        // Send an Error reply to the client.
        sprintf(response_str, "FORMAT ERROR\n");
        printf("ERROR IN readfromsocket()\n");
      }
    //printf("%s\n", request_str);
  
  
  write_str_to_socket(new_fd, response_str, strlen(response_str));
  printf("\n\n");

  // GET THE STATISTICS
  pthread_mutex_lock(&stats_mutex);
  gettimeofday(&service_time_temp, NULL);
  completed_requests++;          // INCREASE THE COMPLETED REQUESTS
  total_service_time += (service_time_temp.tv_usec - entry_time.tv_usec) + 
    (service_time_temp.tv_sec - entry_time.tv_sec) * 1000000;
  total_waiting_time += (got_it_from_queue_time_temp.tv_usec - entry_time.tv_usec) + 
    (got_it_from_queue_time_temp.tv_sec - entry_time.tv_sec) * 1000000;
  pthread_mutex_unlock(&stats_mutex);

  if (request){
    free(request);
  }    
  request = NULL;

  close(new_fd);
  printf("Waiting time: %ld\n Service time: %ld\n",total_waiting_time, total_service_time);
  }    
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {

  int socket_fd, // listen on this socket for new connections
      new_fd;    // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr, // my address information
      client_addr;                // connector's address information
 
 // NEED TO CREATE THREADS FOR THE CONSUMER                                                                           // INPUT
  int i;
  for (i = 0; i < CONSUMER_THREADS; i++)
  {
    if(pthread_create(consumer_threads + i, NULL, (void *) &process_request, NULL) != 0)
      printf("We have a problem jack\n");                                       // INPUT
  }


  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

 if (signal(SIGTSTP, termination) == SIG_ERR)
  {
    exit(1); // 1 because there was a problem with the signal handling
  }

  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection

    // LOCK THE SIZE AND THE QUEUE TO MANIPULATE THEM
    pthread_mutex_lock(&queue_producer_mutex);
    while(QUEUE_SIZE == MAX_QUEUE_SIZE){
      pthread_cond_wait(&non_full_queue,&queue_producer_mutex);
      printf("The queue is full\n");

    }

    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));

    // ADD THE ELEMENT IN THE QUEUE
    queue[QUEUE_TAIL].socketfd = new_fd;          // THE FILE DESCRIPTOR
    gettimeofday(&queue[QUEUE_TAIL].timev, NULL); // THE TIME THE ELEMENT WAS ADDED IN THE QUEUE
    pushed_element();
    printf("HEAD: %d\nTAIL: %d\nSIZE: %d\n",QUEUE_HEAD,QUEUE_TAIL,QUEUE_SIZE);

    // TODO ADD A SIGNAL TO LET THE SYSTEM KNOW THERE IS SOMETHING IN THE QUEUE
    pthread_cond_signal(&non_empty_queue);
    // LET THEM GO (even though we dont love them)
    pthread_mutex_unlock(&queue_producer_mutex);   
  }  

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

