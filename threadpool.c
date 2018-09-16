/**
 * threadpool.c
 *
 * This file will contain your implementation of a threadpool.
 */

#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <stdlib.h>
#include <pthread.h>
#if defined(__linux__)
#include <sys/prctl.h>
#endif

#include "threadpool.h"



static volatile int keepalive;
static volatile int threadsOnHold;

//Stucts

//Semaphore
typedef struct binSem {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} binSem;

//Job
typedef struct job{
    struct job* previous;  //pointer to previous job
    void  (*function)(void* arg);  //pointer to function
    void* arg;  //pointer to args for function
};


//job queue
typedef struct jQueue{
    pthread_mutex_t rwmutex;  //mutex for access to queue
    job *front;  //pointer to front of queue
    job *end;   //pointer to end of queue
    binSem *hasJobs;  //flag
    int len;   //num jobs
} jQueue;

//thread
typedef struct thread{
    int id;     //id
    pthread_t pthread;    //pointer to thread
    struct _threadpool* thpool_p;    //access to pool
}thread;


// _threadpool is the internal threadpool structure that is
// cast to type "threadpool" before it given out to callers
typedef struct _threadpool_st {
    thread** threads;        //pointer to threads
    volatile int numAlive;    //threads currently alive
    volatile int numWorking;   //threads currently working
    pthread_mutex_t threadLock;  //thread count
    pthread_cond_t threadsIdle;  //signal to thpool wait Might not need
    jQueue jobqueue;  //job queue

} _threadpool;



threadpool create_threadpool(int num_threads_in_pool) {

  // sanity check the argument
  if ((num_threads_in_pool <= 0) || (num_threads_in_pool > MAXT_IN_POOL))
    return NULL;

  pool = (_threadpool *) malloc(sizeof(_threadpool));
  if (pool == NULL) {
    fprintf(stderr, "Out of memory creating a new threadpool!\n");
    return NULL;
  }


  threadsOnHold = 0;
  keepalive = 1;

  _threadpool* thpool_p;
  thpool_p = (struct _threadpool*)malloc(sizeof(struct _threadpool));
  if(thpool_p == NULL){
      printf("ERROR: create_threadpool(): could not allocate memory for threads\n");
      return NULL;
  }

  thpool_p->numAlive = 0;
  thpool_p->numWorking = 0;


  //initialize jobqueue
  if(jQueue_init(&thpool_p->jobqueue) == -1){
    printf("ERROR: create_threadpool() could not allocate memory for job queue\n");
    free(thpool_p);
    return NULL;
  }

  //Make threads in pool
  thpool_p->threads = (struct thread**)malloc(num_threads_in_pool * sizeof(struct thread *));
  if(thpool_p->threads == NULL){
    printf("ERROR: create_threadpool() could not allocate memory for threads\n");
    jQueue_destroy(&thpool_p->jobqueue);
    free(thpool_p);
    return NULL;
  }

  pthread_mutex_init(&(thpool_p->threadLock), NULL);
  pthread_cond_init(&thpool_p->threadsIdle, NULL);


  //initialize threads
  int n;
  for(n=0; n<num_threads_in_pool; n++){
    thread_init(thpool_p, &thpool_p->threads[n], n);
  }

  //wait for all threads to initialize
  while(thpool_p->numAlive != num_threads_in_pool){}

  return (threadpool) thpool_p;
}


void dispatch(threadpool from_me, dispatch_fn dispatch_to_here,
          void *arg) {
  _threadpool *pool = (_threadpool *) from_me;

  //create new job
  job* newjob;

  newjob = (struct job*)malloc(sizeof(struct job));
  if (newjob == NULL){
    perror("dispatch() could not allocate memory for new job\n");
    return -1;
  }

  //add function and argument to new job
  newjob->function = dispatch_to_here;
  newjob->arg = arg;

  //add to queue
  jQueue_push(&pool->jobqueue, newjob);


}

void destroy_threadpool(threadpool destroyme) {
  _threadpool *pool = (_threadpool *) destroyme;

  if(pool == NULL) return;

  volatile int totalThreads = pool->numAlive;

  //end each threads infinite loop
  keepalive = 0;

  //give time to kill idle threads
  double timeout = 1.0;
  time_t start, end;
  double elapsed = 0.0;
  time(&start);
  while(elapsed < timeout && pool->numAlive){
    binSem_post_all(pool->jobqueue.hasJobs);
    time(&end);
    elapsed = difftime(end,start);
  }

  //poll remaining threads
  while(pool->numAlive){
    binSem_post_all(&pool->jobqueue);
    sleep(1);
  }


  //cleanup job queue
  jQueue_destroy(&pool->jobqueue);

  //free memory
  int n;
  for(n=0; n < totalThreads; n++){
    thread_destroy(pool->threads[n]);
  }

  free(pool->threads);
  free(pool);

}


/*
 * STUFF FOR THREADS
 */

static int thread_init(_threadpool* thp, struct thread** thread_p, int id){

  *thread_p = (struct thread*)malloc(sizeof(struct thread));
  if(thread_p == NULL){
    printf("ERROR: thread_init() could not allocate memory for thread\n");
    return -1;
  }


  (*thread_p)->thpool_p = thpool_p;
  (*thread_p)->id = id;

  pthread_create(&(*thread_p)->pthread, NULL, (void *)thread_do, (*thread_p));
  pthread_detach((*thread_p)->pthread);

  return 0;
}

//Set calling Thread on hold
static void thread_hold(int sig_id){
  (void)sig_id;
      threadsOnHold = 1;
      while(threadsOnHold){
        sleep(1);
      }
}


static void* thread_do(struct thread* thread_p){

  //set thread name for debugging
  char thread_name[128] = {0};
  sprintf(thread_name, "thread-pool-%d", thread_p->id);
  prctl(PR_SET_NAME, thread_name);

  //make sure all threads were created before starting
  _threadpool* thpool = thread_p->thpool_p;

  //Register Signal handler
  struct sigaction act;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  act.sa_handler = thread_hold;
  if(sigaction(SIGUSR1, &act, NULL) == -1){
    printf("ERROR: thread_do() cannot handle SIGUSR1\n");
  }

  //mark thread as alive
  pthread_mutex_lock(&thpool->threadLock);
  thpool->numAlive += 1;
  pthread_mutex_unlock(&thpool->threadLock);


  while(keepalive){

    binSem_wait(thpool->jobqueue.hasJobs);

    if(keepalive){

      pthread_mutex_lock(&thpool->threadLock);
      thpool->numWorking++;
      pthread_mutex_unlock(&thpool->threadLock);

      //read job from queue and execute
      void (*func_buff)(void*);
      void* arg_buff;
      job* job_p = jQueue_pull(&thpool->jobqueue);

      if(job_p){

        func_buff = job_p->function;
        arg_buff = job_p->arg;
        func_buff(arg_buff);
        free(job_p);

      }

      pthread_mutex_lock(&thpool->threadLock);
      thpool->numWorking--;

      if(!thpool->numWorking){
        pthread_cond_signal(&thpool->threadsIdle);
      }
      pthread_mutex_unlock(&thpool->threadLock);

    }
  }

  //when done thread dies.
  //need it to go back to waiting / blocked

  pthread_mutex_lock(&thpool->threadLock);
  thpool->numAlive--;
  pthread_mutex_unlock(&thpool->threadLock);


  return NULL;
}

//Frees a thread
static void thread_destroy (thread* thread_p){
  free(thread_p);
}



/*
 * STUFF FOR THE JOB QUEUE
 */

static int jQueue_init(jQueue* jobqueue){
  jobqueue->len = 0;
  jobqueue->front = NULL;
  jobqueue->end = NULL;

  jobqueue->hasJobs = (struct binSem*)malloc(sizeof(struct binSem));
  if(jobqueue->hasJobs == NULL){
    return -1;
  }

  pthread_mutex_init(&(jobqueue->rwmutex), NULL);
  binSem_init(jobqueue->hasJobs, 0);

  return 0;

}

//Add Job to queue
static void jQueue_push(jQueue* jobqueue, struct job* newjob){

  //lock the queue from being accessed by other threads
  pthread_mutex_lock(&jobqueue->rwmutex);
  newjob->previous = NULL;

  //critical section
  switch(jobqueue->len){

    case 0: //no jobs in queue
          jobqueue->front = newjob;
          jobqueue->end = newjob;
          break;

    default: //if jobs exist in queue
          jobqueue->end->previous = newjob;
          jobqueue->end = newjob;
  }
  jobqueue->len++;

  //unlock the queue
  binSem_post(jobqueue->hasJobs);
  pthread_mutex_unlock(&jobqueue->rwmutex);

}

//Get first job from queue
static struct job* jQueue_pull(jQueue* jobq){

  //lock queue from other access
  pthread_mutex_lock(&jobq->rwmutex);

  //critical section
  job* job_p = jobq->front;

  switch(jobq->len){
    case 0: //no jobs in queue;
          break;

    case 1: //only one job
          jobq->front = NULL;
          jobq->end = NULL;
          jobq->len = 0;
          break;

    default: //more then 1 job
          jobq->front = job_p->prev;
          jobq->len--;

          //more than one in queue, post it
          binSem_post(jobq->hasJobs);
  }

  //unlock queue
  pthread_mutex_unlock(&jobq->rwmutex);
  return job_p;

}

//Free all queue resources
static void jQueue_destroy(jQueue* jq){
  while(jq->len){
    free(jQueue_pull(jq));
  }

  jq->front = NULL;
  jq->end = NULL;
  binSem_reset(jq->hasJobs);
  jq->len = 0;

  free(jq->hasJobs);

}


/*
 * STUFF FOR SYNCHRON
 */

static void binSem_init(binSem *bsem, int value){
  if(value < 0 || value > 1){
    perror("binSem_init(): Can only take values of 0 or 1\n");
    exit(1);
  }
  pthread_mutex_init(&(binSem->mutex), NULL);
  pthread_cond_init(&(binSem->cond), NULL);
  binSem->v = value;
}

//reset semaphore to 0
static void binSem_reset(binSem *bs){
  binSem_init(bs, 0);
}

//Post to at least one thread
static void binSem_post(binSem *bs){
  pthread_mutex_lock(&bs->mutex);
  bs->v = 1;
  pthread_cond_signal(&bs->cond);
  pthread_mutex_unlock(bs->mutex);
}

//Post to all threads
static void binSem_post_all(binSem *bs){
  pthread_mutex_lock(&bs->mutex);
  bs->v = 1;
  pthread_cond_broadcast(&bs->cond);
  pthread_mutex_unlock(bs->mutex);
}


//wait on semaphore until it has a value of 0
static void binSem_wait(binSem* bs){

  pthread_mutex_lock(&bs->mutex);

  while(bs->v != 1){
    pthread_cond_wait(&bs->cond, &bs->mutex);
  }
  bs->v = 0;
  pthread_mutex_unlock(&bs->mutex);
}











