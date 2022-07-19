/*
UNITED STATES MILITARY ACADEMY
Chris Booth
Kasey Candelario
Real Time Smart Grid
AY17 - XE401
Project Advisor: Dr. Suzanne J. Matthews
*/

#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include "../include/uthash.h"

#define WINDOW_SIZE 60 //number of measurements in window for temporal anomaly detection

char * fdata;
int thread_num;
long fdata_size;

// a "chunk" of PMU data. Input to MapReduce.
struct pmu_string {
    char * data;
    uint64_t len;
};

//constraint hashtable record
struct hash_record {
  char key[64]; //key
  //all the items that follow are values
  int sel;
  char type[32];
  float min;
  float max;
  UT_hash_handle hh; //makes structure hashable (required by uthash)
};

struct hash_record *hashtable = NULL; //declare constraint hashtable (loaded serially, accessed concurrently)

void show_time(const char * message, struct timeval b, struct timeval e){
  fprintf(stderr, "%s: %.2f sec\n",message, (double)(e.tv_sec - b.tv_sec)+(e.tv_usec - b.tv_usec)/1.e6);
}

void * detect(void * rank){
  
  long tid = (long) rank;
  long start = (tid*fdata_size)/( (long) thread_num);
  long stop =  (fdata_size/thread_num)*(tid+1);
  char *line, *timestamp, *sigID, *valstr, *f_state, *line_state;
  float value,min,max;
  char first_char[1];
  //if start not at newline, push forward to next line
  strncpy(first_char,fdata+start,1);
  while ( strcmp(first_char,"\n") != 0) {
    start++;
    strncpy(first_char,fdata+start,1);
  }
  if (0 == tid) start = 0;
  //if last rank, extend stop to last line
  if (thread_num == tid+1) {
    stop = fdata_size-1;
  }
  //if stop is not newline, push forward
  strncpy(first_char,fdata+stop,1);
  while ( strcmp(first_char,"\n" ) != 0) {
    stop++;
    strncpy(first_char,fdata+stop,1);
  }
  
  //loop over lines, grab data, detect anomalies
  long i = start;
  line = strtok_r(fdata+i,"\n",&f_state);
  while (i < stop-1) { //use strtok_r or strsep
    long add = (long) strlen(line);
    //printf("line:%s\n",line); //To see what line is grabbing
    sigID = strtok_r(line,",",&line_state);    
    //printf("get sigID --- tid:%d\n",tid);
    timestamp = strtok_r(NULL,",",&line_state);    
    //printf("get timestamp --- tid:%d\n",tid);
    valstr = strtok_r(NULL,",",&line_state);
    value = atof(valstr);
    //printf("sigID:%s --- timestamp:%s --- valstr:%s --- value:%f\n",sigID,timestamp,valstr,value);
    //lookup signalID, check constraints
    struct hash_record * result;
    HASH_FIND_STR(hashtable, sigID, result);
    if (result != NULL){ //silently ignore hashes that are not in constraints file
      min = result->min;
      max = result->max;
      //prints values outside acceptable constraint range
      if (value > max || value < min){
        printf("Anomaly detected:(CONSTRAINT,%s,%s,%f)\n", sigID, timestamp, value);
      }
    }
    //set i to next line
    i+=add+1;
    line = strtok_r(NULL,"\n",&f_state);
  }
  printf("thread:%d Complete\n",tid);
  return NULL;
} 

int main(int argc, char *argv[])
{
    int fd;
    struct stat finfo;
    char * fname, * cores, * cname;
    struct timeval begin, end;

    gettimeofday(&begin, NULL);
    // Make sure a filename is specified
    if (argc != 4){
      fprintf(stderr, "USAGE: %s <filename> <number of cores> <constraint file>\n", argv[0]);
      exit(1);
    }
    fname = argv[1];
    cores = argv[2];
    cname = argv[3];
    
    fprintf(stderr,"Building constraint hashtable...\n");
    size_t len = 0;
    ssize_t read;
    char * line=NULL, * sigID, * type;
    int sel;
    float min, max;
    //read in the constraint file and populate hashtable
    FILE * fp;
    fp = fopen(cname,"r"); //open constraint file for reading
    
    while ((read = getline(&line, &len, fp)) != -1) {
      //parse the elements of the line
      sigID = strtok(line, ","); //signal ID
      sel = atoi(strtok(NULL, ",")); //SEL id
      type = strtok(NULL, ",");
      min = atof(strtok(NULL,","));
      max = atof(strtok(NULL,"\n"));

      //now add it to the hashtable
      struct hash_record *rec = (struct hash_record*)malloc(sizeof(struct hash_record));
      strncpy(rec->key, sigID, strlen(sigID));
      rec->sel = sel;
      strncpy(rec->type, type, strlen(type));
      rec->min = min;
      rec->max = max;
      HASH_ADD_STR(hashtable, key, rec );
    }
    fclose(fp); //done populating hash table

    //START CONSTRAINT ANOMALY DETECTION

    fprintf(stderr,"Starting Constraint Anomaly Detection\n");
    gettimeofday(&begin, NULL);
    
    len = 0;
    line = NULL;
    char * time;
    float val;

    fprintf(stderr,"Memory Mapping Input File...\n");

    // Read in the file
    fd = open(fname, O_RDONLY);
    // Get the file info (for file length)
    fstat(fd, &finfo);

    uint64_t r = 0;
    fdata_size = finfo.st_size;
    //now read the file into memory(pread)
    fdata = (char *)malloc (fdata_size);
    while(r < (uint64_t)fdata_size)
        r += pread (fd, fdata + r, fdata_size, r);
    printf("pread complete\n");
    
    //Initialize threads
    long t;
    pthread_t * threads;
    thread_num = atoi(&cores[0]);
    printf("number of threads (thread_num):%d\n",thread_num);
    threads = malloc(thread_num*sizeof(pthread_t)); 
    for(t=0;t < thread_num;t++){
      pthread_create(&threads[t], NULL, detect, (void *) t);
    }
    
    //Close threads
    for(t=0;t < thread_num;t++){
      pthread_join(threads[t],NULL);
    }
    printf("threads complete -- closed\n");
    close(fd);
    if (1){//(result.size())
      free (fdata);
    }    
    free(threads);
    gettimeofday(&end, NULL);
    show_time("Constraint Time", begin, end);
    fprintf(stderr,"Constraint: Detection Completed\n");
    /************
    *
    *
    BEGIN PHASE 2 - TEMPORAL ANOMALIES
    *
    *
    ************/
    gettimeofday(&begin, NULL);
    //now read in again and start phase 2
    gettimeofday(&end, NULL);
    //show_time("Temoral: Detection Completed", begin, end);

    return 0;
}
