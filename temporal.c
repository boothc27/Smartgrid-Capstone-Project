/*
UNITED STATES MILITARY ACADEMY
Chris Booth
Kasey Candelario
Real Time Smart Grid
AY17 - XE402
Project Advisor: Dr. Suzanne J. Matthews
*/

#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <math.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include "../include/uthash.h"

long window_size = 60; //number of measurements in temporal window
char * fdata; //input file data
long fdata_size; //input file size
int thread_num; //number of threads
long num_recs; //number of records in hashtable_temp
char ** rec_id; //hashtable_temp record ids
long rec_i = 0; //rec_id current index

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

//temporal hashtable record
struct hash_temp {
  char key[64];
  long size;
  long alloc_size;
  char **values;
  UT_hash_handle hh;
};

struct hash_record *hashtable = NULL; //declare constraint hashtable (loaded serially, accessed concurrently)
struct hash_temp *hashtable_temp = NULL;
pthread_mutex_t lock;

void show_time(const char * message, struct timeval b, struct timeval e){
  fprintf(stderr, "%s: %.2f sec\n",message, (double)(e.tv_sec - b.tv_sec)+(e.tv_usec - b.tv_usec)/1.e6);
}

void * temporal_init(void * rank){
  // Each thread runs over a chunk of fdata, same as constraint
  long tid = (long) rank;
  long start = (tid*fdata_size)/(thread_num);
  long stop =  (fdata_size/thread_num)*(tid+1);
  char *line, *time_val, *sigID, *f_state, *line_state;
  char *begin, *end;
  begin = fdata;
  if (start != 0) {
    while (begin[start] != '\n') {
      start++;
    }
    start++;
  }
  if (thread_num == tid+1) {
    stop = fdata_size-1;
  }
  end = fdata;
  while (end[stop] != '\n') {
    stop++;
  }
  //loop over lines, grab data, create hash records
  char *tdata = fdata+start;
  long i = start;
  line = strsep(&tdata,"\n");
  struct hash_temp *hashtable_local = NULL;
  while (i < stop && line != NULL) {
    long add = (long) strlen(line);
    sigID = strsep(&line,",");
    time_val = line;
    //valstr = strsep(&line,",");
    struct hash_temp * result;
    //fprintf(stderr,"sigID: %s - valstr: %s\n",sigID,valstr);
    //look for hash record matching sigID
    HASH_FIND_STR(hashtable_local, sigID, result);
    if (result == NULL) { //if record not found, create new record
      struct hash_temp *rec = (struct hash_temp*)malloc(sizeof(struct hash_temp));
      strncpy(rec->key, sigID, strlen(sigID));
      rec->values = (char **)malloc( (size_t) 1000*sizeof(char *));
      rec->alloc_size = 1000*sizeof(char *);
      rec->values[0] = time_val;//valstr;
      rec->size = 1;
      HASH_ADD_STR(hashtable_local, key, rec);
    }
    else { //else add time_val to record
      if (result->size*sizeof(char *) >= result->alloc_size) {
        result->values = (char **)realloc(result->values, (size_t) result->alloc_size*2);
        result->alloc_size += result->alloc_size;
      }
      result->values[result->size] = time_val;
      result->size += 1;
    }
    i+=add+1;
    line = strsep(&tdata,"\n");
  }
  //look up record from hashtable_temp -- do essentially same thing as above to update
  //if record not found, create record
  struct hash_temp * local;
  for(local = hashtable_local; local != NULL; local = local->hh.next) {
    struct hash_temp * global;
    pthread_mutex_lock(&lock);
    HASH_FIND_STR(hashtable_temp, local->key, global);
    //create record if does not exist
    if (global == NULL) {
      struct hash_temp *rec = (struct hash_temp*)malloc(sizeof(struct hash_temp));
      strcpy(rec->key, local->key);//, strlen(local->key));
      rec->values = (char **)malloc( (size_t) 1000*sizeof(char *));
      rec->alloc_size = 1000*sizeof(char *);
      rec->size = 0;
      num_recs += 1;
      HASH_ADD_STR(hashtable_temp, key, rec);
    }
    //update records' values
    HASH_FIND_STR(hashtable_temp, local->key, global);
    for(i = 0; i < local->size; i++) {
      if (global->size*sizeof(char *) >= global->alloc_size) {
        global->values = (char **)realloc(global->values, (size_t) global->alloc_size*2);
        global->alloc_size += global->alloc_size;
      }
      global->values[global->size] = local->values[i];
      global->size += 1;
    }
    pthread_mutex_unlock(&lock);
  }
  //fprintf(stderr,"Thread:%d Init Complete\n",tid);
  return NULL;
}

void * temporal_detect(void * rank){
  long tid = (long) rank;
  
  //calculate Fano Factor and print list of temporal anomalies
  struct hash_temp *rec;
  //iterates over hashtable_temp records
  long i = tid*(rec_i/thread_num);
  long j, k, vals_size, denom;
  long stop = (tid+1)*(rec_i/thread_num);
  char *id, *time_val, *time, *valstr;
  float *vals;
  float avg, v, stdev, fano;
  int isanomaly, det = 0;
  if (tid == thread_num-1) stop = rec_i;
  struct hash_temp *result;
  struct hash_record *res;
  vals = (float *)malloc((size_t)1);
  //loop over hash records
  while (i < stop) {
    HASH_FIND_STR(hashtable_temp, rec_id[i], result);
    if (vals_size < result->alloc_size) vals = (float *)realloc(vals, (size_t)result->alloc_size);
    j = 0;
    if (i < rec_i) HASH_FIND_STR(hashtable, rec_id[i], res);
    //skip if ID not found in constraint table
    if (res!=NULL) {
      while (j < result->size && det == 0) {
        avg = 0;
        v = 0;
        isanomaly = 0;
        k = 0;
        while (k < window_size && k+j < result->size) {
          time_val = result->values[k+j];
          char *t = strsep(&time_val,",");
          if (k == 0) time = t;
          valstr = strsep(&time_val,",");
          if (strcmp(res->type, "voltage") == 0) vals[k+j] = atof(valstr)/27000;
          else if (strcmp(res->type, "current") == 0) vals[k+j] = atof(valstr)/1000;
          else vals[k+j] = atof(valstr);
          avg += vals[k+j];
          k++;
        }
        avg/=k;
        k = 0;
        stdev = 0;
        while (k < window_size && k+j < result->size) {
          v = vals[j+k]-avg;
          stdev += v*v;
          k++;
        }
        stdev = sqrt(stdev);
        fano = stdev/avg;
        //temporal anomaly if fano (voltage) > 0.0001 || (current) > 0.15 || (frequency) > 0.001
        
        if (det == 0) {
          if (strcmp(res->type,"voltage") == 0) {
            if (fano > 0.0001) isanomaly = 1;
          }
          else if (strcmp(res->type,"current") == 0) {
            if (fano > 0.15) isanomaly = 1;
          }
          else if (strcmp(res->type,"frequency") == 0) {
            if (fano > 0.001) isanomaly = 1;
          }
          else if (fano > 1) {
            isanomaly = 1;
          }
          if (isanomaly==1) {
          det=1;
          printf("%s,%s,%s,fano:%f,avg:%f,k:%d,size:%d\n",res->key,res->type,time,fano,avg,k,result->size);
          }
        }
        j += window_size; //discrete 1 second window
        //else j++; //sliding 1 second window
        //debugging print statements - prints for each record
        //printf("mean: %f\n",mean);
        //printf("var: %f\n",var);
        //printf("id: %s fano: %f j: %d size: %d\n", res->key, fano, j, result->size);
      }
    }
    det = 0;
    i++;
  }
  return NULL;
}

int main(int argc, char *argv[])
{
    int fd;
    struct stat finfo;
    char * fname, * cores, * cname;
    struct timeval begin_total, end_total, begin_cons, end_cons, begin_hash, end_hash, begin_input, end_input, begin_det, end_det;

    gettimeofday(&begin_total, NULL);
    // Make sure a filename is specified
    if (argc != 4){
      fprintf(stderr, "USAGE: %s <filename> <number of cores> <constraint file> [temporal window]\n", argv[0]);
      exit(1);
    }
    fname = argv[1];
    cores = argv[2];
    cname = argv[3];
    
    fprintf(stderr,"Building Constraint Hashtable...\n");
    gettimeofday(&begin_cons, NULL);
    
    size_t len = 0;
    ssize_t read;
    char * line=NULL, * sigID, * type;
    int sel;
    float min, max;
    // Read in the constraint file and populate hashtable
    FILE * fp;
    fp = fopen(cname,"r"); // Open constraint file for reading
    
    while ((read = getline(&line, &len, fp)) != -1) {
      // Parse the elements of the line
      sigID = strtok(line, ","); //signal ID
      sel = atoi(strtok(NULL, ",")); //SEL id
      type = strtok(NULL, ",");
      min = atof(strtok(NULL,","));
      max = atof(strtok(NULL,"\n"));

      // Now add it to the hashtable
      struct hash_record *rec = (struct hash_record*)malloc(sizeof(struct hash_record));
      strncpy(rec->key, sigID, strlen(sigID));
      rec->sel = sel;
      strncpy(rec->type, type, strlen(type));
      rec->min = min;
      rec->max = max;
      HASH_ADD_STR(hashtable, key, rec );
    }
    fclose(fp); // Done populating hash table
    
    gettimeofday(&end_cons, NULL);

    fprintf(stderr,"Starting Constraint Anomaly Detection\n");
    
    len = 0;
    line = NULL;
    char * time;
    float val;

    fprintf(stderr,"Memory Mapping Input File...\n");
    gettimeofday(&begin_input, NULL);

    // Read in the file
    fd = open(fname, O_RDONLY);
    // Get the file info (for file length)
    fstat(fd, &finfo);

    uint64_t r = 0;
    fdata_size = finfo.st_size;
    // Now read the file into memory(pread)
    fprintf(stderr,"reading file...\n");
    fdata = (char *)malloc (fdata_size);
    while(r < (uint64_t)fdata_size)
        r += pread (fd, fdata + r, fdata_size, r);
    gettimeofday(&end_input, NULL);
    
    
    fprintf(stderr,"Starting Temporal Anomaly Detection\n");
    
    // Parallelize sorting signalIDs using a hashtable
    gettimeofday(&begin_hash, NULL);
    
    // Initialize mutex
    if ( pthread_mutex_init(&lock,NULL) != 0) {
      fprintf(stderr,"init mutex failed");
      return 1;
    }
    
    // Initialize threads to populate hashtable
    long t;
    fprintf(stderr,"initializing sorting threads...\n");
    pthread_t * temp_threads;
    thread_num = atoi(&cores[0]);
    temp_threads = malloc(thread_num*sizeof(pthread_t));
    for(t=0;t < thread_num;t++){
      pthread_create(&temp_threads[t], NULL, temporal_init, (void *) t);
    }
    // Close threads
    for(t=0;t < thread_num;t++){
      pthread_join(temp_threads[t],NULL);
    }
    
    rec_id = (char **)malloc( (size_t) num_recs*64);
    struct hash_temp *rec;
    for (rec=hashtable_temp; rec != NULL; rec=rec->hh.next) {
      char * name = rec->key;
      rec_id[rec_i] = name;
      if (strcmp(rec->key,"DC273711-CBCA-4DF2-A354-655748A78AE5") == 0) {
        int i;
        //for (i = 0; i < rec->size; i++) printf("%s\n",rec->values[i]);
      }
      rec_i += 1;
    }
    //printf("rec_i: %d\n",rec_i);
    gettimeofday(&end_hash, NULL);
    gettimeofday(&begin_det, NULL);
    //Initialize threads to calculate Fano Factor
    fprintf(stderr,"initializing temporal detection threads...\n");
    pthread_t * temp_threads2;
    thread_num = atoi(&cores[0]);
    temp_threads2 = malloc(thread_num*sizeof(pthread_t));
    for(t=0;t < thread_num;t++){
      pthread_create(&temp_threads2[t], NULL, temporal_detect, (void *) t);
    }
    // Close threads
    for(t=0;t < thread_num;t++){
      pthread_join(temp_threads2[t],NULL);
    }
    
    close(fd);
    free(fdata);free(temp_threads);free(hashtable);free(hashtable_temp);
    gettimeofday(&end_det, NULL);
    fprintf(stderr,"Temporal: Detection Completed\n");
    
    show_time("Constraint Hashtable Time", begin_cons, end_cons);
    show_time("Memory Map Input File Time", begin_input, end_input);
    show_time("Temporal Hashtable Time", begin_hash, end_hash);
    show_time("Temporal Detection Time", begin_det, end_det);
    fprintf(stderr, "Temporal Anomaly Detection Completed\n");
    gettimeofday(&end_total, NULL);
    show_time("Total Time", begin_total, end_total);
    return 0;
}
