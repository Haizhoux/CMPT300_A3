#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <math.h>
#include <stdbool.h>

#define MAX 1024

typedef struct
{
    char path[MAX];
    float alpha;
    float beta;
} meta;

typedef struct
{
    //char path[MAX];
    
    int buffer_s;
    int thread_id;
} thread_param;

meta meta_list[MAX];
int thread_GC0reading[20][30]; // for global cheackpointing is 0
float final_result[MAX];
int file_count[50][250];       // for global checkpointing is 1
int lock_array[30];            // lcok array for golboal checkpointing = 1

// lock for single lock
pthread_mutex_t single_lock;

// lock for lock_conf 2
pthread_mutex_t locks[256];


int meta_list_size;
int number_file;
int channel_file_line;
int buffer_size;                // argument 1
int num_threads;                // argument 2
char metadata_file_path[MAX];   // argument 3
int lock_conf;                  // argument 4
int global_checkpointing;       // argument 5
char output_file_path[MAX];     // argument 6
int lock_of_CAS;


void get_line_of_file(char* path)
{
    int line = 0;
    char buffer[1024] = {0};
    char channel_path[1024] = {0};
    FILE *file;
    FILE *channel;
    file = fopen(path, "r");

    if(file == NULL)
    {
        printf("Error to open the file: %s .\n", path);
    }

    //get the channel file path
    for(int i = 0; i < 2;i++)
    {
        fgets(buffer, sizeof(buffer), file);
        if( i == 1)
        {
            //printf("the buffer path is:%s\n", buffer);
            int len = strlen(buffer);
            buffer[len - 2] = '\0';
            strcpy(channel_path, buffer);
        }
    }
    fclose(file);

    //read channel file
    channel = fopen(buffer, "r");
    if(channel == NULL)
    {
        printf("Error to open the file: %s.\n", buffer);
    }

    while(fgets(buffer, sizeof(buffer), channel))
    {
        line++;
    }
    fclose(channel);

    channel_file_line = line;
}

void read_meta_file(char* path){
    char buffer[1024] = {0};
    FILE *file;
    file = fopen(path, "r");
    int line_number = 0;
    int list_index = 0;
    if (file == NULL)
    {
        printf("Error to open the file: %s .\n", path);
    }
    while (fgets(buffer, sizeof(buffer), file))
    {
        if(line_number == 0){
            number_file = atoi(buffer);
            //printf("The number_of_file is:%d\n",number_file);
        }
        else if((line_number - 1) % channel_file_line == 0){
            int len = strlen(buffer);
            buffer[len - 2] = '\0';
            strcpy(meta_list[list_index].path, buffer);
            //printf("The path is:%s\n",meta_list[list_index].path);
        }
        else if((line_number - 1) % channel_file_line == 1){
            meta_list[list_index].alpha = atof(buffer);
            //printf("The alpha is:%f\n",meta_list[list_index].alpha);
        }
        else if((line_number - 1) % channel_file_line == 2){
            meta_list[list_index].beta = atof(buffer);
            //printf("The beta is:%f\n",meta_list[list_index].beta);
            list_index++;
        }
        line_number++;
        meta_list_size = list_index;
    }
    fclose(file);
}

int compare_and_swap(int *value, int expected, int new_value){
    int temp = *value;
    if(temp == expected)
        *value = new_value;
    return temp;
}

void* read_input(void* inf){
    //Reading mode: Global checkpointing is 0
    //---------------------------------------------------------------------------
    if(global_checkpointing == 0){
        thread_param *info = inf;
        float input_value[MAX];
        float result_value[MAX];
        char buffer[MAX] = {0};
        int input_index = 0;
        
        int read = 0;
        int remain = 0;
        float alpha_beta[256][2];
        // get number of files to read
        read = (int)number_file / num_threads;
        remain = number_file % num_threads;
        if(info->thread_id < remain)
        {
            read+=1;
        }
        //printf("tid: %d, # read: %d\n", info->thread_id, read);

        // create a path array
        char path_array[256][MAX];
        for (int i = 0; i < read; i++) {
            int meta_index = info->thread_id + i * num_threads;
            if (meta_index < number_file) {
                strcpy(path_array[i], meta_list[meta_index].path);
                alpha_beta[i][0] = meta_list[meta_index].alpha;
                alpha_beta[i][1] = meta_list[meta_index].beta;
            }
        }

        // print path_array
        // for (int i = 0; i < read; i++) {
        //     printf("tid: %d, path_array[%d]: %s\n", info->thread_id, i, path_array[i]);
        // }


        // loop the path array
        // read file
        for (int i = 0; i < read; i++) {
            FILE *file;
            file = fopen((const char*)path_array[i], "r");
            if (file == NULL)
            {
                printf("Error to open the file: %s\n", path_array[i]);
            }
            for(int line = 0; line < channel_file_line; line++) {
                if (line == 0) {
                    fgets(buffer, sizeof(buffer), file);
                    input_value[line] = atof(buffer);
                    // how many files have been read by this thread
                    thread_GC0reading[info->thread_id][line]++;
                    
                } else {
                    // wait until finish reading all files line -1
                    //while(thread_GC0reading[info->thread_id][line - 1] != read);
                    fgets(buffer, sizeof(buffer), file);
                    input_value[line] = atof(buffer);
                    input_value[line] = alpha_beta[i][0] * input_value[line] + 
                                        (1 - alpha_beta[i][0]) * input_value[line -1];
                    thread_GC0reading[info->thread_id][line]++;
                }
                //printf("t_id: %d, alpha: %.3f, beta: %.3f\n", info->thread_id, alpha_beta[line][0], alpha_beta[line][1]);
                result_value[line] = input_value[line] * alpha_beta[i][1];

                // lock_conf 1
                if (lock_conf == 1) {
                    pthread_mutex_lock(&single_lock);
                    //put correct number to the output array(finan_result)
                    if (final_result[line] == 0) {
                        final_result[line] = result_value[line];
                    } else {
                        final_result[line] += result_value[line];
                    }
                    pthread_mutex_unlock(&single_lock);
                }

                // lock_conf 2
                if (lock_conf == 2) {
                    pthread_mutex_lock(&locks[line]);
                    //put correct number to the output array(finan_result)
                    if (final_result[line] == 0) {
                        final_result[line] = result_value[line];
                    } else {
                        final_result[line] += result_value[line];
                    }
                    pthread_mutex_unlock(&locks[line]);
                }

                // lock_conf 3
                if(lock_conf == 3){
                    while(compare_and_swap(&lock_of_CAS, 0, 1) != 0)
                    ; //Do nothing
                    //put correct number to the output array(finan_result)
                    if (final_result[line] == 0) {
                        final_result[line] = result_value[line];
                    } else {
                        final_result[line] += result_value[line];
                    }
                    lock_of_CAS = 0;
                }

                //printf("t_id: %d, result_value[%d]: %.3f\n", info->thread_id, line, result_value[line]);
            }
            fclose(file);
        }

    }


    
    
    //Reading mode: Global checkpointing is 1
    //---------------------------------------------------------------------------
    else if(global_checkpointing == 1){
        thread_param *info = inf;
        float input_value[MAX];
        float result_value[MAX];
        char buffer[MAX] = {0};
        int input_index = 0;
        
        int read = 0;
        int remain = 0;
        float alpha_beta[256][2];
        // get number of files to read
        read = (int)number_file / num_threads;
        remain = number_file % num_threads;
        if(info->thread_id < remain)
        {
            read+=1;
        }
        //printf("tid: %d, # read: %d\n", info->thread_id, read);

        // create a path array
        char path_array[256][MAX];
        for (int i = 0; i < read; i++) {
            int meta_index = info->thread_id + i * num_threads;
            if (meta_index < number_file) {
                strcpy(path_array[i], meta_list[meta_index].path);
                alpha_beta[i][0] = meta_list[meta_index].alpha;
                alpha_beta[i][1] = meta_list[meta_index].beta;
            }
        }

        // print path_array
        // for (int i = 0; i < read; i++) {
        //     printf("tid: %d, path_array[%d]: %s\n", info->thread_id, i, path_array[i]);
        // }

        // loop the path array
        // read file
        for (int i = 0; i < read; i++) {
            FILE *file;
            file = fopen((const char*)path_array[i], "r");
            if (file == NULL)
            {
                printf("Error to open the file: %s\n", path_array[i]);
            }
            for(int line = 0; line < channel_file_line; line++) {
                if (line == 0) {
                    fgets(buffer, sizeof(buffer), file);
                    input_value[line] = atof(buffer);
                    file_count[info->thread_id][line]+=1;
                    //number of file that other threads have been read the K bytes 
                    int count = 0;
                    for(int k = 0; k < num_threads; k++){
                        if( k != info->thread_id){
                            count = count + file_count[k][line];
                        }
                    }
                   
                    if(count >= number_file - read){
                        //other threads finish reading K bytes
                        //unlock the next line
                        lock_array[line+1] = 1;
                    }

                } else {
                    //check previous line (other threads have been read their respective files or not)
                    //while(lock_array[line] == 0); //Do nothing
                    // wait until finish reading all files line -1
                    // able to read
                    fgets(buffer, sizeof(buffer), file);
                    input_value[line] = atof(buffer);
                    input_value[line] = alpha_beta[i][0] * input_value[line] + 
                                        (1 - alpha_beta[i][0]) * input_value[line -1];
                    int count = 0;
                    for(int k = 0; k < num_threads; k++){
                        if( k != info->thread_id){
                            count = count + file_count[k][line];
                        }
                    }
                    if(count >= number_file - read){
                        //other threads finish reading K bytes
                        //unlock the next line
                        lock_array[line+1] = 1;
                    }
                }
                //printf("t_id: %d, alpha: %.3f, beta: %.3f\n", info->thread_id, alpha_beta[line][0], alpha_beta[line][1]);
                result_value[line] = input_value[line] * alpha_beta[i][1];

                // lock_conf 1
                if (lock_conf == 1) {
                    pthread_mutex_lock(&single_lock);
                    //put correct number to the output array(finan_result)
                    if (final_result[line] == 0) {
                        final_result[line] = result_value[line];
                    } else {
                        final_result[line] += result_value[line];
                    }
                    pthread_mutex_unlock(&single_lock);
                }

                // lock_conf 2
                if (lock_conf == 2) {
                    pthread_mutex_lock(&locks[line]);
                    //put correct number to the output array(finan_result)
                    if (final_result[line] == 0) {
                        final_result[line] = result_value[line];
                    } else {
                        final_result[line] += result_value[line];
                    }
                    pthread_mutex_unlock(&locks[line]);
                }

                // lock_conf 3
                if(lock_conf == 3){
                    while(compare_and_swap(&lock_of_CAS, 0, 1) != 0)
                    ; //Do nothing
                    //put correct number to the output array(finan_result)
                    if (final_result[line] == 0) {
                        final_result[line] = result_value[line];
                    } else {
                        final_result[line] += result_value[line];
                    }
                    lock_of_CAS = 0;
                }

                //printf("t_id: %d, result_value[%d]: %.3f\n", info->thread_id, line, result_value[line]);
            }
            fclose(file);
        }
    }
 
}

void out_put_file(char* output_path)
{
    float temp = 0;
    int result = 0;
    FILE *file;
    file = fopen(output_path, "w");

    if(file == NULL)
    {
        printf("Error to output file.\n");
        exit(1);
    }

    for(int j = 0; j < channel_file_line; j ++)
    {
        result = ceil(final_result[j]);
        fprintf(file, "%d\n",result);
    }
    fclose(file);
}


int main(int argc, char *argv[]){
    if(argc != 7){
        printf("Error, not proper arguments.\n");
        return 0;
    }
    
    //Store arguments
    buffer_size = atoi(argv[1]);                //1
    num_threads = atoi(argv[2]);                //2 
    strcpy(metadata_file_path,argv[3]);         //3
    lock_conf = atoi(argv[4]);                  //4
    global_checkpointing = atoi(argv[5]);       //5
    strcpy(output_file_path,argv[6]);           //6

    // argument validation
    if(lock_conf != 1 && lock_conf != 2 && lock_conf != 3){
        printf("Error: arugment validation of lock configuration.\n");
        return 0;
    }
    if(global_checkpointing != 1 && global_checkpointing != 0){
        printf("Error: arugment validation of global checkpointing.\n");
        return 0;
    }
    if(buffer_size <= 0){
        printf("Error: arugment validation of buffer size.\n");
        return 0;
    }


    get_line_of_file(metadata_file_path);
    read_meta_file(metadata_file_path);

    // init necessary variables
    memset(thread_GC0reading, 0, sizeof(thread_GC0reading));
    // init lock array
    lock_array[0] = 1;
    for(int i = 1; i < 30; i++){
        lock_array[i] = 0;
    }
    //init file_count 
    for(int i = 0; i < 50; i++){
        for(int j = 0; j < 250; j++)
            file_count[i][j] = 0;
    }
    

    // init single_lock
    if (lock_conf == 1) {
        if (pthread_mutex_init(&single_lock, NULL) != 0)
        {
            printf("\n mutex init failed.\n");
            return 1;
        }
    }
    // init mul locks
    if (lock_conf == 2) {
        for(int i = 0; i < channel_file_line; i++)
        {
            if (pthread_mutex_init(&locks[i], NULL) != 0)
            {
                printf("\n mutex init failed.\n");
                return 1;
            }
        }
        
    }
    // init compare_and_swap lock
    if(lock_conf == 3){
        lock_of_CAS = 0;
    }

    //Create thread 
    pthread_t tid[30];
    thread_param thread_params[30];


    for(int i = 0; i < num_threads ; i++){
        tid[i] = i;
        thread_params[i].buffer_s = buffer_size;
        thread_params[i].thread_id = i;
        pthread_create(&tid[i], NULL, read_input, (void*)&thread_params[i]);
    }


    //Join thread
    for(int i = 0; i < num_threads; i++){
        pthread_join(tid[i], NULL);
    }

    //Print output
    // for(int i = 0; i < channel_file_line; i++) {
    //     final_result[i] = ceil(final_result[i]);
    //     printf("%.3f\n", final_result[i]);
    // }

    //Create output file
    out_put_file(output_file_path);

    pthread_exit(NULL);
    return 0;
}
