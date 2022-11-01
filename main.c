#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "arraylist.h"

typedef struct file_args
{
    int file_id;
    char *file_name;
    long int file_size;
    long int list_size;
} TFileArgument, *PFileArgument;

typedef struct key_value_pair {
    int key;
    vector values;
} TKeyValuePair, *PKeyValuePair;

typedef struct partial_solution_vec {
    PKeyValuePair *key_value;
    long int vec_size;
} TPartialSolutionVector, *PPartialSolutionVector;

typedef struct threads_args
{
    int id;
    pthread_barrier_t *barrier;
    pthread_mutex_t *mutex;
    PFileArgument *files_list;
    long int* files_list_size;
} ThreadArguments, *PThreadArguments;

PFileArgument remove_from_array_at_pos(int pos, PFileArgument *files_list, long int *files_list_size)
{
    PFileArgument save_file = files_list[pos];

    // printf("size in remove_from_arry : %ld\n", *files_list_size);

    if (pos < 0 || pos > *files_list_size)
    {
        printf("Invalid position! Please enter position between 1 to %ld\n", *files_list_size);
        return NULL;
    }
    else
    {
        
        /* Copy next element value to current element */
        for (int i = pos - 1; i < *files_list_size - 1; i++)
        {
            files_list[i] = files_list[i + 1];
        }

        /* Decrement array files_list->list_size by 1 */
        *files_list_size = *files_list_size - 1;

        /* Print array after deletion */
        // printf("\nElements of array after delete are : ");
        // for (int i = 0; i < *files_list_size; i++)
        // {
        //     printf("%s\t", files_list[i]->file_name);
        // }
        // printf("\n");
    }

    return save_file;
}

PFileArgument chooseFile(PFileArgument *files_list, long int *files_list_size, pthread_mutex_t *mutex)
{
    PFileArgument save_file = NULL;

    //pthread_mutex_lock(mutex);
    save_file = remove_from_array_at_pos(0, files_list, files_list_size);
    //pthread_mutex_unlock(mutex);
    return save_file;
}

void *f_M(void *arg)
{
    PThreadArguments args = (PThreadArguments)arg;
    
    pthread_mutex_lock(args->mutex);
    printf("Hello from thread %d\n", args->id);
    printf("beginning thread %d size : %ld\n\n", args->id , *args->files_list_size);
    pthread_mutex_unlock(args->mutex);

    while(*args->files_list_size > 0) {
        pthread_mutex_lock(args->mutex);
        PFileArgument chosen_file = chooseFile(args->files_list, args->files_list_size, args->mutex);
        pthread_mutex_unlock(args->mutex);
        printf("size after one delete: %ld\n\n", *args->files_list_size);
        if (chosen_file != NULL)
        {
            printf("Hello from thread %d, chosen file : %s\n", args->id, chosen_file->file_name);
            FILE* input = fopen(chosen_file->file_name, "r");
            int n = 0;
            int nr = 0;
            fscanf(input, "%d\n", &n);
            
            printf("nr:%d\n", n);
            for(int i = 0; i < n; ++i) {
                fscanf(input, "%d\n", &nr);
                printf("nr%d:%d\n", i, nr);
            }
            
            fclose(input);
        }
    }
    
    pthread_barrier_wait(args->barrier);

    pthread_exit(NULL);
}

void *f_R(void *arg)
{
    PThreadArguments args = (PThreadArguments)arg;

    printf("t_id after: %d\n", args->id);

    pthread_exit(NULL);
}

long int calculate_file_size(char *file_name)
{
    // // opening the file in read mode
    FILE *fp = fopen(file_name, "r");

    // checking if the file exist or not
    if (fp == NULL)
    {
        printf("File Not Found!\n");
        return -1;
    }

    fseek(fp, 0L, SEEK_END);

    // calculating the size of the file
    long int res = ftell(fp);

    // closing the file
    fclose(fp);

    return res;
}

int compare_files_by_size_desc(const void *a, const void *b)
{
    PFileArgument f1 = *(PFileArgument *)a;
    PFileArgument f2 = *(PFileArgument *)b;

    // printf("!! f1:%ld si f2:%ld\n", f1->file_size, f2->file_size);
    return -(f1->file_size - f2->file_size);
}

PFileArgument *process_files(FILE *input_file)
{

    char chunk[256];

    int number_of_input_files = atoi(fgets(chunk, sizeof(chunk), input_file));

    PFileArgument *files_list = malloc(number_of_input_files * sizeof(PFileArgument));
    
    if (files_list == NULL)
    {
        perror("Error allocating files list");
        exit(1);
    }

    for (int i = 0; i < number_of_input_files; i++)
    {
        files_list[i] = malloc(sizeof(TFileArgument));

        if (files_list[i] == NULL)
        {
            perror("Error allocating file memory");
            exit(1);
        }

        files_list[i]->file_name = calloc(25, sizeof(char));
    }

    // printf("first: %d\n", number_of_input_files);

    int k = 0;
    int size = 0;
    while (fgets(chunk, sizeof(chunk), input_file) != NULL)
    {

        chunk[strcspn(chunk, "\n")] = 0; // remove unwanted newline at end of file name

        files_list[k]->file_id = k;
        files_list[k]->file_name = strdup(chunk);
        files_list[k]
            ->file_size = calculate_file_size(files_list[k]->file_name);
        size++;

        k++;
    }

    qsort(files_list, size, sizeof(PFileArgument), compare_files_by_size_desc);

    (*files_list)->list_size = size;

    // printf("size before: %ld\n", (*files_list)->list_size);

    return files_list;
}

int main(int argc, char const *argv[])
{
    if (argc < 4)
    {
        perror("Invalid number of arguments");
        exit(1);
    }

    int num_of_mappers = atoi(argv[1]);
    int num_of_reducers = atoi(argv[2]);
    int num_of_total_threads = (num_of_mappers + num_of_reducers);
    
    pthread_t *threads = malloc((num_of_total_threads) * sizeof(pthread_t));
    pthread_barrier_t *barrier = malloc(sizeof(pthread_barrier_t));
    pthread_mutex_t *mutex = malloc(sizeof(pthread_mutex_t));
    
    PPartialSolutionVector partial_solutions = malloc(sizeof(TPartialSolutionVector));
    partial_solutions->vec_size = 0;
    partial_solutions->key_value = malloc(num_of_reducers * sizeof(PKeyValuePair));
    if (partial_solutions->key_value != NULL) {
        for(int i = 0; i < num_of_reducers; ++i) {
            partial_solutions->key_value[i] = malloc(sizeof(TKeyValuePair));
            partial_solutions->key_value[i]->key = i + 2;
            vector_init(&partial_solutions->key_value[i]->values);
        }
    } else {
        perror("Error allocating key_value vec.");
        exit(1);
    }

        if (partial_solutions == NULL)
        {
            perror("Error allocating partial solution vec.");
            exit(1);
        }
    void *status;

    int r = 0;

    FILE *input_file = fopen(argv[3], "r");

    PFileArgument *files_list = process_files(input_file);

    long int files_list_size = (*files_list)->list_size; /////

    // printf("numm: %ld\n", files_list_size);

    // for (int i = 0; i < (*files_list)->list_size; i++)
    // {
    //     printf("File%d\nid:%d\nname:%s\nsize:%ld\nlist_size:%ld\n\n", i, files_list[i]->file_id, files_list[i]->file_name, files_list[i]->file_size, (*files_list)->list_size);
    // }

    if (input_file == NULL)
    {
        perror("File could not be opened");
        exit(1);
    }

    if (threads == NULL)
    {
        perror("Error creating threads");
        exit(1);
    }

    if (barrier == NULL)
    {
        perror("Error creating barrier.");
        exit(1);
    }

    if (mutex == NULL)
    {
        perror("Error creating mutex.");
        exit(1);
    }

    pthread_barrier_init(barrier, NULL, num_of_mappers);
    pthread_mutex_init(mutex, NULL);

    for (int i = 0; i < num_of_total_threads; i++)
    {
        PThreadArguments args = malloc(sizeof(ThreadArguments));

        if (args == NULL) {
            perror("Error allocating thread arguments.");
            exit(1);
        }

        args->id = i;
        args->barrier = barrier;
        args->mutex = mutex;
        args->files_list = files_list;
        args->files_list_size = &files_list_size;

        if (i < num_of_mappers) {
            r = pthread_create(&threads[i], NULL, f_M, args);
            if (r)
            {
                printf("Eroare la crearea thread-ului %d\n", i);
                exit(-1);
            }
        } else {
            r = pthread_create(&threads[i], NULL, f_R, args);

            if (r)
            {
                printf("Eroare la crearea thread-ului %d\n", i);
                exit(-1);
            }
        }

    }

    for (int i = 0; i < num_of_total_threads; i++)
    {
        r = pthread_join(threads[i], &status);

        if (r)
        {
            printf("Eroare la asteptarea thread-ului %d\n", i);
            exit(-1);
        }
    }

    // printf("M : %d, P: %d, file:%s\n", num_of_mappers, num_of_reducers, argv[3]);
    pthread_barrier_destroy(barrier);
    pthread_mutex_destroy(mutex);
    fclose(input_file);
    return 0;
}
