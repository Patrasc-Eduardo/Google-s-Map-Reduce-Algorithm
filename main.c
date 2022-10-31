#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

typedef struct threads_args
{
    int id;
    pthread_barrier_t *barrier;
} ThreadArguments, *PThreadArguments;

typedef struct file_args
{
    int file_id;
    char *file_name;
    long int file_size;
    long int list_size;
} TFileArgument, *PFileArgument;

void *f_M(void *arg)
{
    PThreadArguments args = (PThreadArguments)arg;

    printf("t_id bef: %d\n", args->id);

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

    printf("!! f1:%ld si f2:%ld\n", f1->file_size, f2->file_size);
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

    printf("size before: %ld\n", (*files_list)->list_size);

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
    void *status;

    int r = 0;

    FILE *input_file = fopen(argv[3], "r");

    PFileArgument *files_list = process_files(input_file);


    printf("numm: %ld\n", (*files_list)->list_size);
    for (int i = 0; i < (*files_list)->list_size; i++)
    {
        printf("File%d\nid:%d\nname:%s\nsize:%ld\nlist_size:%ld\n\n", i, files_list[i]->file_id, files_list[i]->file_name, files_list[i]->file_size, (*files_list)->list_size);
    }

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

    pthread_barrier_init(barrier, NULL, num_of_mappers);

    // for (int i = 0; i < num_of_total_threads; i++)
    // {
    //     PThreadArguments args = malloc(sizeof(ThreadArguments));

    //     if (args == NULL) {
    //         perror("Error allocating thread arguments.");
    //         exit(1);
    //     }

    //     args->id = i;
    //     args->barrier = barrier;

    //     if (i < num_of_mappers) {
    //         r = pthread_create(&threads[i], NULL, f_M, args);

    //         if (r)
    //         {
    //             printf("Eroare la crearea thread-ului %d\n", i);
    //             exit(-1);
    //         }
    //     } else {
    //         r = pthread_create(&threads[i], NULL, f_R, args);

    //         if (r)
    //         {
    //             printf("Eroare la crearea thread-ului %d\n", i);
    //             exit(-1);
    //         }
    //     }

    // }

    // for (int i = 0; i < num_of_total_threads; i++)
    // {
    //     r = pthread_join(threads[i], &status);

    //     if (r)
    //     {
    //         printf("Eroare la asteptarea thread-ului %d\n", i);
    //         exit(-1);
    //     }
    // }

    // printf("M : %d, P: %d, file:%s\n", num_of_mappers, num_of_reducers, argv[3]);
    fclose(input_file);
    return 0;
}
