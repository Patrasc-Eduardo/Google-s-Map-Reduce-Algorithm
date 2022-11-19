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

typedef struct key_value_pair
{
    int key;
    vector *values;
} TKeyValuePair, *PKeyValuePair;

typedef struct mappers_lists
{
    vector **lists;
    int num_of_lists;
    int increment;
} TMappersList, *PMappersList;

typedef struct partial_solution_vec
{
    PKeyValuePair *key_value; // ** key_value
    long int vec_size;
} TPartialSolutionVector, *PPartialSolutionVector;

typedef struct threads_args
{
    int id;
    pthread_barrier_t *barrier;
    pthread_mutex_t *mutex;
    PFileArgument *files_list;
    long int *files_list_size;
    PPartialSolutionVector partial_list;
    PMappersList *mappers_lists;
    int num_of_mappers;
    int num_of_reducers;
} ThreadArguments, *PThreadArguments;

void printPartialSolutionsVector(PPartialSolutionVector partial_list)
{
    for (int i = 0; i < partial_list->vec_size; ++i)
    {
        printf("key:%d\n", partial_list->key_value[i]->key);
        printf("values: ");
        print_vector(partial_list->key_value[i]->values);
        printf("\n");
    }
}

int simple_comp_func(const void *a, const void *b)
{
    return (*(int *)a - *(int *)b);
}

void print_mappers_lists(PMappersList *mappers_list, int num_of_reducers)
{
    for (int i = 0; i < (*mappers_list)->num_of_lists; ++i)
    {
        printf("mapper number :%d\n", i);
        printf("values: ");

        for (int j = 0; j < num_of_reducers; ++j)
        {
            print_vector(mappers_list[i]->lists[j]);
        }
        printf("\n");
    }
}

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

    // pthread_mutex_lock(mutex);
    save_file = remove_from_array_at_pos(0, files_list, files_list_size);
    // pthread_mutex_unlock(mutex);
    return save_file;
}

unsigned int log2n(unsigned int n)
{
    return (n > 1) ? 1 + log2n(n / 2) : 0;
}

unsigned int custom_pow(unsigned int a, unsigned int b) // a^b
{
    unsigned int res = 1;
    while (b)
    {
        if (b & 1)
        {
            res = res * a;
        }
        a = a * a;
        b = b >> 1;
    }
    return res;
}

void check_for_perf_power(int n, PPartialSolutionVector partial_solutions, PMappersList *mappers_lists, int mapper_id, int num_of_reducers)
{
    if (n == 1)
    {
        for (int i = 0; i < (*mappers_lists)->num_of_lists; ++i)
        {
            for (int j = 0; j < num_of_reducers; ++j)
            {
                vector_add(mappers_lists[i]->lists[j], 1);
            }

            (*mappers_lists)->increment += 1;
            // vector_add(&partial_solutions->key_value[i]->values, 1);
        }
    }
    unsigned int lgn = log2n(n);
    // printf("lgn: %u\n", lgn);

    int found = 0;
    for (unsigned int b = 2; b <= lgn; ++b)
    {
        long unsigned int lowa = 1L;
        long unsigned int higha = 1L << (lgn / b + 1);
        // printf("BBB: %u\n", b);
        // printf("lowa: %lu\n", lowa);
        // printf("higha: %lu\n", higha);
        while (lowa < higha - 1)
        {
            if (found == 1)
            {
                found = 0;
                break;
            }
            unsigned int mida = (lowa + higha) >> 1;
            unsigned int ab = custom_pow(mida, b);
            // printf("mida: %u\n", mida);
            // printf("ab: %u\n", ab);

            if (ab > n)
            {
                higha = mida;
            }
            else if (ab < n)
            {
                lowa = mida;
            }
            else
            {
                found = 1;
                // printf("n: %u, mida: %u, b:%u\n", n, mida, b); // mida ^ b
                // TODO put in map

                // partial_solutions->key_value[b - 2]->key = b;
                // vector_add(&partial_solutions->key_value[b - 2]->values, ab);
                //printf("b: %d ab:%d\n", b - 2, ab);

                // if (ab) % 4 == 0) {
                //     printf("for 4:%d\n", b - 2);
                // }
                // if ((b - 2) % 5 == 0)
                // {
                //     printf("for 5:%d\n", b - 2);
                // }
                // printf("FOUNDDD, mida:%d, n/ab:%d, b:%d\n", mida, n, b);
                if (b == 5) {
                    printf("555555, mida:%d, n/ab:%d, b:%d\n", mida, n, b);
                }

                if (b <= (num_of_reducers + 1)) {
                    vector_add(mappers_lists[mapper_id]->lists[b - 2], ab);
                } else {
                    printf("FOUNDDD, mida:%d, n/ab:%d, b:%d\n", mida, n, b);
                    // for(int i = 0; i < num_of_reducers; ++i) {
                    //     if ((b-2) % (i + 2) == 0) {
                    //         vector_add(mappers_lists[mapper_id]->lists[i], ab);
                    //     }
                    // }
                }
                // vector_add(mappers_lists[mapper_id]->lists[b - 2], ab);
                (*mappers_lists)->increment += 1;
            }
        }
    }
}
void *f_M(void *arg)
{
    PThreadArguments args = (PThreadArguments)arg;

    if (args->id < args->num_of_mappers)
    {

        while (*args->files_list_size > 0)
        {
            PFileArgument chosen_file;
            pthread_mutex_lock(args->mutex);
            chosen_file = chooseFile(args->files_list, args->files_list_size, args->mutex);
            pthread_mutex_unlock(args->mutex);

            if (chosen_file != NULL)
            {
                // pthread_mutex_lock(args->mutex);
                printf("Hello from Mapper %d, chosen file : %s\n", args->id, chosen_file->file_name);
                // pthread_mutex_unlock(args->mutex);
                FILE *input = fopen(chosen_file->file_name, "r");
                int n = 0;
                int nr = 0;
                fscanf(input, "%d\n", &n);

                for (int i = 0; i < n; ++i)
                {

                    fscanf(input, "%d\n", &nr);

                    // pthread_mutex_lock(args->mutex);
                    // printf("before adding\n");
                    // print_mappers_lists(args->mappers_lists, 5);

                    check_for_perf_power(nr, args->partial_list, args->mappers_lists, args->id, args->num_of_reducers);

                    // print_mappers_lists(args->mappers_lists, 3);
                    // pthread_mutex_unlock(args->mutex);
                }

                fclose(input);
            }
        }
        // pthread_barrier_wait(args->barrier);
    }

    pthread_barrier_wait(args->barrier);

    if (args->id >= args->num_of_mappers)
    {
        int reducer_id = args->id - args->num_of_mappers;
        int key_exponent = (reducer_id + 2);
        int num_of_uniques = 0;
        // for (int i = 0; i < args->num_of_reducers; ++i)
        //{
        args->partial_list->key_value[reducer_id]->key = key_exponent;
        for (int j = 0; j < args->num_of_mappers; ++j)
        {
            int list_size = args->mappers_lists[j]->lists[reducer_id]->size;
            for (int k = 0; k < list_size; ++k)
            {
                vector_add(args->partial_list->key_value[reducer_id]->values, args->mappers_lists[j]->lists[reducer_id]->array[k]);
            }
        }
        //}
        int *arr = args->partial_list->key_value[reducer_id]->values->array;
        int size = args->partial_list->key_value[reducer_id]->values->size;
        qsort(arr, size, sizeof(int), simple_comp_func);

        //write in specific file
        if (key_exponent <= 5) {
            char filename[25];
            // snprintf(filename, sizeof(filename), "test%d/out%d.txt", args->files_list[0]->file_id, key_exponent);
            snprintf(filename, sizeof(filename), "out%d.txt", key_exponent);

            FILE *fout = fopen(filename, "w");

            if (fout == NULL)
            {
                printf("filename: %s\n", filename);
                perror("Error opening write file");
                exit(1);
            }

            int save = arr[0];
            if (size > 0)
            {
                num_of_uniques = 1;
            }

            for (int i = 1; i < size; ++i)
            {
                if (arr[i] != save)
                {
                    num_of_uniques++;
                    save = arr[i];
                }
            }

            fprintf(fout, "%d", num_of_uniques);
            fclose(fout);
        }
        
    }
    pthread_exit(NULL);

    // return args->mappers_lists;
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

        files_list[k]->file_id = atoi(&chunk[4]); // specifies test index (eg test0, test1, etc)
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
    partial_solutions->vec_size = num_of_reducers;
    partial_solutions->key_value = malloc(num_of_reducers * sizeof(PKeyValuePair));

    if (partial_solutions == NULL)
    {
        perror("Error allocating partial solution vec.");
        exit(1);
    }
    if (partial_solutions->key_value != NULL)
    {
        for (int i = 0; i < num_of_reducers; ++i)
        {
            partial_solutions->key_value[i] = malloc(sizeof(TKeyValuePair));
            partial_solutions->key_value[i]->key = i + 2;
            ////vector_create(&partial_solutions->key_value[i]->values);
            partial_solutions->key_value[i]->values = vector_create();
        }
    }
    else
    {
        perror("Error allocating key_value vec.");
        exit(1);
    }

    PMappersList *mappers_lists = malloc(num_of_mappers * sizeof(PMappersList));

    if (mappers_lists == NULL)
    {
        perror("Error allocating mappers_lists.");
        exit(1);
    }

    for (int i = 0; i < num_of_mappers; i++)
    {
        mappers_lists[i] = malloc(sizeof(TMappersList));
        mappers_lists[i]->lists = malloc(num_of_reducers * sizeof(vector *));
        if (mappers_lists[i]->lists == NULL)
        {
            perror("Error allocating lists for each mapper");
            exit(-1);
        }

        /////printf("NUM OF REDUCERS: %d\n", num_of_reducers);
        for (int j = 0; j < num_of_reducers; ++j)
        {
            // mappers_lists[i]->lists[i] = malloc(sizeof(vector));
            ////vector_init(&mappers_lists[i]->lists[j]);
            mappers_lists[i]->lists[j] = vector_create();
        }
    }

    (*mappers_lists)->num_of_lists = num_of_mappers;
    (*mappers_lists)->increment = 0;
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

    pthread_barrier_init(barrier, NULL, num_of_mappers + num_of_reducers);
    pthread_mutex_init(mutex, NULL);
    PThreadArguments *args_array = malloc((num_of_mappers + num_of_reducers) * sizeof(PThreadArguments));

    if (args_array == NULL)
    {
        perror("Error allocating args array\n");
        exit(-1);
    }

    for (int i = 0; i < (num_of_reducers + num_of_mappers); ++i)
    {
        args_array[i] = malloc(sizeof(ThreadArguments));

        args_array[i]->id = i;
        args_array[i]->barrier = barrier;
        args_array[i]->mutex = mutex;
        args_array[i]->files_list = files_list;
        args_array[i]->files_list_size = &files_list_size;
        args_array[i]->partial_list = partial_solutions;
        args_array[i]->mappers_lists = mappers_lists;
        args_array[i]->num_of_mappers = num_of_mappers;
        args_array[i]->num_of_reducers = num_of_reducers;
    }

    for (int i = 0; i < num_of_total_threads; i++)
    {
        // PThreadArguments args = malloc(sizeof(ThreadArguments));

        // if (args == NULL)
        // {
        //     perror("Error allocating thread arguments.");
        //     exit(1);
        // }

        // args->id = i;
        // args->barrier = barrier;
        // args->mutex = mutex;
        // args->files_list = files_list;
        // args->files_list_size = &files_list_size;
        // args->partial_list = partial_solutions;
        // args->mappers_lists = mappers_lists;
        // args->num_of_mappers = num_of_mappers;
        // args->num_of_reducers = num_of_reducers;

        // if (i < num_of_mappers)
        // {
        r = pthread_create(&threads[i], NULL, f_M, args_array[i]);
        if (r)
        {
            printf("Eroare la crearea thread-ului %d\n", i);
            exit(-1);
        }
        //}
        // else
        // {
        //     r = pthread_create(&threads[i], NULL, f_R, args);

        //     if (r)
        //     {
        //         printf("Eroare la crearea thread-ului %d\n", i);
        //         exit(-1);
        //     }
        // }
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

    // printPartialSolutionsVector(partial_solutions);
    printf("\n\n");
    print_mappers_lists(mappers_lists, num_of_reducers);
    printf("\n\n");
    // printf("M : %d, P: %d, file:%s\n", num_of_mappers, num_of_reducers, argv[3]);
    printf("\n\n");
    printPartialSolutionsVector(partial_solutions);
    printf("\n\n");

    pthread_barrier_destroy(barrier);
    pthread_mutex_destroy(mutex);
    fclose(input_file);
    return 0;
}
