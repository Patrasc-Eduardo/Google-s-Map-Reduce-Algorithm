#ifndef ARRAYLIST_H_
#define ARRAYLIST_H_

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define INITIAL_CAPACITY 64
#define min(x, y) (((x) < (y)) ? (x) : (y))

typedef struct _vector
{
    int *array;
    int size;
    int capacity;
} vector;

vector* vector_create()
{
    vector* v = malloc(sizeof(struct _vector));
    if (v == NULL)
    {
        fprintf(stderr, "Not enough memory!");
        abort();
    }
    v->size = 0;
    v->capacity = INITIAL_CAPACITY;
    v->array = (int *)malloc(
        sizeof(int) * v->capacity);
    if (v->array == NULL)
    {
        fprintf(stderr, "Not enough memory!");
        abort();
    }
    return v;
}

void vector_destroy(vector* v)
{
    assert(v);
    free(v->array);
    free(v);
}


static void vector_double_capacity(vector* v)
{
    assert(v);
    int new_capacity = 2 * v->capacity;
    int *new_array = (int *)malloc(
        sizeof(int) * new_capacity);
    if (new_array == NULL)
    {
        fprintf(stderr, "Not enough memory!");
        abort();
    }
    for (int i = 0; i < v->size; i++)
    {
        new_array[i] = v->array[i];
    }
    free(v->array);
    v->array = new_array;
    v->capacity = new_capacity;
}


static void vector_half_capacity(vector* v)
{
    assert(v);
    if (v->capacity <= INITIAL_CAPACITY)
    {
        return;
    }
    int new_capacity = v->capacity / 2;
    int *new_array = (int *)malloc(
        sizeof(int) * new_capacity);
    if (new_array == NULL)
    {
        fprintf(stderr, "Not enough memory!");
        abort();
    }
    for (int i = 0; i < min(v->size, new_capacity); i++)
    {
        new_array[i] = v->array[i];
    }
    free(v->array);
    v->array = new_array;
    v->capacity = new_capacity;
    v->size = min(v->size, new_capacity);
}

void vector_add(vector* v, int value)
{
    assert(v);
    if (v->size >= v->capacity)
    {
        vector_double_capacity(v);
    }
    v->array[v->size++] = value;
}

int vector_get(vector* v, int i)
{
    assert(v);
    if (i < 0 || i >= v->size)
    {
        fprintf(stderr, "Out of index!");
        abort();
    }
    return v->array[i];
}

void vector_put(vector* v, int i, int value)
{
    assert(v);
    if (i < 0 || i >= v->size)
    {
        fprintf(stderr, "Out of index!");
        abort();
    }
    v->array[i] = value;
}

void vector_add_at(vector* v, int i, int value)
{
    assert(v);
    if (i < 0 || i >= v->size)
    {
        fprintf(stderr, "Out of index!");
        abort();
    }
    if (v->size >= v->capacity)
    {
        vector_double_capacity(v);
    }
    for (int j = v->size; j > i; j--)
    {
        v->array[j] = v->array[j - 1];
    }
    v->array[i] = value;
    v->size++;
}

int vector_remove_at(vector* v, int i)
{
    assert(v);
    if (i < 0 || i >= v->size)
    {
        fprintf(stderr, "Out of index!");
        abort();
    }
    int ret = v->array[i];
    for (int j = i + 1; j < v->size; j++)
    {
        v->array[j - 1] = v->array[j];
    }
    v->size--;
    if (4 * v->size < v->capacity)
    {
        vector_half_capacity(v);
    }
    return ret;
}

int vector_is_empty(vector* v)
{
    assert(v);
    return v->size == 0;
}

int vector_size(vector* v)
{
    assert(v);
    return v->size;
}

void vector_clear(vector* v)
{
    assert(v);
    v->size = 0;
    while (v->capacity > INITIAL_CAPACITY)
    {
        vector_half_capacity(v);
    }
}

void print_vector(vector *v)
{   
    printf("{");
    for(int i = 0; i < v->size; i++) {
        printf("%d ", v->array[i]);
    }
    printf("}, ");
    
}
#endif