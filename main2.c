#include <stdio.h>
#include <stdlib.h>

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
        b = b / 2;
    }
    return res;
}

int check_for_perf_power(int n)
{
    if (n == 1)
    {
        return n;
    }
    unsigned int lgn = log2n(n);
    printf("lgn: %u\n", lgn);

    int found = 0;
    for (unsigned int b = 2; b < lgn; ++b)
    {
        long unsigned int lowa = 1L;
        long unsigned int higha = 1L << (lgn / b + 1);
        printf("BBB: %u\n", b);
        printf("lowa: %lu\n", lowa);
        printf("higha: %lu\n", higha);
        while (lowa < higha - 1)
        {   
            if (found == 1) {
                found = 0;
                break;
            }
            unsigned int mida = (lowa + higha) >> 1;
            unsigned int ab = custom_pow(mida, b);
            printf("mida: %u\n", mida);
            printf("ab: %u\n", ab);

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
                printf("n: %u, mida: %u, b:%u\n", n, mida, b); // mida ^ b
            }
        }
    }
}

int main(int argc, char const *argv[])
{
    check_for_perf_power(81) ;
    return 0;
}
