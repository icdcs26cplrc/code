#include "../include/lrc/sep-azure.hh"
using namespace ClientServer;

SEP_AZURE::SEP_AZURE(int k, int r, int p, size_t blocksize)
{
    k_ = k;
    r_ = r;
    p_ = p;
    blocksize_ = blocksize;
    group_size_ = k / p;
    remainder_ = k % p;
    if (remainder_ != 0)
    {
        group_size_++;
    }
    if (k <= 0 || r <= 0 || p <= 0 || blocksize <= 0)
    {
        throw std::invalid_argument("Invalid parameters: values must be positive.");
    }
}