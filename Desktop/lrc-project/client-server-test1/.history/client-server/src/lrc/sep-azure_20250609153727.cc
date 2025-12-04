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

void SEP_AZURE::generate_globalmatrix(int *&final_matrix_g)
{
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void SEP_AZURE::generate_localmatrix(int *&final_matrix_l)
{
    int *temp_matrix = new int[k_ * r_];
    generate_globalmatrix(temp_matrix);

    for (int i = 0; i < p_; i++)
    {
        for (int j = 0; j < k_; j++)
        {
            if (j >= i * group_size_ && j < (i + 1) * group_size_)
            {
                final_matrix_l[i * k_ + j] = temp_matrix[(r_ - 1) * k_ + j];
            }
            else
            {
                final_matrix_l[i * k_ + j] = 0;
            }
        }
    }
}

int SEP_AZURE::single_decode_node_need(int fail_one, int *&node_id)
{
    int return_group_size;
    // top层出错(k+r-1 - k+r+p-1)
    if (fail_one >= (k_ + r_ - 1) && fail_one < (k_ + r_ + p_))
    {
        return_group_size = p_;
        node_id = new int[p_];
        int g = k_ + r_ - 1;
        for (int i = (k_ + r_ - 1); i < (k_ + r_ + p_); i++)
        {
            if (g != fail_one)
            {
                node_id[i - (k_ + r_ - 1)] = g;
            }
            else if (g == fail_one)
            {
                node_id[i - (k_ + r_ - 1)] = g + 1;
                g++;
            }
            g++;
            // cout << "node_id[" << i - (k_ + r_ - 1) << "] = " << node_id[i - (k_ + r_ - 1)] << endl;
        }
    } // lower层出错（0-k+r-2)
    else
    {
        int fail_list[] = {fail_one};
        int start, end, parity_id;
        int *group_id;
        get_group_id(1, fail_list, group_id);
        parity_id = group_id[0] + k_ + r_;

        node_id = new int[group_size_];
        start = group_id[0] * group_size_;
        end = start + group_size_;
        return_group_size = group_size_;

        // cout<<"start : "<<start <<" end : "<<end<<endl;
        int g = start;
        for (int l = start; l < end; l++)
        {
            if (g != fail_one)
            {
                node_id[l - start] = g;
            }
            else if (g == fail_one)
            {
                node_id[l - start] = g + 1;
                g++;
            }
            g++;
            // cout << "node_id[" << l - start << "] = " << node_id[l - start] << endl;
        }
        node_id[return_group_size - 1] = parity_id;
    }
    // cout<<"return : "<<return_group_size<<endl;
    return return_group_size;
}