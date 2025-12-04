#include "../include/lrc/uniform-lrc.hh"
using namespace ClientServer;

UNIFORM_LRC::UNIFORM_LRC(int data, int global, int local, size_t BlockSize)
{
    k_ = data;
    r_ = global;
    p_ = local;
    blocksize_ = BlockSize;
    group_size_ = (data + global) / local;
    remainder_ = (data + global) % local;
    if (remainder_ !=0)
    {
        group_size_++;
    }
    
    if (k_ <= 0 || r_ <= 0 || p_ <= 0 || blocksize_ <= 0)
    {
        throw std::invalid_argument("Invalid parameters: values must be positive.");
    }
}
void UNIFORM_LRC::generate_globalmatrix(int *&final_matrix_g)
{
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void UNIFORM_LRC::generate_localmatrix(int *&final_matrix_l)
{
    int group_size = (k_ + r_) / p_;
    int remainder = 0;
    int *matrix = cauchy_original_coding_matrix(k_, r_, 8);
    int temp = 0;
    int *xor_g = new int[k_];
    for (int i = 0; i < k_; i++)
    {
        for (int j = 0; j < r_; j++)
        {
            temp = temp ^ matrix[j * k_ + i];
        }
        xor_g[i] = temp;
        temp = 0;
    }
    for (int i = 0; i < p_; i++)
    {
        if (i < (p_ - 1))
        {
            for (int j = 0; j < k_; j++)
            {
                if (remainder == 0)
                {
                    if (j >= i * group_size && j < (i + 1) * group_size)
                    {
                        final_matrix_l[i * k_ + j] = 1;
                    }
                    else
                    {
                        final_matrix_l[i * k_ + j] = 0;
                    }
                }
                else
                {
                    if (i < (p_ - remainder))
                    {
                        if (j >= i * (group_size - 1) && j < (i + 1) * (group_size - 1))
                        {
                            final_matrix_l[i * k_ + j] = 1;
                        }
                        else
                        {
                            final_matrix_l[i * k_ + j] = 0;
                        }
                    }
                    else
                    {
                        if (j >= ((p_ - remainder) * (group_size - 1) + (i - (p_ - remainder)) * group_size) && j < ((p_ - remainder) * (group_size - 1) + (i + 1 - (p_ - remainder)) * group_size))
                        {
                            final_matrix_l[i * k_ + j] = 1;
                        }
                        else
                        {
                            final_matrix_l[i * k_ + j] = 0;
                        }
                    }
                }
            }
        }
        else
        {
            for (int j = 0; j < k_; j++)
            {
                if (j >= (k_ + r_ - group_size))
                {
                    final_matrix_l[i * k_ + j] = xor_g[j] ^ 1;
                }
                else
                {
                    final_matrix_l[i * k_ + j] = xor_g[j];
                }
            }
        }
    }
}

// void UNIFORM_LRC::uniform_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs, int blocksize)
// {
//     // Generate the new LRC matrix
//     int *matrix_g = new int[k * r]; // 全局矩阵：k*r
//     int *matrix_l = new int[k * p]; // 局部矩阵：k*p
//     uniform_generate_globalmatrix(k, r, matrix_g);
//     uniform_generate_localmatrix(k, r, p, matrix_l);
//     // for (int  i = 0; i < p; i++)
//     // {
//     //     for (int j = 0; j < k; j++)
//     //     {
//     //         cout<< "local_matrix[" << i << "][" << j << "] = " << new_matrix_l[i * k + j] << endl;
//     //     }

//     // }

//     if (matrix_g == nullptr || matrix_l == nullptr)
//     {
//         cerr << "Error: Matrix generation failed." << endl;
//         return;
//     }

//     jerasure_matrix_encode(k, r, 8, matrix_g, data_ptrs, global_coding_ptrs, blocksize);
//     jerasure_matrix_encode(k, p, 8, matrix_l, data_ptrs, local_coding_ptrs, blocksize);
// }

int UNIFORM_LRC::single_decode_node_need(int fail_one, int *&node_id)
{
    int fail_list[] = {fail_one};
    int start, end, parity_id, group_real;
    int *group_id;
    get_group_id(1, fail_list, group_id);
    parity_id = group_id[0] + k_ + r_;
    if (remainder_ == 0)
    {
        node_id = new int[group_size_];
        start = group_id[0] * group_size_;
        end = start + group_size_;
        group_real = group_size_;
    }
    else
    {
        if (group_id[0] < (p_ - remainder_))
        {
            node_id = new int[group_size_ - 1];
            start = group_id[0] * (group_size_ - 1);
            end = start + group_size_ - 1;
            group_real = group_size_ - 1;
        }
        else
        {
            node_id = new int[group_size_];
            start = (p_ - remainder_) * (group_size_ - 1) + (group_id[0] - (p_ - remainder_)) * group_size_;
            end = start + group_size_;
            group_real = group_size_;
        }
    }

    if (fail_one < end)
    {
        int j = start;
        for (int i = start; i < end; i++)
        {
            if (j != fail_one)
            {
                node_id[i - start] = j;
            }
            else if (j == fail_one)
            {
                node_id[i - start] = j + 1;
                j++;
            }
            j++;
            // cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
        }
        node_id[group_size_ - 1] = parity_id;
    } // 局部校验位lose
    else
    {
        for (int i = start; i < end; i++)
        {
            node_id[i - start] = i;
            // cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
        }
    }
    return group_real;
}

bool UNIFORM_LRC::single_decode(int fail_one, char **data_ptrs, char **code_ptr)
{
    int group_real;
    int decode = -1;
    int *erasures = new int[2];
    int *group_id;
    get_group_id(1, &fail_one, group_id);
    erasures[1] = -1;

    if (remainder_ == 0)
    {
        group_real = group_size_;
        if (fail_one < (k_ + r_))
        {
            erasures[0] = fail_one % group_real;
        }
        else
        {
            erasures[0] = group_real;
        }
    }
    else
    {
        if (group_id[0] < (p_ - remainder_))
        {
            group_real = group_size_ - 1;
            if (fail_one < (k_ + r_))
            {
                erasures[0] = fail_one % group_real;
            }
            else
            {
                erasures[0] = group_real;
            }
        }
        else
        {
            group_real = group_size_;
            if (fail_one < (k_ + r_))
            {
                erasures[0] = (fail_one - (p_ - remainder_) * (group_size_ - 1)) % group_real;
            }
            else
            {
                erasures[0] = group_real;
            }
        }
    }
    int *final_matrix = new int[(group_real + 1) * group_real];

    for (int i = 0; i < (group_real + 1); i++)
    {
        if (i >= 0 && i < group_real)
        {
            for (int j = 0; j < group_real; j++)
            {
                if (j == i)
                {
                    final_matrix[i * group_real + j] = 1;
                }
                else
                {
                    final_matrix[i * group_real + j] = 0;
                }
                // cout << "final_matrix[" << i << "][" << j << "] = " << final_matrix[i * group_size + j] << endl;
            }
        }
        else if (i == group_real)
        {
            for (int j = 0; j < group_real; j++)
            {
                final_matrix[i * group_real + j] = 1;
            }
        }
    }
    decode = jerasure_matrix_decode(group_real, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);

    if (decode == 0)
    {
        cout << "decode success" << endl;
        return true;
    }
    else
    {
        cout << "decode fail" << endl;
        return false;
    }
}

bool UNIFORM_LRC::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
{
    bool repair[fail_num];
    int *group_id;
    get_group_id(fail_num, fail_list, group_id);
    for (int i = 0; i < fail_num; i++)
    {
        int start, end,group_real;
            if (remainder_ == 0)
            {
                start = i * group_size_;
                end = start + group_size_;
                group_real = group_size_;
            }
            else
            {
                if (i < (p_ - remainder_))
                {
                    start = i * (group_size_ - 1);
                    end = start + (group_size_ - 1);
                    group_real = group_size_ - 1;
                }
                else
                {
                    int small_part = (p_ - remainder_) * (group_size_ - 1);
                    start = small_part + (i - (p_ - remainder_)) * group_size_;
                    end = start + group_size_;
                    group_real = group_size_;
                }
            }
            char **data_ptr_ = new char *[group_real];
            if (i < (p_ - 1))
            {
                for (int j = start; j < end; j++)
                {
                    data_ptr_[j - start] = data_ptrs[j];
                    // cout << "data_ptr_[" << j - start << "] = " << strlen(data_ptr_[j - start]) << endl;
                }
            }
            else if (i == (p_ - 1))
            {
                for (int g = (k_ + r_ - 1 - group_size_); g < (k_ + r_ - 1); g++)
                {
                    if (g < k_)
                    {
                        data_ptr_[g - (k_ + r_ - group_size_)] = data_ptrs[g];
                    }
                    else if (g >= k_ && g < (k_ + r_ - 1))
                    {
                        data_ptr_[g - (k_ + r_ - group_size_)] = global_code_ptr[g - k_];
                    }
                }
            }
            bool repair = single_decode(fail_list[i], data_ptr_, local_ptr + group_id[i]);
            if (repair == false)
            {
                cout << "cant repair : " << fail_list[i] << endl;
                return false;
            }
            else
            {
                cout << "repair success : " << fail_list[i] << endl;
                // 将修复后的数据块放回到data_ptrs中
                if (i < (p_ - 1))
                {
                    for (int j = start; j < end; j++)
                    {
                        data_ptrs[j] = data_ptr_[j - start];
                        // cout << "data_ptrs[" << j << "] = " << strlen(data_ptrs[j]) << endl;
                    }
                }
                else if (i == (p_ - 1))
                {
                    for (int j = (k_ + r_ - 1 - group_size_); j < (k_ + r_ - 1); j++)
                    {
                        if (j < k_)
                        {
                            data_ptrs[j] = data_ptr_[j - (k_ + r_ - 1 - group_size_)];
                        }
                        else if (j >= k_ && j < (k_ + r_ - 1))
                        {
                            global_code_ptr[j - k_] = data_ptr_[j - (k_ + r_ - 1 - group_size_)];
                        }
                    }
                }
                // 将修复后的数据块放回到local_ptr中
                // local_ptr[i] = local_ptr[i];
            }
    }
    return true;
}

int UNIFORM_LRC::find_mds_local_parity_blocks(int fail_number, int *fail_list, int *&node_id)
{
    // 若最后一位校验位出错则需要全部的全局校验位
    if (fail_list[fail_number - 1] == (k_ + r_ + p_ - 1))
    {
        for (int i = 0; i < r_; i++)
        {
            node_id[k_ + i] = k_ + i;
        }
        sort(node_id, node_id + k_ + r_);
        auto last = std::unique(node_id, node_id + k_ + r_);
        int new_size_ = last - node_id;
        node_id = (int *)realloc(node_id, new_size_ * sizeof(int));
        if (node_id == nullptr)
        {
            std::cerr << "Memory reallocation failed." << std::endl;
            return -1;
        }
        // 更新count
        int count = new_size_ - k_;
        // 去掉node_id数组中和fail_list数组相同的数值
        std::vector<int> node_id_vector(node_id, node_id + k_ + count);
        node_id_vector.erase(std::remove_if(node_id_vector.begin(), node_id_vector.end(),
                                            [&](int id)
                                            {
                                                return std::find(fail_list, fail_list + fail_number, id) != fail_list + fail_number;
                                            }),
                             node_id_vector.end());

        // 更新node_id数组
        int new_size = node_id_vector.size();
        node_id = (int *)realloc(node_id, new_size * sizeof(int));
        if (node_id == nullptr)
        {
            std::cerr << "Memory reallocation failed." << std::endl;
            return -1;
        }
        std::copy(node_id_vector.begin(), node_id_vector.end(), node_id);

        // 返回新的大小
        return new_size;
    }
    else
    {
        find_k_blocks(fail_number, fail_list, node_id);
        return k_;
    }
}