#include "../include/lrc/grade-lrc.hh"
using namespace std;
#include <algorithm> // For std::min
using namespace ClientServer;

NewLRC::NewLRC(int data, int global, int local, size_t BlockSize)
{
    k_ = data;
    r_ = global;
    p_ = local;
    blocksize_ = BlockSize;
    group_size_ = (data + global) / local;
    remainder_ = (data + global) % local;
    if (k_ <= 0 || r_ <= 0 || p_ <= 0 || blocksize_ <= 0)
    {
        throw std::invalid_argument("Invalid parameters: values must be positive.");
    }
}

void NewLRC::generate_localmatrix(int *&final_matrix_l)
{
    // Generate the original Cauchy matrix
    // 这里的k是数据块数，r是全局校验块数，p是局部校验块数
    // 这里的final_matrix是最终生成的矩阵
    int *x = new int[r_];
    int *y = new int[k_];
    int x_r_1 = k_ + r_ + 1;
    for (int i = 0; i < r_; i++)
    {
        x[i] = i;
    }
    for (int i = 0; i < k_; i++)
    {
        y[i] = i + r_;
    }
    // final_matrix_g = cauchy_xy_coding_matrix(k, r, 8, x, y);
    //  for (int i = 0; i < r; i++)
    //  {
    //      for (int j = 0; j < k; j++)
    //      {
    //          cout<< "final_matrix_g[" << i << "][" << j << "] = " << final_matrix_g[i * k + j] << endl;
    //      }

    // }

    // 局部校验位系数,最后一个局部校验位由g_k+r+1-(alpha[i])得出

    int numerator = 1;
    int denominator = 1;
    int *alpha = new int[k_];
    int *g_k_r_1 = new int[k_];
    int *last_line = new int[k_];
    // Initialize final_matrix_l
    for (int i = 0; i < r_ * k_; i++)
    {
        final_matrix_l[i] = 0;
    }

    // 计算分子：prod (b_{r+1} - b_s) for s=1 to r
    for (int s = 0; s < r_; s++)
    {
        int b_s = x_r_1 ^ x[s];
        numerator = galois_single_multiply(numerator, b_s, 8);
    }
    // cout<< "numerator = " << numerator << endl;

    // 计算分母：prod (a_i - b_t) for t=1 to r+1
    for (int i = 0; i < k_; i++)
    {
        // int b_t_g = y[i];
        for (int j = 0; j <= r_; j++)
        {
            int b_t_a = 0;
            if (j < r_)
            {
                b_t_a = y[i] ^ x[j];
            }
            else
            {
                b_t_a = y[i] ^ x_r_1;
            }
            denominator = galois_single_multiply(denominator, b_t_a, 8);
        }
        alpha[i] = galois_single_divide(numerator, denominator, 8);
        // cout<<"alpha ["<<i<<"] = "<<alpha[i]<<endl;
        denominator = 1;
    }
    // 计算g_k+r+1
    for (int i = 0; i < k_; i++)
    {
        int x_y = x_r_1 ^ y[i];
        // cout<<"x_y = "<<x_y<<endl;
        g_k_r_1[i] = galois_single_divide(1, x_y, 8);
        // cout<<"g["<<i<<"] = "<<g_k_r_1[i]<<endl;
    }

    // 计算局部校验位矩阵
    int local_group_size = (k_ + r_) / p_;
    //int remainder = (k_ + r_) % p_;
    // cout<<"local_group_size = "<<local_group_size<<"remainder = "<<remainder<<endl;
    // p整除k+r
    if (remainder_ == 0)
    {
        for (int i = 0; i < p_; i++)
        {
            if (i >= 0 && i < (p_ - 1))
            {
                for (int j = 0; j < k_; j++)
                {
                    if (j >= i * local_group_size && j < (i + 1) * local_group_size)
                    {
                        final_matrix_l[i * k_ + j] = alpha[j];
                        // cout<< "local_matrix[" << i << "][" << j << "] = " << final_matrix_l[i * k + j] << endl;
                    }
                    else
                    {
                        final_matrix_l[i * k_ + j] = 0;
                        // cout<< "local_matrix[" << i << "][" << j << "] = " << final_matrix_l[i * k + j] << endl;
                    }
                }
            }
            else if (i == (p_ - 1))
            {
                // 局部校验位生成矩阵最后一行
                int temp = (p_ - 1) * local_group_size;
                for (int j = 0; j < k_; j++)
                {
                    if (j >= temp)
                    {
                        final_matrix_l[i * k_ + j] = g_k_r_1[j];
                    }
                    else
                    {
                        final_matrix_l[i * k_ + j] = g_k_r_1[j] ^ alpha[j];
                    }
                    // cout<< "local_matrix[" << i << "][" << j << "] = " << final_matrix_l[i * k + j] << endl;
                }
            }
        }
    }
    else
    {
        int small_group_num = p_ - remainder_;
        for (int i = 0; i < p_; i++)
        {
            if (i != (p_ - 1))
            {
                if (i < small_group_num)
                {
                    for (int j = 0; j < k_; j++)
                    {
                        if (j >= i * (local_group_size - 1) && j < (i + 1) * (local_group_size - 1))
                        {
                            final_matrix_l[i * k_ + j] = alpha[j];
                        }
                        else
                        {
                            final_matrix_l[i * k_ + j] = 0;
                        }
                    }
                }
                else if (i >= small_group_num && i < (p_ - 1))
                {
                    int small_num = small_group_num * (local_group_size - 1);
                    for (int j = 0; j < k_; j++)
                    {
                        if (j >= (small_num + (i - small_num) * local_group_size) && j < (small_num + (i + 1 - small_num) * local_group_size))
                        {
                            final_matrix_l[i * k_ + j] = alpha[j];
                        }
                        else
                        {
                            final_matrix_l[i * k_ + j] = 0;
                        }
                    }
                }
            }
            else if (i == (p_ - 1))
            {
                // 局部校验位生成矩阵最后一行
                int temp = k_ + r_ - local_group_size;
                for (int j = 0; j < k_; j++)
                {
                    if (j >= temp)
                    {
                        final_matrix_l[i * k_ + j] = g_k_r_1[j];
                    }
                    else
                    {
                        final_matrix_l[i * k_ + j] = g_k_r_1[j] ^ alpha[j];
                    }
                }
            }
        }
    }
    // cout << "end" << endl;
}

void NewLRC::generate_globalmatrix(int *&final_matrix_g)
{
    // Generate the original Cauchy matrix
    // 这里的k是数据块数，r是全局校验块数，p是局部校验块数
    // 这里的final_matrix是最终生成的矩阵
    int *x = new int[r_];
    int *y = new int[k_];
    int x_r_1 = k_ + r_ + 1;
    for (int i = 0; i < r_; i++)
    {
        x[i] = i;
    }
    for (int i = 0; i < k_; i++)
    {
        y[i] = i + r_;
    }
    final_matrix_g = cauchy_xy_coding_matrix(k_, r_, 8, x, y);
}

// void NewLRC::new_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs, int blocksize)
// {
//     // Generate the new LRC matrix
//     int *new_matrix_g = new int[k * r]; // 全局矩阵：k*r
//     int *new_matrix_l = new int[k * p]; // 局部矩阵：k*p
//     new_lrc_generate_globalmatrix(k, r, p, new_matrix_g);
//     new_lrc_generate_localmatrix(k, r, p, new_matrix_l);
//     // for (int  i = 0; i < p; i++)
//     // {
//     //     for (int j = 0; j < k; j++)
//     //     {
//     //         cout<< "local_matrix[" << i << "][" << j << "] = " << new_matrix_l[i * k + j] << endl;
//     //     }

//     // }

//     if (new_matrix_g == nullptr || new_matrix_l == nullptr)
//     {
//         cerr << "Error: Matrix generation failed." << endl;
//         return;
//     }

//     jerasure_matrix_encode(k, r, 8, new_matrix_g, data_ptrs, global_coding_ptrs, blocksize);
//     // char **data_ptrs_array_ = data_ptrs;
//     // for (int i = 0; i < r; i++)
//     // {
//     //     data_ptrs_array_[k + i] = global_coding_ptrs[i];
//     // }
//     jerasure_matrix_encode(k , p, 8, new_matrix_l, data_ptrs, local_coding_ptrs, blocksize);
// }

int NewLRC::single_decode_node_need(int fail_one, int *&node_id)
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
        int j =start;
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

bool NewLRC::single_decode(int fail_one, char **data_ptrs, char **code_ptr,size_t decode_size)
{
    int group_real, start, end;
    int decode = -1;
    int *erasures = new int[2];
    int *group_id;
    get_group_id(1, &fail_one, group_id);
    erasures[1] = -1;

    if (decode_size == 0)
    {
        decode_size = blocksize_;
    }

    if (remainder_ == 0)
    {
        group_real = group_size_;
        start = group_id[0] * group_size_;
        end = start + group_size_;
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
            start = group_id[0] * (group_size_ - 1);
            end = start + group_size_ - 1;
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
            start = (p_ - remainder_) * (group_size_ - 1) + (group_id[0] - (p_ - remainder_)) * group_size_;
            end = start + group_size_;
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

    // 单位矩阵
    for (int i = 0; i < group_real; i++)
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

    int *new_matrix_l = new int[k_ * p_];
    generate_localmatrix(new_matrix_l);

    // fail不是最后一组
    if (group_id[0] < (p_ - 1))
    {
        // 赋值final_matrix的最后一行（校验位）
        for (int i = start; i < end; i++)
        {
            final_matrix[group_real * group_real + i - start] = new_matrix_l[group_id[0] * k_ + i];
            // cout << "final_matrix[" << group_size * group_size + i << "] = " << final_matrix[group_size * group_size + i] << endl;
        }
        // 解码
        // cout<< "erasures[0] = " << erasures[0] << endl;
        // cout<<"fail_one = " << fail_one << endl;
        // cout << "group_size = " << group_size << endl;
        // cout << "group_id = " << group_id << endl;
        // if (code_ptr[0] != nullptr)
        // {
        //     cout << "code_ptr[0] = " << strlen(code_ptr[0]) << endl;
        // }
        // else
        // {
        //     cout << "code_ptr[0] is null" << endl;
        // }

        // for (int i = 0; i < group_size; i++)
        // {
        //     if (data_ptrs[i] != nullptr)
        //     {
        //         cout << "data_ptrs[" << i << "] = " << strlen(data_ptrs[i]) << endl;
        //     }
        //     else
        //     {
        //         cout << "data_ptrs[" << i << "] is null" << endl;
        //     }
        // }

        decode = jerasure_matrix_decode(group_real, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, decode_size);
    }
    else
    {
        int numerator = 1;
        int denominator = 1;
        int x_r_1 = k_ + r_ + 1;
        // 计算分子：prod (b_{r+1} - b_s) for s=1 to r
        for (int s = 0; s < r_; s++)
        {
            int b_s = x_r_1 ^ s;
            numerator = galois_single_multiply(numerator, b_s, 8);
        }
        cout << "numerator = " << numerator << endl;

        // 计算分母：prod (a_i - b_t) for t=1 to r+1
        for (int i = (k_ + r_ - group_size_); i < (k_ + r_); i++)
        {
            int b_t_g = -1;
            if (i < k_)
            {
                b_t_g = r_ + i;
            }
            else
            {
                b_t_g = i - k_;
            }
            for (int j = 0; j <= r_; j++)
            {
                if (j != b_t_g)
                {
                    int b_t_a = b_t_g ^ j;
                    denominator = galois_single_multiply(denominator, b_t_a, 8);
                }
            }
            final_matrix[group_real * group_real + i - (k_ + r_ - group_size_)] = galois_single_divide(numerator, denominator, 8);
            //cout << "final_matrix[" << group_real * group_real + i - (k_ + r_ - group_size_) << "] = " << final_matrix[group_real * group_real + i - (k_ + r_ - group_real)] << endl;
            denominator = 1;
        }
        // 解码
        erasures[0] = (fail_one - (k_ + r_ - group_size_)) % group_real;
        decode = jerasure_matrix_decode(group_real, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, decode_size);
    }
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

// int NewLRC::new_get_group_id(int k, int r, int p, int fail_one)
// {
//     int group_id = -1;
//     int group_size = (k + r) / p;
//     int remainder = (k + r) % p;
//     int small_group_num = p - remainder;
//     // cout<< "group_size = " << group_size << endl;
//     // cout<< "remainder = " << remainder << endl;
//     if (fail_one < (k + r))
//     {
//         if (remainder == 0)
//         {
//             group_id = fail_one / group_size;
//             if ((group_id != 0) && (fail_one % group_size != 0))
//                 group_id--;
//             return group_id;
//         }
//         else
//         {
//             int small_part = small_group_num * (group_size - 1);
//             if (fail_one < small_part)
//             {
//                 group_id = fail_one / (group_size - 1);
//                 if ((group_id != 0) && (fail_one % group_size != 0))
//                     group_id = group_id - 1;
//                 return group_id;
//             }
//             else
//             {
//                 group_id = (fail_one - small_part) / group_size + small_group_num;
//                 if ((group_id != small_group_num) && ((fail_one - small_part) % group_size != 0))
//                     group_id = group_id - 1;

//                 return group_id;
//             }
//         }
//     }
//     else
//     {
//         group_id = fail_one - (k + r);
//         return group_id;
//     }
// }

int NewLRC::check_decode(int fail_num, int *fail_list_group_id)
{
    int flag = -1;
    if (fail_num < (r_ + 1))
    {
        flag = 0;
        return flag;
    }
    else if (fail_num == (r_ + 1))
    {
        flag = 1;
        return 1;
    }
    else if (fail_num == (r_ + 2))
    {
        flag = 2;
        return flag;
    }
    else if (fail_num > (r_ + 2) && fail_num <= (r_ + p_))
    {
        flag = fail_num - r_;
        while (fail_num > (r_ + 1))
        {
            if (fail_list_group_id[fail_num - 1] != fail_list_group_id[fail_num - 2])
            {
                fail_num--;
            }
            else if (fail_list_group_id[fail_num - 1] == fail_list_group_id[fail_num - 2])
            {
                cout << "too many failure happened in wrong places" << endl;
                flag = -1;
                return flag;
                break;
            }
        }
        return flag;
    }
    else if (fail_num > (r_ + p_))
    {
        cout << "too many failure" << endl;
        flag = -1;
        return flag;
    }
}

// bool NewLRC::if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id)
// {
//     // 如果fail均为局部校验位，则不需要mds
//     int count = 0;
//     for (int i = 0; i < fail_num; i++)
//     {
//         if (fail_list[i] >= (k + r))
//         {
//             count++;
//         }
//     }
//     if (count == fail_num)
//     {
//         cout << "no need mds" << endl;
//         return false;
//     }
//     else
//     {
//         int *group_fail_num = new int[p]();
//         for (int j = 0; j < p; j++)
//         {
//             for (int i = 0; i < fail_num; i++)
//             {
//                 if (fail_list_group_id[i] == j)
//                 {
//                     group_fail_num[j]++;
//                 }
//             }
//             // cout << "group_fail_num[" << j << "] = " << group_fail_num[j] << endl;
//         }

//         for (int i = 0; i < p; i++)
//         {
//             if (group_fail_num[i] > 1)
//             {
//                 cout << "need mds" << endl;
//                 return true;
//             }
//         }
//         int single_repair_cost = get_repair_num(k, r, p, fail_num, group_fail_num);
//         if (single_repair_cost >= k)
//         {
//             cout << "need mds" << endl;
//             return true;
//         }

//         cout << "no need mds" << endl;
//         return false;
//     }
// }

// int NewLRC::get_repair_num(int k, int r, int p, int fail_num, int *fail_list_group_id)
// {
//     int group_size = (k + r) / p;
//     int remainder = (k + r) % p;
//     int small_group_num = p - remainder;
//     if (remainder == 0)
//     {
//         return group_size * fail_num;
//     }
//     else
//     {
//         int count_1 = 0;
//         int count_2 = 0;
//         for (int i = 0; i < fail_num; i++)
//         {
//             if (fail_list_group_id[i] < small_group_num)
//             {
//                 count_1++;
//             }
//             else
//             {
//                 count_2++;
//             }
//         }
//         return (count_1 * (group_size - 1) + count_2 * group_size);
//     }
// }

// void NewLRC::find_k_blocks(int k, int r, int locality, int fail_num, int *fail_list, int *&node_id)
// {
//     node_id = new int[k - locality];
//     int count = 0;
//     for (int i = 0; i < k + r && count < (k - locality); i++)
//     {
//         bool is_failed = false;
//         for (int j = 0; j < fail_num; j++)
//         {
//             if (fail_list[j] == i)
//             {
//                 is_failed = true;
//                 break;
//             }
//         }
//         if (!is_failed)
//         {
//             node_id[count++] = i;
//         }
//     }
// }

// int NewLRC::muti_decode_node_need(int fail_number, int *fail_list, int *&node_id)
// {
//     int *fail_group_id;
//     get_group_id(fail_number, fail_list, fail_group_id);
//     // 是否可以修复
//     int flag = check_decode(fail_number, fail_group_id);
//     // 是否需要mds修复
//     bool if_mds = if_need_mds(fail_number, fail_list, fail_group_id);
//     if (flag != -1)
//     {

//         bool fail_group = false;
//         for (int i = 0; i < fail_number; i++)
//         {
//             if (fail_group_id[i] != fail_group_id[i + 1])
//             {
//                 fail_group = true;
//                 break;
//             }
//         }
//         if (flag == 2 && !fail_group)
//         {
//             int r_2_need = find_k_g_blocks(fail_number, fail_list, node_id);
//             return r_2_need;
//         }
//         else
//         {
//             if (if_mds == false)
//             {
//                 int repair_num = get_repair_num(fail_number, fail_group_id);
//                 node_id = new int[repair_num];
//                 int node_need_num = 0;
//                 int node_need = 0;
//                 for (int i = 0; i < fail_number && node_need_num < repair_num; i++)
//                 {
//                     int *node_id_split = nullptr;
//                     node_need = single_decode_node_need(fail_list[i], node_id_split);
//                     if (node_need != 0)
//                     {
//                         for (int j = 0; j < node_need; j++)
//                         {

//                             node_id[node_need_num] = node_id_split[j];
//                             node_need_num++;
//                         }
//                     }
//                     else
//                     {
//                         cerr << "Error: node_id_split is null." << endl;
//                     }
//                     delete[] node_id_split;
//                     node_need = 0;
//                 }
//                 // resize 去掉重复的值
//                 node_need_num = resize_node_id(node_need_num, fail_number, fail_list, node_id);
//                 return node_need_num;
//             }
//             else
//             {
//                 int mds_local = find_mds_local_parity_blocks(fail_number, fail_list, node_id);
//                 // 去掉重复的值
//                 // mds_local = resize_node_id(mds_local,fail_number,fail_list,node_id);
//                 return mds_local;
//             }
//         }
//     }
//     else
//     {
//         cout << "can not repair" << endl;
//         return -1;
//     }
// }

// int NewLRC::find_k_g_blocks(int fail_num, int *fail_list, int *&node_id)
// {
//     int count = 0;
//     for (int i = 0; i < (k_ + r_ + p_); i++)
//     {
//         bool is_failed = false;
//         for (int j = 0; j < fail_num; j++)
//         {
//             if (fail_list[j] == i)
//             {
//                 is_failed = true;
//                 break;
//             }
//         }
//         if (!is_failed)
//         {
//             node_id[count] = i;
//             count++;
//         }
//     }

//     return count;
// }

// int NewLRC::resize_node_id(int k, int count, int fail_num, int *fail_list, int *&node_id)
// {
//     // 去掉重复的节点
//     std::sort(node_id, node_id + k + count);
//     auto last = std::unique(node_id, node_id + k + count);
//     int new_size_ = last - node_id;
//     node_id = (int *)realloc(node_id, new_size_ * sizeof(int));
//     if (node_id == nullptr)
//     {
//         std::cerr << "Memory reallocation failed." << std::endl;
//         return -1;
//     }
//     // 更新count
//     count = new_size_ - k;
//     // 去掉node_id数组中和fail_list数组相同的数值
//     std::vector<int> node_id_vector(node_id, node_id + k + count);
//     node_id_vector.erase(std::remove_if(node_id_vector.begin(), node_id_vector.end(),
//                                         [&](int id)
//                                         {
//                                             return std::find(fail_list, fail_list + fail_num, id) != fail_list + fail_num;
//                                         }),
//                          node_id_vector.end());

//     // 更新node_id数组
//     int new_size = node_id_vector.size();
//     node_id = (int *)realloc(node_id, new_size * sizeof(int));
//     if (node_id == nullptr)
//     {
//         std::cerr << "Memory reallocation failed." << std::endl;
//         return -1;
//     }
//     std::copy(node_id_vector.begin(), node_id_vector.end(), node_id);

//     // 返回新的大小
//     return new_size;
// }

bool NewLRC::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_ptrs, char **local_ptr,size_t decode_size)
{
    bool repair[fail_num];
    int *group_id;
    get_group_id(fail_num, fail_list, group_id);
    for (int i = 0; i < fail_num; i++)
    {
        repair[i] = false;
        char **data_ptr_;
        char **code_ptr_ = new char *[1];
        if (group_id[i] < (p_ - 1))
        {
            if (remainder_ == 0)
            {
                data_ptr_ = new char *[group_size_];
                for (int j = group_id[i] * group_size_; j < (group_id[i] + 1) * group_size_; j++)
                {
                    data_ptr_[j - group_id[i] * group_size_] = data_ptrs[j];
                    cout << "data_ptr_[" << j - i * group_size_ << "] = " << strlen(data_ptr_[j - i * group_size_]) << endl;
                }
                code_ptr_[0] = local_ptr[group_id[i]];
            }
            else
            {
                if (group_id[i] < (p_ - remainder_))
                {
                    data_ptr_ = new char *[group_size_ - 1];
                    for (int j = group_id[i] * (group_size_ - 1); j < (group_id[i] + 1) * (group_size_ - 1); j++)
                    {
                        data_ptr_[j - (group_id[i] * (group_size_ - 1))] = data_ptrs[j];
                        cout << "data_ptr_[" << j - i * group_size_ << "] = " << strlen(data_ptr_[j - i * group_size_]) << endl;
                    }
                    code_ptr_[0] = local_ptr[group_id[i]];
                }
                else
                {
                    data_ptr_ = new char *[group_size_];
                    int small_part = (p_ - remainder_) * (group_size_ - 1);
                    for (int j = small_part + (group_id[i] - (p_ - remainder_)) * group_size_; j < small_part + (group_id[i] + 1 - (p_ - remainder_)) * group_size_; j++)
                    {
                        data_ptr_[j - (small_part + (group_id[i] - (p_ - remainder_)) * group_size_)] = data_ptrs[j];
                        cout << "data_ptr_[" << j - i * group_size_ << "] = " << strlen(data_ptr_[j - i * group_size_]) << endl;
                    }
                    code_ptr_[0] = local_ptr[group_id[i]];
                }
            }
        }
        else if (group_id[i] == p_ - 1)
        {
            data_ptr_ = new char *[group_size_];
            for (int i = (k_ + r_ - group_size_); i < (k_ + r_); i++)
            {
                if (i < k_)
                {
                    data_ptr_[i - (k_ + r_ - group_size_)] = data_ptrs[i];
                }
                else if (i >= k_ && i < (k_ + r_))
                {
                    data_ptr_[i - (k_ + r_ - group_size_)] = global_ptrs[i - k_];
                }
            }
            code_ptr_[0] = local_ptr[group_id[i]];
        }
        repair[i] = single_decode(fail_list[i], data_ptr_, code_ptr_,decode_size);
        if (repair[i] == false)
        {
            cout << "cant repair : " << fail_list[i] << endl;
            return false;
            break;
        }
    }
    return true;
}
int NewLRC::find_mds_local_parity_blocks(int fail_number, int *fail_list, int *&node_id)
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
// bool NewLRC::muti_decode( int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
// {
//     // 找到每个fail节点的group_id
//     int *group_id = new int[fail_num];
//     for (int i = 0; i < fail_num; i++)
//     {
//         group_id[i] = new_get_group_id(k, r, p, fail_list[i]);
//         // std::cout << "group_id[" << i << "] = " << group_id[i] << endl;
//     }
//     int local_repair = check_decode(k, r, p, fail_num, group_id);
//     // 每组故障节点数
//     int count = new_fail_localparity_num(k, r, p, fail_num, fail_list);
//     bool need_mds = if_need_mds(k, r, p, fail_num, fail_list, group_id);
//     if (local_repair != -1)
//     {
//         if (need_mds == true)
//         {
//             bool mds_repair = false;
//             if (local_repair == 0)
//             {
//                 if (count == 0)
//                 {
//                     mds_repair = new_mds_decode(k, r, local_repair, fail_num, fail_list, data_ptrs, global_code_ptr, blocksize);
//                     return mds_repair;
//                 }
//                 else
//                 {
//                     int fail_num_ = fail_num - count;
//                     int *fail_list_ = new int[fail_num_];
//                     std::copy(fail_list, fail_list + fail_num_, fail_list_);
//                     mds_repair = new_mds_decode(k, r, local_repair, fail_num_, fail_list_, data_ptrs, global_code_ptr, blocksize);
//                     // 修复校验位
//                     int *fail_list_local = new int[count];
//                     std::copy(fail_list + fail_num_, fail_list + fail_num, fail_list_local);
//                     for (int i = 0; i < p; i++)
//                     {
//                         cout << "local_ptr[" << i << "] = " << strlen(local_ptr[i]) << endl;
//                     }

//                     bool mutiple_single_repair = mutiple_single_decode(k, r, p, count, fail_list_local, data_ptrs, local_ptr, blocksize);
//                     if (mds_repair == true && mutiple_single_repair == true)
//                     {
//                         return true;
//                     }
//                     else
//                     {
//                         cout << "cant repair" << endl;
//                         return false;
//                     }
//                     delete[] fail_list_;
//                     delete[] fail_list_local;
//                 }
//             }
//             else
//             {
//                 // local_repair != 0
//                 if (count == 0)
//                 {
//                     // 还需要local_repair局部校验位
//                     for (int i = 0; i < local_repair; i++)
//                     {
//                         global_code_ptr[r + i] = local_ptr[i];
//                     }
//                     mds_repair = new_mds_decode(k, r, local_repair, fail_num, fail_list, data_ptrs, global_code_ptr, blocksize);
//                     return mds_repair;
//                 }
//                 else
//                 {
//                     int fail_num_ = fail_num - count;
//                     int *fail_list_ = new int[fail_num_];
//                     std::copy(fail_list, fail_list + fail_num_, fail_list_);
//                     for (int i = 0; i < p; i++)
//                     {
//                         global_code_ptr[k + i] = local_ptr[i];
//                     }
//                     mds_repair = new_mds_decode(k, r, p, fail_num_, fail_list_, data_ptrs, global_code_ptr, blocksize);
//                     // 修复校验位
//                     int *fail_list_local = new int[count];
//                     std::copy(fail_list + fail_num_, fail_list + fail_num, fail_list_local);
//                     bool mutiple_single_repair = mutiple_single_decode(k, r, p, count, fail_list_, data_ptrs, local_ptr, blocksize);
//                     if (mds_repair == true && mutiple_single_repair == true)
//                     {
//                         return true;
//                     }
//                     else
//                     {
//                         cout << "cant repair" << endl;
//                         return false;
//                     }
//                     delete[] fail_list_;
//                     delete[] fail_list_local;
//                 }
//             }
//             return true;
//         }
//         else
//         {
//             bool mutiple_single_repair = mutiple_single_decode(k, r, p, fail_num, fail_list, data_ptrs, local_ptr, blocksize);
//             return mutiple_single_repair;
//         }
//     }
// }



    // debug
    //  for (int i = 0; i < (k+r); i++)
    //  {
    //      if (i< k)
    //      {
    //          cout << "data_ptrs["<<i<<"] = " << string(data_ptrs[i]).size() << endl;
    //      }else{
    //          cout << "code_ptr["<<i<<"] = " << string(code_ptr[i-k]).size() << endl;
    //      }

    // }

//     int decode = jerasure_matrix_decode(k, r + locality, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize);
//     if (decode == 0)
//     {
//         cout << "MDS decode success" << endl;
//         return true;
//     }
//     else
//     {
//         cout << "MDS decode fail" << endl;
//         return false;
//     }
//     delete[] final_matrix;
//     delete[] erasures;
//     delete[] global_parity;
// }
