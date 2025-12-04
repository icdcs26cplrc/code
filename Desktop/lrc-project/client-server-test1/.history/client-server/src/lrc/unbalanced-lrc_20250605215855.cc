#include "../include/lrc/unbalaced-lrc.hh"
using namespace ClientServer;

UNBALANCED_LRC::UNBALANCED_LRC(int data, int global, int local, size_t BlockSize)
{
    k_ = data;
    r_ = global;
    p_ = local;
    blocksize_ = BlockSize;
    group_size_ = (data + global  - 1) / local;
    remainder_ = (data + global - 1) % local;
    if (remainder_ !=0)
    {
        group_size_++;
    }
    
    cout<<"group_size_ : "<<group_size_<<" remainder_ :"<<remainder_<<endl;
    if (k_ <= 0 || r_ <= 0 || p_ <= 0 || blocksize_ <= 0)
    {
        throw std::invalid_argument("Invalid parameters: values must be positive.");
    }
}

void UNBALANCED_LRC::generate_globalmatrix(int *&final_matrix_g)
{
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void UNBALANCED_LRC::generate_localmatrix(int *&final_matrix_l)
{
    final_matrix_l = new int[p_*k_];
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

    // 局部校验位系数,最后一个局部校验位由g_k+r+1-(alpha[i])得出

    int numerator = 1;
    int denominator = 1;
    int *alpha = new int[k_];
    int *g_k_r_1 = new int[k_];
    int *last_line = new int[k_];
    // Initialize final_matrix_l
    for (int i = 0; i < p_ * k_; i++)
    {
        final_matrix_l[i] = 0;
    }

    // 计算分子：prod (b_{r+1} - b_s) for s=1 to r
    for (int s = 0; s < (r_ - 1); s++)
    {
        int b_s = x[r_ - 1] ^ x[s];
        numerator = galois_single_multiply(numerator, b_s, 8);
    }
    //cout<< "numerator = " << numerator << endl;

    // 计算分母：prod (a_i - b_t) for t=1 to r+1
    for (int i = 0; i < k_; i++)
    {
        // int b_t_g = y[i];
        for (int j = 0; j < r_; j++)
        {
            int b_t_a = y[i] ^ x[j];
            denominator = galois_single_multiply(denominator, b_t_a, 8);
        }
        alpha[i] = galois_single_divide(numerator, denominator, 8);
        //cout<<"alpha ["<<i<<"] = "<<alpha[i]<<endl;
        denominator = 1;
    }
    // 计算g_k+r+1
    for (int i = 0; i < k_; i++)
    {
        int x_y = x[r_ - 1] ^ y[i];
        // cout<<"x_y = "<<x_y<<endl;
        g_k_r_1[i] = galois_single_divide(1, x_y, 8);
        //cout<<"g["<<i<<"] = "<<g_k_r_1[i]<<endl;
    }

    // 计算局部校验位矩阵
    // int local_group_size = (k_ + r_) / p_;
    // int remainder = (k_ + r_) % p_;
    // cout<<"local_group_size = "<<local_group_size<<"remainder = "<<remainder<<endl;
    // p整除k+r

    for (int i = 0; i < p_; i++)
    {
        for (int j = 0; j < k_; j++)
        {
            if (remainder_ == 0)
            {
                if (i < (p_ - 1))
                {

                    if (j >= i * group_size_ && j < (i + 1) * group_size_)
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
                else if(i == (p_-1))
                {
                    // 局部校验位生成矩阵最后一行
                    int temp = k_ + r_ - group_size_;

                    if (j >= temp && j < k_)
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
            else
            {
                int small_group_num = p_ - remainder_;
                if (i >= 0 && i < small_group_num)
                {

                    if (j >= i * (group_size_ - 1) && j < (i + 1) * (group_size_ - 1))
                    {
                        final_matrix_l[i * k_ + j] = alpha[j];
                    }
                    else
                    {
                        final_matrix_l[i * k_ + j] = 0;
                    }
                }
                else if (i >= small_group_num && i < (p_ - 1))
                {
                    int small_group = small_group_num * (group_size_ - 1);

                    if (j >= (small_group + (i - small_group_num) * group_size_) && j < (small_group + (i + 1 - small_group_num) * group_size_))
                    {
                        final_matrix_l[i * k_ + j] = alpha[j];
                    }
                    else
                    {
                        final_matrix_l[i * k_ + j] = 0;
                    }
                }
                else if (i == (p_ - 1))
                {
                    // 局部校验位生成矩阵最后一行
                    int temp = k_ + r_-1 - group_size_;

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
            //cout<<" final_matrix_l["<<i * k_ + j<<"] = "<<final_matrix_l[i * k_ + j]<<endl;
        }
    }
    int * check = new int[k_]();
    for (int i = 0; i < k_; i++)
    {
        
        for (int j = 0; j < p_; j++)
        {
            check [i] = check [i] ^final_matrix_l[j*k_+i];
        }
        //cout<<"check ["<<i<<"] = "<< check [i]<<endl;
    }
    
}

// void UNBALANCED_LRC::unbalanced_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs, int blocksize)
// {
//     // Generate the new LRC matrix
//     int *matrix_g = new int[k * r]; // 全局矩阵：k*r
//     int *matrix_l = new int[k * p]; // 局部矩阵：k*p
//     unbalanced_generate_globalmatrix(k, r, matrix_g);
//     unbalanced_generate_localmatrix(k, r, p, matrix_l);
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

int UNBALANCED_LRC::single_decode_node_need(int fail_one, int *&node_id)
{
    int return_group_size;
    // top层出错(k+r-1 - k+r+p-1)
    if (fail_one >= (k_ + r_ - 1) && fail_one < (k_ + r_ + p_))
    {
        return_group_size = p_;
        node_id =new int [p_];
        int g= k_ + r_ - 1;
        for (int i = (k_ + r_ - 1); i < (k_ + r_ + p_) ; i++)
        {
            if (g != fail_one)
            {
                node_id[i - (k_ + r_ - 1)] = g;
            }
            else if( g == fail_one)
            {
                node_id[i - (k_ + r_ - 1)] = g + 1;
                g++;
            }
            g++;
            //cout << "node_id[" << i - (k_ + r_ - 1) << "] = " << node_id[i - (k_ + r_ - 1)] << endl;
        }
    } // lower层出错（0-k+r-2)
    else
    {
        int fail_list[] = {fail_one};
        int start, end, parity_id;
        int *group_id;
        get_group_id(1, fail_list, group_id);
        parity_id = group_id[0] + k_ + r_;
        if (remainder_ == 0)
        {
            node_id = new int[group_size_];
            start = group_id[0] * group_size_;
            end = start + group_size_;
            return_group_size = group_size_;
        }
        else
        {
            if (group_id[0] < (p_ - remainder_))
            {
                node_id = new int[group_size_ - 1];
                start = group_id[0] * (group_size_ - 1);
                end = start + group_size_ - 1;
                return_group_size = group_size_ - 1;
            }
            else
            {
                node_id = new int[group_size_];
                start = (p_ - remainder_) * (group_size_ - 1) + (group_id[0] - (p_ - remainder_)) * group_size_;
                end = start + group_size_;
                return_group_size = group_size_;
            }
        }
        //cout<<"start : "<<start <<" end : "<<end<<endl;
        int g = start;
        for (int l = start; l < end && g < return_group_size-1; l++)
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
            //cout << "node_id[" << l - start << "] = " << node_id[l - start] << endl;
        }
        node_id[return_group_size - 1] = parity_id;
    }
    //cout<<"return : "<<return_group_size<<endl;
    return return_group_size;
}

bool UNBALANCED_LRC::single_decode(int fail_one, char **data_ptrs, char **code_ptr)
{
    //cout<<"fail : "<<fail_one<<endl;
    int decode = -1;
    int *final_matrix;

    int *group_id;
    int *erasures = new int[2];
    erasures[1] = -1;
    get_group_id(1, &fail_one, group_id);
    if (fail_one >= (k_ + r_ - 1) && fail_one < (k_ + r_ + p_))
    {
        final_matrix = new int[p_ * (p_ + 1)];
        for (int i = 0; i < (p_ + 1); i++)
        {
            if (i < p_)
            {
                for (int j = 0; j < p_; j++)
                {
                    if (i == j)
                    {
                        final_matrix[i * p_ + j] = 1;
                    }
                    else
                    {
                        final_matrix[i * p_ + j] = 0;
                    }
                }
            }
            else if (i == p_)
            {
                for (int j = 0; j < p_; j++)
                {
                    final_matrix[i * p_ + j] = 1;
                }
            }
        }
        if (fail_one != (k_ + r_ - 1))
        {
            erasures[0] = fail_one - (k_ + r_);
        }
        else
        {
            erasures[0] = p_;
        }
        decode = jerasure_matrix_decode(p_, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);
    }
    else
    {
        int group_real = group_size_;
        if (remainder_ != 0 && group_id[0] < (p_ - remainder_))
        {
            group_real = group_size_ - 1;
        }
        final_matrix = new int[group_real * (group_real + 1)];
        for (int i = 0; i < group_real; i++)
        {
            for (int j = 0; j < group_real; j++)
            {

                if (i == j)
                {
                    final_matrix[i * group_real + j] = 1;
                }
                else
                {
                    final_matrix[i * group_real + j] = 0;
                }
            }
        }
        // 非最后一个组出错
        if (fail_one < (k_ + r_ - 1 - group_size_))
        {
            if (remainder_ == 0)
            {
                erasures[0] = fail_one % group_real;
            }
            else
            {
                if (group_id[0] < (p_ - remainder_))
                {
                    erasures[0] = fail_one % group_real;
                }
                else
                {
                    erasures[0] = (fail_one - (p_ - remainder_) * (group_size_ - 1)) % group_size_;
                }
            }
            int *local_parity;
            generate_localmatrix(local_parity);
            int start = 0;
            for (int i = 0; i < k_; i++)
            {

                if (local_parity[group_id[0] * k_ + i] != 0)
                {
                    start = i;
                    break;
                }
            }
            for (int i = start; i < start + group_real; i++)
            {
                final_matrix[group_real * group_real + (i - start)] = local_parity[group_id[0] * k_ + i];
            }
            //cout<<" erasure :"<<erasures[0]<<"group_real : "<<group_real<<endl;
            decode = jerasure_matrix_decode(group_real, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);
        }
        else
        {
            erasures[0] = fail_one - (k_ + r_ - 1 - group_size_);
            int numerator = 1;
            for (int s = 0; s < (r_ - 1); s++)
            {
                int b_s = (r_ - 1) ^ s;
                numerator = galois_single_multiply(numerator, b_s, 8);
            }
            // 最后一组数据块分母：
            int denominator = 1;
            int *alpha = new int[k_ - (k_ + r_ - 1 - group_size_)];
            for (int i = (k_ + r_ - 1 - group_size_); i < k_; i++)
            {
                // int b_t_g = y[i];
                for (int j = 0; j < r_; j++)
                {
                    int b_t_a = (i + r_) ^ j;
                    denominator = galois_single_multiply(denominator, b_t_a, 8);
                }
                alpha[i - (k_ + r_ - 1 - group_size_)] = galois_single_divide(numerator, denominator, 8);
                // cout<<"alpha ["<<i<<"] = "<<alpha[i]<<endl;
                denominator = 1;
            }
            int beta_denominator = 1;
            int *beta = new int[r_ - 1];
            for (int i = 0; i < (r_ - 1); i++)
            {
                for (int j = 0; j < r_; j++)
                {
                    if (i != j)
                    {
                        int temp_ = i ^ j;
                        beta_denominator = galois_single_multiply(beta_denominator, temp_, 8);
                    }
                }
                beta[i] = galois_single_divide(numerator, beta_denominator, 8);
                beta_denominator = 1;
            }
            for (int i = 0; i < group_size_; i++)
            {
                if (i >= (group_size_ - (r_ - 1)) && i < group_size_)
                {
                    final_matrix[group_real * group_real + i] = beta[i - (group_size_ - (r_ - 1))];
                }
                else
                {
                    final_matrix[group_real * group_real + i] = alpha[i];
                }
            }
            decode = jerasure_matrix_decode(group_size_, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);
        }
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

bool UNBALANCED_LRC::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
{
    int *group_id;
    get_group_id(fail_num, fail_list, group_id);
    int top_fail = 0;
    int group_real;
    int *top_level = new int[p_];
    // 查看top层的fail并且重新赋值group_id
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] >= (k_ + r_ - 1) && fail_list[i] < (k_ + r_ + p_))
        {
            // if (fail_list[i] == k_ + r_ - 1)
            // {
            //     group_id[i] = p_ - 1;
            // }
            // else
            // {
            //     group_id[i] = group_id[i] - p_;
            // }
            top_level[top_fail] = fail_list[i];
            top_fail++;
        }
    }
    // 优先处理top层的fail
    if (top_fail == 1)
    {
        char **data_p = new char *[p_];
        for (int i = 0; i < p_; i++)
        {
            data_p[i] = local_ptr[i];
        }
        char **code_p = new char *[1];
        code_p[0] = global_code_ptr[r_ - 1];
        bool top = single_decode(top_level[0], data_p, code_p);
        if (top == false)
        {
            //cout << "cant repair : " << top_level[0] << endl;
            return false;
        }
        // 将修复后的数据块放回到local_ptr和global_code_ptr中
        for (int i = 0; i < p_; i++)
        {
            local_ptr[i] = data_p[i];
        }
        global_code_ptr[r_ - 1] = code_p[0];
        // 去掉top_level[0]在fail_list中的位置
        for (int i = 0; i < fail_num; i++)
        {
            if (fail_list[i] == top_level[0])
            {
                for (int j = i; j < fail_num - 1; j++)
                {
                    fail_list[j] = fail_list[j + 1];
                }
                fail_num--;
                break;
            }
        }
    }
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] > (k_ + r_ - 1) && fail_list[i] < (k_ + r_ + p_))
        {
            
            group_id[i] = group_id[i] - p_;
            
            
        }
    }
    // 处理剩余的fail_list

    // 先处理局部组中只有一个fail的情况
    int *fail_list_group_id = new int[p_]();
    bool need_second_decode = false;
    int *top_fail_id = new int[2]();
    for (int i = 0; i < p_; i++)
    {
        int *temp = new int[2]();
        for (int j = 0; j < fail_num; j++)
        {
            if (group_id[j] == i)
            {
                temp[fail_list_group_id[i]] = j;
                fail_list_group_id[i]++;
            }
        }
        if (fail_list_group_id[i] == 1)
        {
            int start, end;
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
            //cout<<"start : "<<start<<" end : "<<end<<" group :"<<group_real<<"temp : "<<temp[0]<<endl;
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
                        data_ptr_[g - (k_ + r_ -1- group_size_)] = data_ptrs[g];
                    }
                    else if (g >= k_ && g < (k_ + r_ - 1))
                    {
                        data_ptr_[g - (k_ + r_-1 - group_size_)] = global_code_ptr[g - k_];
                    }
                }
            }
            char **local_r = new char*[1];
            local_r[0] = local_ptr[i];
            bool repair = single_decode(fail_list[temp[0]], data_ptr_, local_r);
            if (repair == false)
            {
                cout << "cant repair : " << fail_list[temp[0]] << endl;
                return false;
            }
            else
            {
                cout << "repair success : " << fail_list[temp[0]] << endl;
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
                local_ptr[i] = local_r[0];
            }
        }
        else if (fail_list_group_id[i] == 2)
        {
            need_second_decode = true;
            top_fail_id[0] = temp[0];
            top_fail_id[1] = temp[1];
        }
        delete[] temp;
    }
    // 处理局部组中有两个fail的情况->top层fail：1，局部组fail：1，优先处理top层fail
    if (need_second_decode)
    {
        char **data_p_sec = new char *[p_];
        for (int i = 0; i < p_; i++)
        {
            data_p_sec[i] = local_ptr[i];
        }
        char **code_p_sec = new char *[1];
        code_p_sec[0] = global_code_ptr[r_ - 1];
        bool top = single_decode(fail_list[top_fail_id[1]], data_p_sec, code_p_sec);
        if (top == false)
        {
            cout << "cant repair : " << top_level[top_fail_id[1]] << endl;
            return false;
        }
        // 将修复后的数据块放回到local_ptr和global_code_ptr中
        for (int i = 0; i < p_; i++)
        {
            local_ptr[i] = data_p_sec[i];
        }
        global_code_ptr[r_ - 1] = code_p_sec[0];
        // 局部修复
        int start_sec, end_sec;
        if (remainder_ == 0)
        {
            start_sec = group_id[top_fail_id[0]] * group_size_;
            end_sec = start_sec + group_size_;
            group_real = group_size_;
        }
        else
        {
            if (group_id[top_fail_id[0]] < (p_ - remainder_))
            {
                start_sec = group_id[top_fail_id[0]] * (group_size_ - 1);
                end_sec = start_sec + (group_size_ - 1);
                group_real = group_size_ - 1;
            }
            else
            {
                int small_part = (p_ - remainder_) * (group_size_ - 1);
                start_sec = small_part + (group_id[top_fail_id[0]] - (p_ - remainder_)) * group_size_;
                end_sec = start_sec + group_size_;
                group_real = group_size_;
            }
        }
        char **data_ptr_sec = new char *[group_real];
        if (group_id[top_fail_id[0]] < (p_ - 1))
        {
            for (int j = start_sec; j < end_sec; j++)
            {
                data_ptr_sec[j - start_sec] = data_ptrs[j];
                // cout << "data_ptr_[" << j - start << "] = " << strlen(data_ptr_[j - start]) << endl;
            }
        }
        else if (group_id[top_fail_id[0]] == (p_ - 1))
        {
            for (int j = (k_ + r_ - 1 - group_size_); j < (k_ + r_ - 1); j++)
            {
                if (j < k_)
                {
                    data_ptr_sec[j - (k_ + r_ - group_size_)] = data_ptrs[j];
                }
                else if (j >= k_ && j < (k_ + r_ - 1))
                {
                    data_ptr_sec[j - (k_ + r_ - group_size_)] = global_code_ptr[j - k_];
                }
            }
        }
        bool repair = single_decode(fail_list[top_fail_id[0]], data_ptr_sec, local_ptr + group_id[top_fail_id[0]]);
        if (repair == false)
        {
            cout << "cant repair : " << fail_list[top_fail_id[0]] << endl;
            return false;
        }
        else
        {
            cout << "repair success : " << fail_list[top_fail_id[0]] << endl;
            // 将修复后的数据块放回到data_ptrs中
            if (group_id[top_fail_id[0]] < (p_ - 1))
            {
                for (int j = start_sec; j < end_sec; j++)
                {
                    data_ptrs[j] = data_ptr_sec[j - start_sec];
                    // cout << "data_ptrs[" << j << "] = " << strlen(data_ptrs[j]) << endl;
                }
            }
            else if (group_id[top_fail_id[0]] == (p_ - 1))
            {
                for (int j = (k_ + r_ - 1 - group_size_); j < (k_ + r_ - 1); j++)
                {
                    if (j < k_)
                    {
                        data_ptrs[j] = data_ptr_sec[j - (k_ + r_ - 1 - group_size_)];
                    }
                    else if (j >= k_ && j < (k_ + r_ - 1))
                    {
                        global_code_ptr[j - k_] = data_ptr_sec[j - (k_ + r_ - 1 - group_size_)];
                    }
                }
            }
        }
    }
    else
    {
        return true;
    }
}

int UNBALANCED_LRC::check_decode(int fail_num, int *fail_list_group_id)
{
    int* group_id = new int[fail_num];
    for (int i = 0; i < fail_num; i++)
    {
        group_id[i] = fail_list_group_id[i];
    }
    
    for (int i = 0; i < fail_num; i++)
    {
        if (group_id[i] == 2 * p_)
        {
            group_id[i] = p_ - 1;
        }
        else if (group_id[i] >= p_ && group_id[i] < 2 * p_)
        {
            group_id[i] = group_id[i] - p_;
        }
    }

    int flag = -1;
    if (fail_num < (r_ + 1))
    {
        flag = 0;
        return flag;
    }
    else if (fail_num == (r_ + 1))
    {
        flag = 1;
        return flag;
    }
    else if (fail_num > (r_ + 1) && fail_num <= (r_ + p_))
    {
        flag = fail_num - r_;
        while (fail_num > (r_ + 1))
        {
            if (group_id[fail_num - 1] != group_id[fail_num - 2])
            {
                fail_num--;
            }
            else if (group_id[fail_num - 1] == group_id[fail_num - 2])
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

bool UNBALANCED_LRC::if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id)
{
    int count_local = 0;
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] >= (k_ + r_))
        {
            count_local++;
        }
    }
    if (count_local == fail_num)
    {
        cout << "no need mds" << endl;
        return false;
    }
    else
    {
        int *group_fail_num = new int[p_]();
        int *top_level = new int[p_]();
        int count_top_level = 0;
        // 一个组中有大于一个错,MDS NEED
        for (int j = 0; j < 2 * p_; j++)
        {
            if (j < p_)
            {
                for (int i = 0; i < fail_num; i++)
                {
                    if (fail_list_group_id[i] == j)
                    {
                        // cout << "fail_list_group_id[" << i << "] = " << fail_list_group_id[i] << endl;
                        group_fail_num[j]++;
                    }
                }
            }
            else if (j >= p_ && j < 2 * p_)
            {
                for (int i = 0; i < fail_num; i++)
                {
                    if (fail_list_group_id[i] == j && group_fail_num[j - p_] != 0)
                    {
                        // cout << "fail_list_group_id[" << i << "] = " << fail_list_group_id[i] << endl;
                        top_level[i]++;
                        count_top_level++;
                    }
                }
            }
            // cout << "group_fail_num[" << j << "] = " << group_fail_num[j] << endl;
            if (group_fail_num[j] > 1 || count_top_level > 1)
            {
                cout << "need mds" << endl;
                cout << "group_fail_num[" << j << "] = " << group_fail_num[j] << endl;
                cout << "count_top_level = " << count_top_level << endl;
                return true;
                break;
            }
        }
        // 局部组修复IO大于mds修复IO,MDS NEED
        int single_repair_cost = get_repair_num(fail_num, fail_list_group_id);
        //cout << "single_repair_cost = " << single_repair_cost << endl;
        if (single_repair_cost >= k_)
        {
            // cout << "single_repair_cost = " << single_repair_cost << endl;
            cout << "need mds" << endl;
            return true;
        }

        cout << "no need mds" << endl;
        return false;
    }
}

int UNBALANCED_LRC::get_repair_num(int fail_num, int *fail_list_group_id)
{
    int small_group_num = p_ - remainder_;
    int top_ = p_ + 1;
    int repair_num = 0;
    
    for (int i = 0; i < fail_num; i++)
    {
        //cout << "fail_list_group_id[" << i << "] = " << fail_list_group_id[i] << endl;
        if (fail_list_group_id[i] >= p_)
        {
            top_--;
        }
    }
    //cout << "top_ = " << top_ << endl;
    if (top_ == p_ + 1)
    {
        if (remainder_ == 0)
        {
            repair_num = group_size_ * fail_num;
        }
        else
        {
            int count_1 = 0;
            int count_2 = 0;
            for (int i = 0; i < fail_num; i++)
            {
                if (fail_list_group_id[i] < small_group_num)
                {
                    count_1++;
                }
                else
                {
                    count_2++;
                }
            }
            repair_num = count_1 * (group_size_ - 1) + count_2 * group_size_;
        }
    }
    else
    {
        if (remainder_ == 0)
        {
            
           
                repair_num = (group_size_ - 1) * (fail_num - (p_ + 1 - top_)) + top_;
            
        }
        else
        {
            int count_1 = 0;
            int count_2 = 0;
            for (int i = 0; i < fail_num; i++)
            {
                if (fail_list_group_id[i] >= 0 && fail_list_group_id[i] < small_group_num)
                {
                    count_1++;
                }
                else if (fail_list_group_id[i] >= small_group_num && fail_list_group_id[i] < p_)
                {
                    count_2++;
                }
            }
            
                if (count_2 == 0)
                {
                    repair_num = (fail_num - (p_ + 1 - top_)) * (group_size_ - 1-1) + top_;
                }
                else
                {
                    repair_num = count_1 * (group_size_ - 1-1) + (count_2 ) * (group_size_ - 1) + top_;
                }
            
        }
    }
    //cout << "repair_num = " << repair_num << endl;
    return repair_num;
}

void UNBALANCED_LRC::get_group_id(int fail_num, int *fail_list, int *&group_id)
{

    group_id = new int[fail_num];
    // cout<< "group_size = " << group_size << endl;
    // cout<< "remainder = " << remainder << endl;
    if (remainder_ == 0)
    {
        for (int i = 0; i < fail_num; i++)
        {
            if (fail_list[i] >= 0 && fail_list[i] < (k_ + r_ - 1))
            {
                group_id[i] = fail_list[i] / group_size_;
                // if ((group_id[i] != 0) && (fail_list[i] % group_size_ != 0))
                //     group_id[i]--;
            }
            else if (fail_list[i] == (k_ + r_ - 1))
            {
                group_id[i] = 2 * p_;
            }
            else if (fail_list[i] >= (k_ + r_) && fail_list[i] < (k_ + r_ + p_))
            {

                group_id[i] = fail_list[i] - (k_ + r_) + p_;
            }
        }
    }
    else
    {
        int small_part = (p_ - remainder_) * (group_size_ - 1);
        for (int i = 0; i < fail_num; i++)
        {
            if (fail_list[1] >= 0 && fail_list[i] < small_part)
            {
                group_id[i] = fail_list[i] / (group_size_ - 1);
                // if ((group_id[i] != 0) && (fail_list[i] % (group_size_ - 1) != 0))
                //     group_id[i]--;
            }
            else if (fail_list[i] >= small_part && fail_list[i] < (k_ + r_ - 1))
            {
                int temp = (fail_list[i] - small_part) / group_size_;
                // if ((temp != 0) && ((fail_list[i] - small_part) % group_size_ != 0))
                //     temp--;
                group_id[i] = temp + p_ - remainder_;
            }
            else if (fail_list[i] == (k_ + r_ - 1))
            {
                group_id[i] = 2 * p_;
            }
            else if (fail_list[i] >= (k_ + r_) && fail_list[i] < (k_ + r_ + p_))
            {
                group_id[i] = fail_list[i] - (k_ + r_) + p_;
            }
        }
    }

    // for (int i = 0; i < fail_num; i++)
    // {
    //     cout << "group_id[" << i << "] = " << group_id[i] << endl;
    // }
}
