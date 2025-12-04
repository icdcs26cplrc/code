#include "../../include/lrc/grade-lrc.hh"
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



int NewLRC::single_decode_node_need(int fail_one, int *&node_id,int group_id__ )
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
