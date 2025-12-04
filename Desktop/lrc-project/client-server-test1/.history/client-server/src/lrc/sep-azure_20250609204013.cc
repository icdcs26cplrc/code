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
    final_matrix_g = new int[r_ * k_];
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void SEP_AZURE::generate_localmatrix(int *&final_matrix_l)
{
    int *temp_matrix = new int[k_ * r_];
    final_matrix_l = new int[p_ * k_];
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

bool SEP_AZURE::single_decode(int fail_one, char **data_ptrs, char **code_ptr)
{
    // cout<<"fail : "<<fail_one<<endl;
    int decode = -1;
    int *final_matrix;

    int *group_id;
    int *erasures = new int[2];
    erasures[1] = -1;
    get_group_id(1, &fail_one, group_id);
    if (fail_one >= (k_ + r_ - 1) && fail_one < (k_ + r_ + p_))
    {
        final_matrix = new int[p_];
        for (int j = 0; j < p_; j++)
        {
            // final_matrix[i * p_ + j] = 1;
            final_matrix[j] = 1;
        }
        //     }
        // }
        if (fail_one != (k_ + r_ - 1))
        {
            erasures[0] = fail_one - (k_ + r_);
        }
        else
        {
            erasures[0] = p_;
        }
        decode = jerasure_matrix_decode(p_, 1, 8, final_matrix, 1, erasures, data_ptrs, code_ptr, blocksize_);
    }
    else
    {
        int group_real = group_size_;
        final_matrix = new int[group_real];
        erasures[0] = fail_one % group_size_;

        int *global_parity;
        generate_globalmatrix(global_parity);
        int start = group_id[0] * group_real;
        int end = start + group_real;
        for (int i = start; i < end; i++)
        {
            // final_matrix[group_real * group_real + (i - start)] = local_parity[group_id[0] * k_ + i];
            final_matrix[(i - start)] = global_parity[(r_ - 1) * k_ + i];
        }
        // cout<<" erasure :"<<erasures[0]<<"group_real : "<<group_real<<endl;
        decode = jerasure_matrix_decode(group_real, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);
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

bool SEP_AZURE::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
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
            cout << "cant repair : " << top_level[0] << endl;
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
    bool take_out_global = false;
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] > (k_ + r_ - 1) && fail_list[i] < (k_ + r_ + p_))
        {

            group_id[i] = group_id[i] - p_;
        }
        else if (fail_list[i] == (k_ + r_ - 1))
        {
            take_out_global = true;
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
            start = i * group_size_;
            end = start + group_size_;
            group_real = group_size_;
            // cout<<"start : "<<start<<" end : "<<end<<" group :"<<group_real<<"temp : "<<temp[0]<<endl;
            char **data_ptr_ = new char *[group_real];

            for (int j = start; j < end; j++)
            {
                data_ptr_[j - start] = data_ptrs[j];
                // cout << "data_ptr_[" << j - start << "] = " << strlen(data_ptr_[j - start]) << endl;
            }

            char **local_r = new char *[1];
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

                for (int j = start; j < end; j++)
                {
                    data_ptrs[j] = data_ptr_[j - start];
                    // cout << "data_ptrs[" << j << "] = " << strlen(data_ptrs[j]) << endl;
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
    // 处理局部组中有两个fail的情况->top层fail：1，局部组fail：1，优先处理top层fail。前提：k+r-1不在fail_list中
    if (need_second_decode && !take_out_global)
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

        start_sec = group_id[top_fail_id[0]] * group_size_;
        end_sec = start_sec + group_size_;
        group_real = group_size_;

        char **data_ptr_sec = new char *[group_real];

        for (int j = start_sec; j < end_sec; j++)
        {
            data_ptr_sec[j - start_sec] = data_ptrs[j];
            // cout << "data_ptr_[" << j - start << "] = " << strlen(data_ptr_[j - start]) << endl;
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

            for (int j = start_sec; j < end_sec; j++)
            {
                data_ptrs[j] = data_ptr_sec[j - start_sec];
                // cout << "data_ptrs[" << j << "] = " << strlen(data_ptrs[j]) << endl;
            }
        }
    }
    else if(take_out_global)
    {
        char **data_p_sec = new char *[p_];
        for (int i = 0; i < p_; i++)
        {
            data_p_sec[i] = local_ptr[i];
        }
        char **code_p_sec = new char *[1];
        code_p_sec[0] = global_code_ptr[r_ - 1];
        bool top = single_decode((k_+r_-1), data_p_sec, code_p_sec);
        if (top == false)
        {
            cout << "cant repair : " << (k_+r_-1) << endl;
            return false;
        }
        // 将修复后的数据块放回到local_ptr和global_code_ptr中
        for (int i = 0; i < p_; i++)
        {
            local_ptr[i] = data_p_sec[i];
        }
        global_code_ptr[r_ - 1] = code_p_sec[0];
    }
    return true;
}

void SEP_AZURE::get_group_id(int fail_num, int *fail_list, int *&group_id)
{

    group_id = new int[fail_num];
    // cout<< "group_size = " << group_size << endl;
    // cout<< "remainder = " << remainder << endl;

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
    // for (int i = 0; i < fail_num; i++)
    // {
    //     cout << "group_id[" << i << "] = " << group_id[i] << endl;
    // }
}

int SEP_AZURE::check_decode(int fail_num, int *fail_list_group_id)
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

int SEP_AZURE::get_repair_num(int fail_num, int *fail_list_group_id)
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
        
            repair_num = group_size_ * fail_num;
        
    }
    else
    {
          
        repair_num = (group_size_ - 1) * (fail_num - (p_ + 1 - top_)) + top_;
            
    }
    //cout << "repair_num = " << repair_num << endl;
    return repair_num;
}

bool SEP_AZURE::if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id)
{
    int count_local = 0;
    bool take_out_global = false;
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
        for (int j = 0; j <= 2 * p_; j++)
        {
            if (j < p_)
            {
                for (int i = 0; i < fail_num; i++)
                {
                    if (fail_list_group_id[i] == j)
                    {
                        // cout << "fail_list_group_id[" << i << "] = " << fail_list_group_id[i] << endl;
                        group_fail_num[j]++;
                    }else if (fail_list[i] >= k_ && fail_list[i] < (k_ + r_-1))
                    {
                        cout << "need mds" << endl;
                        return true;
                        break;
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
            else if (j == 2 * p_)
            {
                for (int i = 0; i < fail_num; i++)
                {
                    if (fail_list[i] == (k_+r_-1))
                    {
                        take_out_global = true;
                    }
                    
                }
                
            }
            
            // cout << "group_fail_num[" << j << "] = " << group_fail_num[j] << endl;
            if (group_fail_num[j] > 1 || count_top_level > 1 || (take_out_global && count_top_level > 0))
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


