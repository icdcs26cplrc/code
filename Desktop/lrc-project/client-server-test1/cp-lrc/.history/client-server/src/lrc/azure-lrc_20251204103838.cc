#include "../../include/lrc/azure-lrc.hh"
using namespace ClientServer;

AZURE_LRC::AZURE_LRC(int k, int r, int p, size_t blocksize)
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

void AZURE_LRC::generate_globalmatrix(int *&final_matrix_g)
{
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void AZURE_LRC::generate_localmatrix(int *&final_matrix_l)
{
    for (int i = 0; i < p_; i++)
    {
        for (int j = 0; j < k_; j++)
        {
            if (j >= i * group_size_ && j < (i + 1) * group_size_)
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


int AZURE_LRC::single_decode_node_need(int fail_one, int *&node_id ,int group_id__)
{
    int fail_list[] = {fail_one};
    if (fail_one >= (k_) && fail_one < (k_ + r_))
    {
        // need mds
        find_k_blocks(1, fail_list, node_id);
        return k_;
    }
    else
    {
        int *group_id;
        int start, end, parity_id, group_real;
        get_group_id(1, fail_list, group_id);
        parity_id = group_id[0] + k_ + r_;
        group_real = group_size_;
        if (remainder_ == 0)
        {
            start = group_id[0] * group_real;
            end = start + group_real;
        }
        else
        {
            if (group_id[0] < (p_ - remainder_))
            {
                group_real--;
                start = group_id[0] * group_real;
                end = start + group_real;
            }
            else
            {
                start = (group_id[0] - (p_ - remainder_)) * group_real + (p_ - remainder_) * (group_real - 1);
                end = start + group_real;
            }
        }

        node_id = new int[group_real];
        // 找到解码所需要的node_id
        if (fail_one < end)
        {
            int group_index = fail_one % group_size_;
            int j = start;
            for (int i = start; i < end - 1; i++)
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
                cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
            }
        }

        return group_size_;
    }
}

bool AZURE_LRC::single_decode(int fail_one, char **data_ptrs, char **code_ptr,size_t decode_size )
{
    int decode = -1;
    // int *group_id;
    int fail_list[] = {fail_one};
    // get_group_id( 1, fail_list, group_id);
    // std::cout << "group_id = " << group_id << endl;

    // cout << "remainder = " << remainder << endl;
    int *erasures = new int[2];
    erasures[0] = fail_one % group_size_;
    if (fail_one >= (k_ + r_))
    {
        erasures[0] = group_size_;
    }
    //cout<<"erase :"<<erasures[0]<<endl;
    erasures[1] = -1;
    if (fail_one >= k_ && fail_one < (k_ + r_))
    {
        bool mds_repair = mds_decode(0, 1, fail_list, data_ptrs, code_ptr);
        return mds_repair;
    }
    else
    {
        //int *final_matrix = new int[(group_size_ + 1) * group_size_];
        int *final_matrix = new int[(1) * group_size_];
        // 单位矩阵
        //for (int i = 0; i < (group_size_ + 1); i++)
        //{
            for (int j = 0; j < group_size_; j++)
            {
                     //final_matrix[i * group_size_ + j] = 1;
                     final_matrix[j] = 1;
                 }
                //cout << "final_matrix[" << i << "][" << j << "] = " << final_matrix[i * group_size_ + j] << endl;
            //}
        if (decode_size == 0)
        {
            decode_size = blocksize_;
        }
        
        decode = jerasure_matrix_decode(group_size_, 1, 8, final_matrix, 1, erasures, data_ptrs, code_ptr, decode_size);
    }

    if (decode == 0)
    {
        cout << "decode success" << endl;
        return true;
    }
    else
    {
        cout << "decode result : " << decode << endl;
        cout << "decode fail" << endl;
        return false;
    }
}


bool AZURE_LRC::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr, size_t decode_size)
{

    bool repair[fail_num];
    int *group_id;
    get_group_id(fail_num, fail_list, group_id);
    for (int i = 0; i < fail_num; i++)
    {
        repair[i] = false;
        int start, end, group_real;
        if (remainder_ == 0)
        {
            start = group_id[i] * group_size_;
            end = start + group_size_;
            group_real = group_size_;
        }
       
        char **data_ptr_ = new char *[group_real];
        for (int j = start; j < end; j++)
        {
            data_ptr_[j - start] = data_ptrs[j];
            //cout << "decode : data_ptr_[" << j - start << "] = " << strlen(data_ptr_[j - start]) << endl;
        }
        char **code_ptr_ = new char *[1];
        code_ptr_[0] = local_ptr[group_id[i]];
        //cout<<"decode : size of local_ptr : "<<strlen(code_ptr_[0])<<endl;
        repair[i] = single_decode(fail_list[i], data_ptr_, code_ptr_,decode_size);
        if (repair[i] == false)
        {
            cout << "cant repair : " << fail_list[i] << endl;
            return false;
        }
        else
        {
            cout << "repair success : " << fail_list[i] << endl;
            // 将修复后的数据块放回到data_ptrs中

            for (int j = start; j < end; j++)
            {
                data_ptrs[j] = data_ptr_[j - start];
                // cout << "data_ptrs[" << j << "] = " << strlen(data_ptrs[j]) << endl;
            }
            local_ptr[group_id[i]] = code_ptr_[0];
            // 将修复后的数据块放回到local_ptr中
            // local_ptr[i] = local_ptr[i];
        }
    }
    return true;
}

bool AZURE_LRC::if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id)
{
    // 如果fail均为局部校验位，则不需要mds
    int count = 0;
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] >= (k_ + r_))
        {
            count++;
        }
    }
    if (count == fail_num)
    {
        cout << "no need mds" << endl;
        return false;
    }
    else
    {
        int *group_fail_num = new int[p_]();
        // 一个组中有大于一个错,MDS NEED
        for (int j = 0; j < p_; j++)
        {
            for (int i = 0; i < fail_num; i++)
            {
                if (fail_list_group_id[i] == j)
                {
                    group_fail_num[j]++;
                }
                // 全局校验位出错
                else if (fail_list[i] >= k_ && fail_list[i] < (k_ + r_))
                {
                    cout << "need mds" << endl;
                    return true;
                    break;
                }
            }
            // cout << "group_fail_num[" << j << "] = " << group_fail_num[j] << endl;
            if (group_fail_num[j] > 1)
            {
                cout << "need mds" << endl;
                return true;
                break;
            }
        }
        // 局部组修复IO大于mds修复IO,MDS NEED
        int single_repair_cost = get_repair_num(fail_num, fail_list_group_id);
        if (single_repair_cost >= k_)
        {
            cout << "need mds" << endl;
            return true;
        }

        cout << "no need mds" << endl;
        return false;
    }
}



