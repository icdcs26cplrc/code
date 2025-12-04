#include "../include/lrc/azure-lrc.hh"
using namespace ClientServer;

AZURE_LRC::AZURE_LRC(int k, int r, int p, size_t blocksize)
{
    k_ = k;
    r_ = r;
    p_ = p;
    blocksize_ = blocksize;
    group_size_ = k / p;
    remainder_ = k % p;
    if (remainder_ !=0)
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

// void AZURE_LRC::encode( char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs)
// {
//     // Generate the new LRC matrix
//     int *azure_matrix_g = new int[k * r]; // 全局矩阵：k*r
//     int *azure_matrix_l = new int[k * p]; // 局部矩阵：k*p
//     generate_globalmatrix( azure_matrix_g);
//     generate_localmatrix( azure_matrix_l);
//     // for (int  i = 0; i < p; i++)
//     // {
//     //     for (int j = 0; j < k; j++)
//     //     {
//     //         cout<< "local_matrix[" << i << "][" << j << "] = " << new_matrix_l[i * k + j] << endl;
//     //     }

//     // }

//     if (azure_matrix_g == nullptr || azure_matrix_l == nullptr)
//     {
//         cerr << "Error: Matrix generation failed." << endl;
//         return;
//     }

//     jerasure_matrix_encode(k, r, 8, azure_matrix_g, data_ptrs, global_coding_ptrs, blocksize);
//     jerasure_matrix_encode(k, p, 8, azure_matrix_l, data_ptrs, local_coding_ptrs, blocksize);
// }

int AZURE_LRC::single_decode_node_need(int fail_one, int *&node_id)
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
        int start,end,parity_id,group_real;
        get_group_id(1, fail_list, group_id);
        parity_id = group_id[0] + k_ + r_;
        group_real = group_size_;
        if (remainder_ == 0)
        {
            start = group_id[0] * group_real;
            end = start + group_real;
        }else {
            if (group_id[0] < (p_ - remainder_))
            {
                group_real --;
                start = group_id[0] * group_real;
                end = start + group_real;
            }else 
            {
                start = (group_id[0] - (p_-remainder_)) * group_real + (p_-remainder_)*(group_real-1);
                end = start + group_real;
            }
            
            
        }
        
        node_id = new int[group_real];
        // 找到解码所需要的node_id
        
        // cout << "start = " << start << " end = " << end << endl;
        // cout << "group_id = " << group_id << " group_index = " << group_index << endl;
        // cout << "fail_one = " << fail_one << endl;
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

bool AZURE_LRC::single_decode(int fail_one, char **data_ptrs, char **code_ptr)
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

    erasures[1] = -1;
    if (fail_one >= k_ && fail_one < (k_ + r_))
    {
        bool mds_repair = mds_decode(0, 1, fail_list, data_ptrs, code_ptr);
        return mds_repair;
    }
    else
    {
        int *final_matrix = new int[(group_size_ + 1) * group_size_];
        // 单位矩阵
        for (int i = 0; i < (group_size_ + 1); i++)
        {
            if (i >= 0 && i < group_size_)
            {
                for (int j = 0; j < group_size_; j++)
                {
                    if (j == i)
                    {
                        final_matrix[i * group_size_ + j] = 1;
                    }
                    else
                    {
                        final_matrix[i * group_size_ + j] = 0;
                    }
                    // cout << "final_matrix[" << i << "][" << j << "] = " << final_matrix[i * group_size + j] << endl;
                }
            }
            else if (i == group_size_)
            {
                for (int j = 0; j < group_size_; j++)
                {
                    final_matrix[i * group_size_ + j] = 1;
                }
            }
        }
        decode = jerasure_matrix_decode(group_size_, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);
    }

    if (decode == 0)
    {
        cout << "decode success" << endl;
        return true;
    }
    else
    {
        cout<<"decode result : "<<decode<<endl;
        cout << "decode fail" << endl;
        return false;
    }
}

// int AZURE_LRC::muti_decode_node_need(int fail_number, int *fail_list, int *&node_id)
// {
//     int *fail_group_id;
//     get_group_id(fail_number, fail_list, fail_group_id);
//     // 是否可以修复
//     int flag = check_decode(fail_number, fail_group_id);
//     // 是否需要mds修复
//     bool if_mds = if_need_mds(fail_number, fail_list, fail_group_id);
//     if (flag != -1)
//     {
//         if (if_mds == false)
//         {
//             int repair_num = get_repair_num(fail_number, fail_group_id);
//             node_id = new int[repair_num];
//             int node_need_num = 0;
//             int node_need = 0;
//             for (int i = 0; i < fail_number; i++)
//             {

//                 for (int i = 0; i < fail_number; i++)
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
//                 return node_need_num;
//             }
//         }
//         else
//         {
//             find_k_blocks(fail_number, fail_list, node_id);
//             return k;
//         }
//     }
//     else
//     {
//         cout << "can not repair" << endl;
//         return -1;
//     }
// }

// bool AZURE_LRC::muti_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
// {
//     int *fail_group_id;
//     get_group_id(fail_num, fail_list, fail_group_id);
//     // 是否可以修复
//     int flag = check_decode(fail_num, fail_group_id);
//     // 是否需要mds修复
//     bool if_mds = if_need_mds(fail_num, fail_list, fail_group_id);
//     if (flag != -1)
//     {
//         if (if_mds == true)
//         {
//             char **code_ptr = new char *[r + p];
//             for (int i = 0; i < (r + flag); i++)
//             {
//                 if (i >= 0 && i < r)
//                 {
//                     code_ptr[i] = global_code_ptr[i];
//                 }
//                 else if (i >= r && i < (r + flag))
//                 {
//                     code_ptr[i] = local_ptr[i - r];
//                 }
//             }

//             bool mds_repair = mds_decode(flag, fail_num, fail_list, data_ptrs, code_ptr);
//             return mds_repair;
//         }
//         else
//         {
//             bool muti = muti_single_decode(fail_num, fail_list, data_ptrs, global_code_ptr, local_ptr);
//             return muti;
//         }
//     }
//     else
//     {
//         cout << "cant repair" << endl;
//         return false;
//     }
// }

bool AZURE_LRC::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
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
        // else
        // {
        //     if (group_id[i] < (p_ - remainder_))
        //     {
        //         start = group_id[i] * (group_size_ - 1);
        //         end = start + (group_size_ - 1);
        //         group_real = group_size_ - 1;
        //     }
        //     else
        //     {
        //         int small_part = (p_ - remainder_) * (group_size_ - 1);
        //         start = small_part + (group_id[i] - (p_ - remainder_)) * group_size_;
        //         end = start + group_size_;
        //         group_real = group_size_;
        //     }
        // }
        char **data_ptr_ = new char *[group_real];
            for (int j = start; j < end; j++)
            {
                data_ptr_[j - start] = data_ptrs[j];
                cout << "data_ptr_[" << j - start << "] = " << strlen(data_ptr_[j - start]) << endl;
            }
       char **code_ptr_ = new char *[1];
       code_ptr_[0] = local_ptr[group_id[i]];
        repair[i] = single_decode(fail_list[i], data_ptr_, code_ptr_);
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
// bool AZURE_LRC::mds_decode( int locality, int fail_num, int *fail_list, char **data_ptrs, char **code_ptr )
// {
//     int *final_matrix = new int[(k + r + locality) * k];
//     for (int i = 0; i < k; i++)
//     {
//         for (int j = 0; j < k; j++)
//         {
//             if (i == j)
//             {
//                 final_matrix[i * k + j] = 1;
//             }
//             else
//             {
//                 final_matrix[i * k + j] = 0;
//             }
//         }
//     }
//     int *erasures = new int[fail_num + 1];
//     for (int i = 0; i <= fail_num; i++)
//     {
//         erasures[i] = fail_list[i];
//         // cout << "erasures[" << i << "] = " << erasures[i] << endl;
//         if (i == fail_num)
//         {
//             erasures[i] = -1;
//         }
//     }

//     int *global_parity = cauchy_original_coding_matrix(k, r, 8);
//     for (int i = 0; i < r; i++)
//     {
//         for (int j = 0; j < k; j++)
//         {
//             final_matrix[(k + i) * k + j] = global_parity[i * k + j];
//             // cout <<"global_parity[" << i << "][" << j << "] = " << global_parity[i * k + j] << endl;
//             // cout << "final_matrix[" << (k + i) * k + j << "] = " << final_matrix[(k + i) * k + j] << endl;
//         }
//     }
//     if (locality > 0)
//     {
//         int *locality_matrix = new int[locality * k];
//         generate_localmatrix( locality_matrix);
//         for (int i = 0; i < locality; i++)
//         {
//             for (int j = 0; j < k; j++)
//             {
//                 final_matrix[(k + r + i) * k + j] = locality_matrix[i * k + j];
//                 // cout << "locality_matrix[" << i << "][" << j << "] = " << locality_matrix[i * k + j] << endl;
//                 // cout << "final_matrix[" << (k + r + i) * k + j << "] = " << final_matrix[(k + r + i) * k + j] << endl;
//             }
//         }
//     }

//     // debug
//     //  for (int i = 0; i < (k+r); i++)
//     //  {
//     //      if (i< k)
//     //      {
//     //          cout << "data_ptrs["<<i<<"] = " << string(data_ptrs[i]).size() << endl;
//     //      }else{
//     //          cout << "code_ptr["<<i<<"] = " << string(code_ptr[i-k]).size() << endl;
//     //      }

//     // }

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

// int AZURE_LRC::get_repair_num( int fail_num, int *fail_list_group_id)
// {
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

// int AZURE_LRC::check_decode( int fail_num, int *fail_list_group_id)
// {
//     int flag = -1;
//     if (fail_num < (r + 1))
//     {
//         flag = 0;
//         return flag;
//     }
//     else if (fail_num == (r + 1))
//     {
//         flag = 1;
//         return flag;
//     }
//     else if (fail_num > (r + 1) && fail_num <= (r + p))
//     {
//         flag = fail_num - r;
//         while (fail_num > (r + 1))
//         {
//             if (fail_list_group_id[fail_num - 1] != fail_list_group_id[fail_num - 2])
//             {
//                 fail_num--;
//             }
//             else if (fail_list_group_id[fail_num - 1] == fail_list_group_id[fail_num - 2])
//             {
//                 cout << "too many failure happened in wrong places" << endl;
//                 flag = -1;
//                 return flag;
//                 break;
//             }
//         }
//         return flag;
//     }
//     else if (fail_num > (r + p))
//     {
//         cout << "too many failure" << endl;
//         flag = -1;
//         return flag;
//     }
// }

// void AZURE_LRC::find_k_blocks( int fail_num, int *fail_list, int *&node_id)
// {
//     node_id = new int[k];
//     int count = 0;
//     for (int i = 0; i < (k + r + p) && count < k; i++)
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

// void AZURE_LRC::get_group_id( int fail_num, int *fail_list, int *&group_id)
// {
//     group_id = new int[fail_num];
//     int group_size = k / p;
//     // cout<< "group_size = " << group_size << endl;
//     // cout<< "remainder = " << remainder << endl;
//     for (int i = 0; i < fail_num; i++)
//     {
//         if (fail_list[i] < (k + r))
//         {

//             group_id[i] = fail_list[i] / group_size;
//             if ((group_id != 0) && (fail_list[i] % group_size != 0))
//                 group_id[i]--;
//         }
//         else
//         {
//             group_id[i] = fail_list[i] - (k + r);
//         }
//     }
// }