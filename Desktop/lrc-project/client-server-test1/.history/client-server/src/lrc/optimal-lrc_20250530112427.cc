#include "../include/lrc/optimal-lrc.hh"
using namespace ClientServer;

OPTIMAL_LRC::OPTIMAL_LRC(int data, int global, int local, size_t BlockSize)
{
    k_ = data;
    r_ = global;
    p_ = local;
    blocksize_ = BlockSize;
    group_size_ = k_ / p_ + r_;
    remainder_ = 0;
    if (remainder_ !=0)
    {
        group_size_++;
    }
    if (k_ <= 0 || r_ <= 0 || p_ <= 0 || blocksize_ <= 0)
    {
        throw std::invalid_argument("Invalid parameters: values must be positive.");
    }
}

void OPTIMAL_LRC::generate_globalmatrix(int *&final_matrix_g)
{
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void OPTIMAL_LRC::generate_localmatrix(int *&final_matrix_l)
{
    int group_size = k_ / p_;
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
        for (int j = 0; j < k_; j++)
        {
            if (j >= i * group_size && j < (i + 1) * group_size)
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

int OPTIMAL_LRC::get_repair_num(int fail_num,int *fail_list_group_id)
{
    //optimal局部修复全剧校验位只用计算一次
    
    return (group_size_-r_) * (fail_num-1) + group_size_;
    
    
}
// void OPTIMAL_LRC::optimal_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs,char **local_coding_ptrs, int blocksize){
//     // Generate the new LRC matrix
//     int *matrix_g = new int[k * r]; // 全局矩阵：k*r
//     int *matrix_l = new int[k * p]; // 局部矩阵：k*p
//     optimal_generate_globalmatrix(k, r, matrix_g);
//     optimal_generate_localmatrix(k,r,  p, matrix_l);
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
//     jerasure_matrix_encode(k , p, 8, matrix_l, data_ptrs, local_coding_ptrs, blocksize);
// }

int OPTIMAL_LRC::single_decode_node_need(int fail_one, int *&node_id)
{
    int fail_list[] = {fail_one};
    int start, end, parity_id;
    int *group_id;
    get_group_id(1, fail_list, group_id);
    parity_id = group_id[0] + k_ + r_;
    node_id = new int[group_size_];
    // 找到解码所需要的node_id
    start = group_id[0] * (group_size_ - r_);
    end = start + group_size_ - r_;
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
            } // cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
            j++;
        }
        for (int i = 0; i < r_; i++)
        {
            node_id[group_size_ - r_ - 1 + i] = k_ + i;
        }

        node_id[group_size_ - 1] = parity_id;
    } // 全部校验位lose
    else if (fail_one >= k_ && fail_one < (k_ + r_))
    {
        for (int i = start; i < end; i++)
        {
            node_id[i - start] = i;
        }
        int j =k_;
        for (int i = k_; i <k_+ r_; i++)
        {
            if (j != fail_one)
            {
                node_id[group_size_ - r_ + i-k_] = j;
            }
            else
            {
                node_id[group_size_ - r_ + i-k_] = j + 1;
                j++;
            }
            j++;
        }
        node_id[group_size_ - 1] = parity_id;
    }
    else
    {
        for (int i = start; i < end; i++)
        {
            node_id[i - start] = i;
            // cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
        }
        for (int i = 0; i < r_; i++)
        {
            node_id[group_size_ - r_ + i] = k_ + i;
        }
    }
    return group_size_;
}

bool OPTIMAL_LRC::single_decode(int fail_one, char **data_ptrs, char **code_ptr)
{
    int decode = -1;
    int *erasures = new int[2];
    erasures[1] = -1;
    if (fail_one < k_)
    {
        erasures[0] = fail_one % (group_size_ - r_);
    }
    else if (fail_one >= k_ && fail_one < (k_ + r_))
    {
        erasures[0] = (fail_one - k_) % r_;
    }
    else
    {
        erasures[0] = group_size_;
    }
    int *final_matrix = new int[(group_size_ + 1) * group_size_];
    for (int i = 0; i < (group_size_ + 1); i++)
    {

        for (int j = 0; j < group_size_; j++)
        {
            if (i >= 0 && i < group_size_)
            {
                if (i == j)
                {
                    final_matrix[i * group_size_ + j] = 1;
                }
                else
                {
                    final_matrix[i * group_size_ + j] = 0;
                }
            }
            else if (i == group_size_)
            {
                final_matrix[i * group_size_ + j] = 1;
            }
        }
    }
    decode = jerasure_matrix_decode(group_size_, 1, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_);

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

bool OPTIMAL_LRC::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
{
    bool repair[fail_num];
    int *group_id;
    get_group_id(fail_num, fail_list, group_id);
    for (int i = 0; i < fail_num; i++)
    {
        repair[i] = false;
        char **data_ptr_ = new char *[group_size_];
        char **code_ptr_ = new char *[1];
        for (int j = group_id[i] * (group_size_ - r_); j < (group_id[i] + 1) * (group_size_ - r_); j++)
        {
            data_ptr_[j - group_id[i] * (group_size_ - r_)] = data_ptrs[j];
            //cout << "data_ptr_[" << j - group_id[i] * (group_size_ - r_) << "] = " << strlen(data_ptr_[j - group_id[i] * (group_size_ - r_)]) << endl;
        }
        for (int g = 0; g < r_; g++)
        {
            data_ptr_[group_size_ - r_ + g] = global_code_ptr[g];
        }

        code_ptr_[0] = local_ptr[group_id[i]];
        // cout<<"code_ptr_[0] = "<<strlen(code_ptr_[0])<<endl;
        repair[i] = single_decode(fail_list[i], data_ptr_, code_ptr_);
        if (repair[i] == false)
        {
            cout << "cant repair : " << fail_list[i] << endl;
            return false;
            break;
        }else{
            cout << "repair success : " << fail_list[i] << endl;
            // 将修复后的数据块放回到data_ptrs中
            for (int s = group_id[i] * (group_size_ - r_); s < (group_id[i] + 1) * (group_size_ - r_); s++)
            {
                data_ptrs[s] = data_ptr_[s - group_id[i] * (group_size_ - r_)];
                //cout << "data_ptrs[" << j << "] = " << strlen(data_ptrs[j]) << endl;
            }
            for (int g = 0; g < r_; g++)
            {
                global_code_ptr[g] = data_ptr_[group_size_ - r_ + g];
            }
            local_ptr[group_id[i]] = code_ptr_[0];
        }
    }
    return true;
}

void OPTIMAL_LRC::get_group_id(int fail_num, int *fail_list, int *&group_id)
{
    group_id = new int[fail_num];
    int *temp = new int[fail_num];
    int count_ = 0;
    // cout<< "group_size = " << group_size << endl;
    // cout<< "remainder = " << remainder << endl;

    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] < k_)
        {
            group_id[i] = fail_list[i] / (group_size_ - r_);
            if ((group_id[i] != 0) && (fail_list[i] % (group_size_ - r_) != 0))
                group_id[i]--;
        }
        else if (fail_list[i] >= k_ && fail_list[i] < (k_ + r_))
        {
            // 全局节点错误，group_id设置为-1
            group_id[i] = -1;
            temp[count_] = i;
            count_++;
        }
        else
        {
            group_id[i] = fail_list[i] - (k_ + r_);
        }
    }
    for (int i = 0; i < fail_num; i++)
    {
        if (group_id[i] == -1)
        {
            bool found = false;
            for (int j = 0; j < p_; j++)
            {
                bool exists = false;
                for (int k = 0; k < fail_num; k++)
                {
                    if (group_id[k] == j)
                    {
                        exists = true;
                        break;
                    }
                }
                if (!exists)
                {
                    group_id[i] = j;
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                group_id[i] = 0;
            }
        }
    }
}

bool OPTIMAL_LRC::if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id)
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
        bool repair_last_patity = false;
        for (int i = 0; i < fail_num; i++)
        {
            if (fail_list[i] == (k_ + r_ + p_ - 1))
            {
                repair_last_patity = true;
            }
        }
        if (count == 0 )
        {
            if (single_repair_cost >= k_)
            {
                cout << "need mds" << endl;
                return true;
            }
        }
        else
        {
            int repair_k = k_ + r_ - (fail_num - count);

            if (single_repair_cost < repair_k)
            {
                cout << "no need mds" << endl;
                return false;
            }
            else
            {
                cout << "need mds" << endl;
                return true;
            }
        }

        cout << "no need mds" << endl;
        return false;
    }
}

int OPTIMAL_LRC::find_mds_local_parity_blocks(int fail_number, int *fail_list, int *&node_id)
{
    int single_repair = 0;
    find_k_blocks(fail_number, fail_list, node_id);
    //若局部校验位出错则需要全部全局校验位（局部修复）
    for (int i = 0; i < fail_number; i++)
    {
        if ((fail_list[i] >= (k_ + r_)))
        {
            for (int i = 0; i < r_; i++)
            {
                node_id[k_ + i] = k_ + i;
                single_repair++;
            }
            break;
        }
    }
    if (single_repair == 0)
    {
        return k_;
    }
    else
    {
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
}

