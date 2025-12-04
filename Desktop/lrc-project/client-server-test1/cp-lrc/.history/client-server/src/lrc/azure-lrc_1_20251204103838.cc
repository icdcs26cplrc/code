#include "../../include/lrc/azure-lrc_1.hh"
using namespace ClientServer;

AZURE_LRC_1::AZURE_LRC_1(int k, int r, int p, size_t blocksize)
{
    k_ = k;
    r_ = r;
    p_ = p;
    blocksize_ = blocksize;
    group_size_ = k_ / (p_ - 1);
    remainder_ = k_ % (p_ - 1);
    if (remainder_ != 0)
    {
        group_size_++;
    }
    if (k <= 0 || r <= 0 || p <= 0 || blocksize <= 0)
    {
        throw std::invalid_argument("Invalid parameters: values must be positive.");
    }
}
void AZURE_LRC_1::generate_globalmatrix(int *&final_matrix_g)
{
    final_matrix_g = cauchy_original_coding_matrix(k_, r_, 8);
}

void AZURE_LRC_1::generate_localmatrix(int *&final_matrix_l)
{
    int *matrix = cauchy_original_coding_matrix(k_, r_, 8);
    int temp = 0;

    for (int i = 0; i < p_; i++)
    {
        if (i < (p_ - 1))
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
        else
        {
            for (int e = 0; e < k_; e++)
            {
                for (int l = 0; l < r_; l++)
                {
                    temp = temp ^ matrix[l * k_ + e];
                }
                final_matrix_l[i * k_ + e] = temp;
                temp = 0;
            }
        }
    }
}

int AZURE_LRC_1::single_decode_node_need(int fail_one, int *&node_id, int group_id__)
{
    int fail_list[] = {fail_one};
    int start, end, parity_id, return_group_size;

    if (fail_one >= (k_) && fail_one < (k_ + r_) || fail_one == (k_ + r_ + p_ - 1))
    {
        start = k_;
        end = k_ + r_;
        parity_id = k_ + r_ + p_ - 1;
        return_group_size = r_;
    }
    else
    {
        int *group_id;
        get_group_id(1, fail_list, group_id);
        parity_id = group_id[0] + k_ + r_;
        node_id = new int[group_size_];
        // 找到解码所需要的node_id
        start = group_id[0] * group_size_;
        end = start + group_size_;
        return_group_size = group_size_;
        // cout<<"group_id : "<<group_id[0] << endl;
    }
    // cout << "start = " << start << " end = " << end << endl;
    // cout << "group_id = " << group_id[0]  << endl;
    // cout << "fail_one = " << fail_one << endl;
    node_id = new int[return_group_size];
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

            cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
            j++;
            if (j == end)
            {
                break;
            }
        }
        node_id[return_group_size - 1] = parity_id;
    } // 局部校验位lose
    else
    {
        for (int i = start; i < end; i++)
        {
            node_id[i - start] = i;
            cout << "node_id[" << i - start << "] = " << node_id[i - start] << endl;
        }
    }
    return return_group_size;
}

bool AZURE_LRC_1::single_decode(int fail_one, char **data_ptrs, char **code_ptr, size_t decode_size)
{
    int group_real;
    int decode = -1;
    int *erasures = new int[2];

    erasures[1] = -1;
    if ((fail_one >= (k_) && fail_one < (k_ + r_)) || fail_one == (k_ + r_ + p_ - 1))
    {
        group_real = r_;
        if (fail_one != (k_ + r_ + p_ - 1))
        {
            erasures[0] = (fail_one - k_) % group_real;
        }
        else
        {
            erasures[0] = group_real;
        }
    }
    else if (fail_one < k_ || (fail_one >= (k_ + r_) && fail_one < (k_ + r_ + p_ - 1)))
    {
        group_real = group_size_;
        if (fail_one < k_)
        {
            erasures[0] = fail_one % group_real;
        }
        else
        {
            erasures[0] = group_real;
        }
    }

    // int *final_matrix = new int[(group_real + 1) * group_real];
    int *final_matrix = new int[(1) * group_real];

    for (int j = 0; j < group_real; j++)
    {
        // final_matrix[i * group_real + j] = 1;
        final_matrix[j] = 1;
    }
    //     }
    // }
    if (decode_size == 0)
    {
        decode_size = blocksize_;
    }

    decode = jerasure_matrix_decode(group_real, 1, 8, final_matrix, 1, erasures, data_ptrs, code_ptr, decode_size);

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
void AZURE_LRC_1::get_group_id(int fail_num, int *fail_list, int *&group_id)
{

    group_id = new int[fail_num];
    // cout<< "group_size = " << group_size << endl;
    // cout<< "remainder = " << remainder << endl;

    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] < k_)
        {
            group_id[i] = fail_list[i] / group_size_;
            // if ((group_id[i] != 0) && (fail_list[i] % groupsize_val != 0))
            //     group_id[i]--;
        }
        else if (fail_list[i] >= k_ && fail_list[i] < (k_ + r_))
        {
            group_id[i] = p_ - 1;
        }
        else if (fail_list[i] >= (k_ + r_) && fail_list[i] < (k_ + r_ + p_))
        {
            group_id[i] = fail_list[i] - (k_ + r_);
        }
    }
}

bool AZURE_LRC_1::muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr, size_t decode_size)
{
    bool repair[fail_num];
    int *group_id;
    get_group_id(fail_num, fail_list, group_id);
    for (int i = 0; i < fail_num; i++)
    {
        repair[i] = false;
        char **data_ptr_;
        char **code_ptr_ = new char *[1];
        if ((fail_list[i] >= k_ && fail_list[i] < (k_ + r_)) || fail_list[i] == (k_ + r_ + p_ - 1))
        {
            data_ptr_ = new char *[r_];
            for (int j = 0; j < r_; j++)
            {
                data_ptr_[j] = global_code_ptr[j];
            }
            code_ptr_[0] = local_ptr[p_ - 1];
        }
        else
        {
            data_ptr_ = new char *[group_size_];
            for (int j = group_id[i] * group_size_; j < (group_id[i] + 1) * group_size_; j++)
            {
                data_ptr_[j - group_id[i] * group_size_] = data_ptrs[j];
                cout << "data_ptr_[" << j - i * group_size_ << "] = " << strlen(data_ptr_[j - i * group_size_]) << endl;
            }
            code_ptr_[0] = local_ptr[group_id[i]];
        }
        // cout<<"code_ptr_[0] = "<<strlen(code_ptr_[0])<<endl;
        repair[i] = single_decode(fail_list[i], data_ptr_, code_ptr_, decode_size);
        if (repair[i] == false)
        {
            cout << "cant repair : " << fail_list[i] << endl;
            return false;
            break;
        }
        else
        {
            cout << "repair success : " << fail_list[i] << endl;
            if (fail_list[i] >= k_ && fail_list[i] < (k_ + r_))
            {
                // 将修复后的数据块放回到global_code_ptr中
                for (int l = 0; l < r_; l++)
                {
                    global_code_ptr[l] = data_ptr_[l];
                    // cout << "global_code_ptr[" << l << "] = " << strlen(global_code_ptr[l]) << endl;
                }
                local_ptr[p_ - 1] = code_ptr_[0];
            }
            else
            {
                // 将修复后的数据块放回到local_ptr中
                for (int q = group_id[i] * group_size_; q < (group_id[i] + 1) * group_size_; q++)
                {
                    data_ptrs[q] = data_ptr_[q - group_id[i] * group_size_];
                    // cout << "data_ptrs[" << q << "] = " << strlen(data_ptrs[q]) << endl;
                }
                local_ptr[group_id[i]] = code_ptr_[0];
            }
        }
    }
    return true;
}

int AZURE_LRC_1::find_mds_local_parity_blocks(int fail_number, int *fail_list, int *&node_id)
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

int AZURE_LRC_1::get_repair_num(int fail_num, int *fail_list_group_id)
{

    int count_1 = 0, count_2 = 0;
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list_group_id[i] < (p_ - 1))
        {
            count_1++;
        }
        else if (fail_list_group_id[i] == p_ - 1)
        {
            count_2++;
        }
    }

    return group_size_ * count_1 + r_ * count_2;
}
