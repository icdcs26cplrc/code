#include "../include/lrc/lrc_base.hh"

using namespace std;
using namespace ClientServer;

void LRC_BASE::encode(char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs)
{
    // 1. 生成全局矩阵
    int k_val = k();
    int r_val = r();
    int p_val = p();
    int blocksize_val = blocksize();

    // for (int i = 0; i < (k_val+r_val+p_val); i++)
    // {
    //    if (i<(k_val))
    //    {
    //     print_data_info(data_ptrs[i],blocksize_val,"encode data_ptr");
    //    }else if (i>=k_val && i < (k_val+r_val))
    //    {
    //     cout<<"Before encode : size of global_ptr :"<<strlen(global_coding_ptrs[i-k_val])<<endl;
    //    }else if(i >= (k_val+r_val) && i< (k_val+r_val+p_val)){
    //     cout<<"Before encode : size of local_ptr :"<<strlen(local_coding_ptrs[i-k_val-r_val])<<endl;
    //    }
       
       
    // }
    int *global_matrix = new int[k_val * r_val];
    generate_globalmatrix(global_matrix);
    if (!global_matrix)
    {
        std::cerr << "Global matrix generation failed!" << std::endl;
        return;
    }
    // for (int i = 0; i < r_val; i++)
    // {
    //     for (int j = 0; j < k_val; j++)
    //     {
    //         cout<<"global_matrix["<<i*k_val+j<<"] ="<< global_matrix[i*k_val+j]<<endl;
    //     }
        
    // }
    

    // 2. 生成局部矩阵
    int *local_matrix = new int[k_val * p_val];
    generate_localmatrix(local_matrix);
    // for (int i = 0; i < p_val; i++)
    // {
    //     for (int j = 0; j < k_val; j++)
    //     {
    //         cout<<"local_matrix["<<i*k_val+j<<"] ="<< local_matrix[i*k_val+j]<<endl;
    //     }
        
    // }
    
    if (!local_matrix)
    {
        std::cerr << "Local matrix generation failed!" << std::endl;
        delete[] global_matrix;
        return;
    }

    // 3. 调用 Jerasure 编码（公共步骤）
    jerasure_matrix_encode(k_val, r_val, 8, global_matrix, data_ptrs, global_coding_ptrs, blocksize_val);
    jerasure_matrix_encode(k_val, p_val, 8, local_matrix, data_ptrs, local_coding_ptrs, blocksize_val);
    for (int i = 0; i < (k_val+r_val+p_val); i++)
    {
       if (i<(k_val))
       {
        print_data_info(data_ptrs[i],blocksize_val,"after encode data ptr :");
       }else if (i>=k_val && i < (k_val+r_val))
       {
        print_data_info(global_coding_ptrs[i-k_val],blocksize_val,"after encode global ptr :");
       }else if(i >= (k_val+r_val) && i< (k_val+r_val+p_val)){
        print_data_info(local_coding_ptrs[i-k_val-r_val],blocksize_val,"after encode local ptr :");
       }
       
       
    }
    

    // 4. 清理矩阵内存
    delete[] global_matrix;
    delete[] local_matrix;
}

void LRC_BASE::print_data_info(char* data, int size, const char* name) {
    cout << name << " size: " << size << ", first 10 bytes: ";
    for (int i = 0; i < min(10, size); i++) {
        cout << (int)(unsigned char)data[i] << " ";
    }
    cout << endl;
}

void LRC_BASE::find_k_blocks(int fail_num, int *fail_list, int *&node_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();
    node_id = new int[k()];
    int count = 0;
    for (int i = 0; i < (k_val + r_val + p_val) && count < k_val; i++)
    {
        bool is_failed = false;
        for (int j = 0; j < fail_num; j++)
        {
            if (fail_list[j] == i)
            {
                is_failed = true;
                break;
            }
        }
        if (!is_failed)
        {
            node_id[count] = i;
            count++;
        }
    }
}

void LRC_BASE::get_group_id(int fail_num, int *fail_list, int *&group_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();
    int remainder_val = remainder();
    int groupsize_val = group_size();
    group_id = new int[fail_num];
    // cout<< "group_size = " << group_size << endl;
    // cout<< "remainder = " << remainder << endl;
    if (remainder_val == 0)
    {
        for (int i = 0; i < fail_num; i++)
        {
            if (fail_list[i] < (k() + r()))
            {
                group_id[i] = fail_list[i] / groupsize_val;
                // if ((group_id[i] != 0) && (fail_list[i] % groupsize_val != 0))
                //     group_id[i]--;
            }
            else
            {
                group_id[i] = fail_list[i] - (k_val + r_val);
            }
        }
    }
    else
    {
        int small_part = (p_val - remainder_val) * (groupsize_val - 1);
        for (int i = 0; i < fail_num; i++)
        {
            if (fail_list[i] < small_part)
            {
                group_id[i] = fail_list[i] / (groupsize_val - 1);
                if ((group_id[i] != 0) && (fail_list[i] % (groupsize_val - 1) != 0))
                    group_id[i]--;
            }
            else if (fail_list[i] >= small_part && fail_list[i] < (k_val + r_val))
            {
                int temp = (fail_list[i] - small_part) / groupsize_val;
                if ((temp != 0) && ((fail_list[i] - small_part) % groupsize_val != 0))
                    temp--;
                group_id[i] = temp + p_val - remainder_val;
            }
            else
            {
                group_id[i] = fail_list[i] - (k_val + r_val);
            }
        }
    }
}

int LRC_BASE::check_decode(int fail_num, int *fail_list_group_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();

    int flag = -1;
    if (fail_num < (r_val + 1))
    {
        flag = 0;
        return flag;
    }
    else if (fail_num == (r_val + 1))
    {
        flag = 1;
        return flag;
    }
    else if (fail_num > (r_val + 1) && fail_num <= (r_val + p_val))
    {
        flag = fail_num - r_val;
        while (fail_num > (r_val + 1))
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
    else if (fail_num > (r_val + p_val))
    {
        cout << "too many failure" << endl;
        flag = -1;
        return flag;
    }
}

bool LRC_BASE::if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();

    // 如果fail均为局部校验位，则不需要mds
    int count = 0;
    for (int i = 0; i < fail_num; i++)
    {
        if (fail_list[i] >= (k_val + r_val))
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
        int *group_fail_num = new int[p_val]();
        // 一个组中有大于一个错,MDS NEED
        for (int j = 0; j < p_val; j++)
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
        cout<<"single_repair_cost : "<<single_repair_cost<<endl;
        if (single_repair_cost >= k_val)
        {
            cout << "need mds" << endl;
            return true;
        }
    }
    cout << "no need mds" << endl;
    return false;
}

int LRC_BASE::get_repair_num(int fail_num, int *fail_list_group_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();
    int remainder_val = remainder();
    int groupsize_val = group_size();

    int small_group_num = p_val - remainder_val;
    if (remainder_val == 0)
    {
        return groupsize_val * fail_num;
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
        return (count_1 * (groupsize_val - 1) + count_2 * groupsize_val);
    }
}

bool LRC_BASE::mds_decode(int locality, int fail_num, int *fail_list, char **data_ptrs, char **code_ptr)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();
    int remainder_val = remainder();
    int groupsize_val = group_size();
    size_t blocksize_val = blocksize();

    int *final_matrix = new int[(k_val + r_val + locality) * k_val];
    for (int i = 0; i < k_val; i++)
    {
        for (int j = 0; j < k_val; j++)
        {
            if (i == j)
            {
                final_matrix[i * k_val + j] = 1;
            }
            else
            {
                final_matrix[i * k_val + j] = 0;
            }
            cout << "final_matrix[" << ( i) * k_val + j << "] = " << final_matrix[( i) * k_val + j] << endl;
        }
        
    }
    int *erasures = new int[fail_num + 1];
    for (int i = 0; i <= fail_num; i++)
    {
        if (i< fail_num)
        {
            erasures[i] = fail_list[i];
            
        }
        else if (i == fail_num)
        {
            erasures[i] = -1;
        }
        //cout << "mds : erasures[" << i << "] = " << erasures[i] << endl;
    }

    int *global_parity = cauchy_original_coding_matrix(k_val, r_val, 8);
    for (int i = 0; i < r_val; i++)
    {
        for (int j = 0; j < k_val; j++)
        {
            final_matrix[(k_val + i) * k_val + j] = global_parity[i * k_val + j];
            // cout <<"global_parity[" << i << "][" << j << "] = " << global_parity[i * k + j] << endl;
            cout << "final_matrix[" << (k_val + i) * k_val + j << "] = " << final_matrix[(k_val + i) * k_val + j] << endl;
        }
    }
    if (locality > 0)
    {
        int *locality_matrix = new int[locality * k_val];
        generate_localmatrix(locality_matrix);
        for (int i = 0; i < locality; i++)
        {
            for (int j = 0; j < k_val; j++)
            {
                final_matrix[(k_val + r_val + i) * k_val + j] = locality_matrix[i * k_val + j];
                // cout << "locality_matrix[" << i << "][" << j << "] = " << locality_matrix[i * k + j] << endl;
                cout << "final_matrix[" << (k_val + r_val + i) * k_val + j << "] = " << final_matrix[(k_val + r_val + i) * k_val + j] << endl;
            }
        }
    }

    // debug
     for (int i = 0; i < (k_val+r_val); i++)
     {
         if (i< k_val)
         {
             print_data_info(data_ptrs[i],blocksize_val,"mds decode data ptr : ");
         }else{
            print_data_info(code_ptr[i-k_val],blocksize_val,"mds decode code ptr : ");
        }

    }
    

    int decode = jerasure_matrix_decode(k_val, r_val + locality, 8, final_matrix, 0, erasures, data_ptrs, code_ptr, blocksize_val);
    for (int i = 0; i < (k_val+r_val); i++)
    {
        if (i< k_val)
        {
            print_data_info(data_ptrs[i],blocksize_val,"After mds decode data ptr : ");
        }else{
           print_data_info(code_ptr[i-k_val],blocksize_val,"After mds decode code ptr : ");
       }

   }
    if (decode == 0)
    {
        cout << "MDS decode success" << endl;
        return true;
    }
    else
    {
       
        cout<<"decode :"<<decode<<endl;
        cout << "MDS decode fail" << endl;
        return false;
    }
    delete[] final_matrix;
    delete[] erasures;
    delete[] global_parity;
}

int LRC_BASE::resize_node_id(int num, int fail_num, int *fail_list, int *&node_id)
{
    // 去掉重复的节点
    sort(node_id, node_id + num);
    auto last = std::unique(node_id, node_id + num);
    int new_size_ = last - node_id;
    node_id = (int *)realloc(node_id, new_size_ * sizeof(int));
    if (node_id == nullptr)
    {
        std::cerr << "Memory reallocation failed." << std::endl;
        return -1;
    }
    // 更新count
    // count = new_size_ - k;
    // 去掉node_id数组中和fail_list数组相同的数值
    std::vector<int> node_id_vector(node_id, node_id + new_size_);
    node_id_vector.erase(std::remove_if(node_id_vector.begin(), node_id_vector.end(),
                                        [&](int id)
                                        {
                                            return std::find(fail_list, fail_list + fail_num, id) != fail_list + fail_num;
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
    cout<<"resize : "<<new_size<<endl;
    // 返回新的大小
    return new_size;
}

int LRC_BASE::find_mds_local_parity_blocks(int fail_number, int *fail_list, int *&node_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();
    if (fail_list[fail_number - 1] != (k_val + r_val + p_val - 1))
    {
        find_k_blocks(fail_number, fail_list, node_id);
    }
    else
    {
        int count = 0;
        for (int i = (k_val + r_val - 1); i >= 0 && count < k_val; i--)
        {
            bool is_failed = false;
            for (int j = 0; j < fail_number; j++)
            {
                if (fail_list[j] == i)
                {
                    is_failed = true;
                    break;
                }
            }
            if (!is_failed)
            {
                node_id[count] = i;
                count++;
            }
            if (i == 0 && count < (k_val - 1))
            {
                for (int g = 0; g < ((k_val - 1) - count); g++)
                {
                    node_id[count + g] = k_val + r_val + g;
                }
            }
        }
        sort(node_id, node_id + k_val);
    }

    return k_val;
}

int LRC_BASE::muti_decode_node_need(int fail_number, int *fail_list, int *&node_id)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();
    int remainder_val = remainder();
    int groupsize_val = group_size();
    size_t blocksize_val = blocksize();

    int *fail_group_id;
    get_group_id(fail_number, fail_list, fail_group_id);
    // 是否需要mds修复
    bool if_mds = if_need_mds(fail_number, fail_list, fail_group_id);
    // 是否可以修复
    int flag = check_decode(fail_number, fail_group_id);
    
    if (flag != -1)
    {
        if (if_mds == false)
        {
            int repair_num = get_repair_num(fail_number, fail_group_id);
            //可能存在重复节点故+10
            node_id = new int[repair_num+20];
            int node_need_num = 0;
            int node_need = 0;
            for (int i = 0; i < fail_number && node_need_num < (repair_num+20); i++)
            {
                int *node_id_split = new int [groupsize_val];
                node_need = single_decode_node_need(fail_list[i], node_id_split);
                if (node_need != 0)
                {
                    for (int j = 0; j < node_need; j++)
                    {

                        node_id[node_need_num] = node_id_split[j];
                        node_need_num++;
                    }
                }
                else
                {
                    cerr << "Error: node_id_split is null." << endl;
                }
                delete[] node_id_split;
                node_need = 0;
            }
            // resize 去掉重复或者是fail的节点
            node_need_num = resize_node_id(node_need_num, fail_number, fail_list, node_id);
            return node_need_num;
        }
        else
        {
            int mds_local = find_mds_local_parity_blocks(fail_number, fail_list, node_id);
            // 去掉重复的值
            // mds_local = resize_node_id(mds_local,fail_number,fail_list,node_id);
            return mds_local;
        }
    }
    else
    {
        cout << "can not repair" << endl;
        return -1;
    }
}

bool LRC_BASE::muti_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr)
{
    int k_val = k();
    int r_val = r();
    int p_val = p();

    int *fail_group_id;
    get_group_id(fail_num, fail_list, fail_group_id);
    // 是否可以修复
    int flag = check_decode(fail_num, fail_group_id);
    // 是否需要mds修复
    bool if_mds = if_need_mds(fail_num, fail_list, fail_group_id);
    if (flag != -1)
    {
        if (if_mds == true)
        {
            // 若有局部校验位fail
            int if_single = 0;
            // int *local_parity = new int[fail_num];
            bool mds_repair = false;
            for (int i = 0; i < fail_num; i++)
            {
                if ((fail_list[i] - (k_val + r_val)) >= flag)
                {
                    // local_parity[if_single] = fail_list[i] - (k_val + r_val);
                    if_single++;
                }
            }
            cout << "if_single : " << if_single << endl;
            char **code_ptr = new char *[r_val + flag];
            for (int i = 0; i < (r_val + flag); i++)
            {
                if (i >= 0 && i < r_val)
                {
                    code_ptr[i] = global_code_ptr[i];
                }
                else if (i >= r_val && i < (r_val + flag))
                {
                    code_ptr[i] = local_ptr[i - r_val];
                }
                cout<<" mds decode : size of code_ptr["<<i<<"]"<<strlen(code_ptr[i])<<endl;
            }
            if (if_single == 0)
            {
                mds_repair = mds_decode(flag, fail_num, fail_list, data_ptrs, code_ptr);
                return mds_repair;
            }
            else
            {
                int fail_mds = fail_num - if_single;
                int *fail_mds_ = new int[fail_mds];
                for (int i = 0; i < fail_mds; i++)
                {
                    fail_mds_[i] = fail_list[i];
                }
                mds_repair = mds_decode(flag, fail_mds, fail_mds_, data_ptrs, code_ptr);
                int *local_fail_list = new int[if_single];
                for (int i = 0; i < if_single; i++)
                {
                    local_fail_list[i] = fail_list[fail_num - if_single + i];
                }
                bool local_parity_repair = muti_single_decode(if_single, local_fail_list, data_ptrs, global_code_ptr, local_ptr);
                if (local_parity_repair && mds_repair)
                {
                    return true;
                }
                else
                {
                    if (!local_parity_repair)
                    {
                        cout << "mds&&local repair : local parity  false !" << endl;
                        return false;
                    }
                    else if (!mds_repair)
                    {
                        cout << " mds&&local repair : mds false !" << endl;
                        return false;
                    }
                    else
                    {
                        cout << " mds&&local repair : mds && local parity false !" << endl;
                        return false;
                    }
                }
            }
            //修复结束重新赋值
            for (int i = 0; i < (r_val + flag); i++)
            {
                if (i >= 0 && i < r_val)
                {
                     global_code_ptr[i]=code_ptr[i];
                }
                else if (i >= r_val && i < (r_val + flag))
                {
                    local_ptr[i - r_val]=code_ptr[i]  ;
                }
            }
        }
        else
        {
            bool muti_repair = muti_single_decode(fail_num, fail_list, data_ptrs, global_code_ptr, local_ptr);
            return muti_repair;
        }
    }
    else
    {
        cout << "cant repair" << endl;
        return false;
    }
}
