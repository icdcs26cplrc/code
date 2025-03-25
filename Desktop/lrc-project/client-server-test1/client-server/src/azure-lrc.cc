#include "../include/azure-lrc.hh"
using namespace std;
using namespace ClientServer;

bool check_k_data(const vector<int>& erasures, int k) {
    int flag = 1;
    for (int i = 0; i < k; i++)
    {
        if (find(erasures.begin(), erasures.end(), i) != erasures.end())
        {
            flag =0;
            break;
        }
        
    }
    if (flag)
    {
        return true;
    }
    
    return false;
}
// 假设每组包含 (k/p) 个数据块 + 1个局部校验块
vector<int> get_local_group_all_indices(int group_id, int k, int r, int p) 
{
    int group_size = k / p;
    std::vector<int> blocks;
    // 添加数据块索引
    for (int i = 0; i < group_size; i++) 
    {
        int idx = group_id * group_size + i;
        if (idx < k) blocks.push_back(idx);
    }
    // 添加局部校验块索引（假设全局校验块数为r，局部校验块从k+r开始）
    blocks.push_back(k + r + group_id);
    return blocks;
}
void xor_blocks(const char* src, char* dest, int size) {
    for (int i = 0; i < size; i++) {
        dest[i] ^= src[i]; // 直接异或到目标块（累积效果）
    }
}
void xor_multiple_blocks(const vector<char*>& blocks, char* dest, int size) {
    if (blocks.empty()) return;
    
    // 先复制第一个块到目标块
    memcpy(dest, blocks[0], size);
    // 从第二个块开始异或
    for (size_t i = 1; i < blocks.size(); i++) {
        xor_blocks(blocks[i], dest, size);
    }
}
bool decode(int k,int r,int p, char **data_ptrs, char **coding_ptrs, vector<int> erasures, int blocksize, EncodeType encode_type, bool repair) {
    if (encode_type == azure_lrc)
    {
         vector<int> matrix((r + p) * k, 0);
        lrc_generate_matrix(k, r, p, matrix.data());

        bool local_repaired = false;
        for (int group_id = 0; group_id < p; group_id++) {
            vector<int> group_blocks = get_local_group_all_indices(group_id, k, r, p);
            int local_parity_idx = group_blocks.back();

            // 统计组内损坏块
            vector<int> failed_in_group;
            for (int block_idx : group_blocks) {
                if (find(erasures.begin(), erasures.end(), block_idx) != erasures.end()) {
                    failed_in_group.push_back(block_idx);
                }
            }

            // 局部修复逻辑
            if (failed_in_group.size() == 1) {
                int failed_block = failed_in_group[0];
                bool is_data_block = (failed_block < k);
                char* target_ptr = is_data_block ? data_ptrs[failed_block] 
                                                : coding_ptrs[failed_block - k];

                // 收集存活块指针
                vector<char*> alive_blocks;
                for (int block_idx : group_blocks) {
                    if (block_idx != failed_block && 
                        find(erasures.begin(), erasures.end(), block_idx) == erasures.end()) {
                        char* src_ptr = (block_idx < k) ? data_ptrs[block_idx] 
                                                       : coding_ptrs[block_idx - k];
                        alive_blocks.push_back(src_ptr);
                    }
                }

                // 修复关键修正：使用正确的多块异或调用
                xor_multiple_blocks(alive_blocks, target_ptr, blocksize);

                // 更新损坏列表
                auto pos = find(erasures.begin(), erasures.end(), failed_block);
                if (pos != erasures.end()) {
                    erasures.erase(pos);
                    local_repaired = true;
                }
            } 
        }

        // 局部修复后检查是否成功
        if (local_repaired && erasures.empty()) {
            return true;
        }

        
        // 全局修复逻辑（当局部修复失败时触发）

        if (!erasures.empty()) {
            // 将损坏块索引转换为 Jerasure 要求的数组
            int num_erasures = erasures.size();
            int* erasure_codes = new int[num_erasures + 1];
            for (int i = 0; i < num_erasures; i++) {
                erasure_codes[i] = erasures[i];
            }
            erasure_codes[num_erasures] = -1; // Jerasure 要求以 -1 结尾

            // 调用 Jerasure 全局解码
            int success = jerasure_matrix_decode(
                k, r + p, 8, matrix.data(),
                1,  // 使用位矩阵（Azure LRC兼容）
                erasure_codes,
                data_ptrs, coding_ptrs, blocksize
            );

            delete[] erasure_codes;
            return (success == 0); // 返回解码是否成功
        }
        
    }
    
    return true;

}

bool lrc_generate_matrix(int k, int r, int p, int *final_matrix) {
    int *matrix = cauchy_original_coding_matrix(k, r, 8);
    bzero(final_matrix,  (r + p) *k* sizeof(int));
    memcpy(final_matrix, matrix, k*r*sizeof(int));
    int l = k/p;
    for (int i = 0; i < p; i++)
    {
        for (int j = i*l+1; j < (i+1)*l; j++)
        {
            final_matrix[k*r + i*k+j] = 1;
        }
        
    }
    
    return true;
}
bool encode(int k, int r, int p, char **data_ptrs, char **coding_ptrs, int blocksize, EncodeType encode_type) {
    if (encode_type == azure_lrc)
    {
        vector<int> new_matrix(k*(r+p), 0);
        lrc_generate_matrix(k, r, p, new_matrix.data());
        jerasure_matrix_encode(k, (r+p), 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);
   
    }
    
     return true;
}       
// Your code implementation here
int main() {
    // Example usage of Azure functions
    return 0;
}