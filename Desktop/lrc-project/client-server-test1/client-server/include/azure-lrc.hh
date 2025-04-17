#ifndef AZURELRC_HH
#define AZURELRC_HH
#include "metadata.hh"
#include "common.hh"

class AzureLRC {
public:
    AzureLRC();
    ~AzureLRC();
    bool lrc_generate_matrix(int k, int r, int p, int *final_matrix);
    void dfs(std::vector<int> temp, std::shared_ptr<std::vector<std::vector<int>>> ans, int cur, int n, int k);
    bool combine(std::shared_ptr<std::vector<std::vector<int>>> ans, int n, int k);
    bool encode(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, std::string  encode_type);
    bool decode(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, std::shared_ptr<std::vector<int>> erasures, int blocksize, std::string encode_type, bool repair = false);
    bool check_received_block(int k, int expect_block_number, std::shared_ptr<std::vector<int>> shards_idx_ptr, int shards_ptr_size = -1);
    bool check_k_data(std::vector<int> erasures, int k);
    int check_decodable_azure_lrc(int k, int g, int l, std::vector<int> failed_block, std::vector<int> new_matrix);
};
#endif
