#ifndef NEW_LRC_HH
#define NEW_LRC_HH
#include "metadata.hh"
#include "common.hh"

namespace ClientServer {
class NewLRC {
public:
    NewLRC();
    void new_lrc_generate_matrix(int k, int r, int p, int *final_matrix_g,int *final_matrix_l);
    bool combine(std::shared_ptr<std::vector<std::vector<int>>> ans, int n, int k);
    void new_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs,char **local_coding_ptrs, int blocksize);
    bool decode(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, std::shared_ptr<std::vector<int>> erasures, int blocksize, std::string encode_type, bool repair = false);
    int check_decodable_azure_lrc(int k, int g, int l, std::vector<int> failed_block, std::vector<int> new_matrix);
};
}
#endif
