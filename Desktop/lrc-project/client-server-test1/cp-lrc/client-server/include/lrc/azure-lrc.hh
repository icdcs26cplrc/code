#ifndef AZURE_LRC_HH
#define AZURE_LRC_HH

//#include "../include/metadata.hh"
//#include "../include/common.hh"
#include "lrc_base.hh"

namespace ClientServer
{
    class AZURE_LRC : public LRC_BASE
    {
    private:
        int k_;
        int r_;
        int p_;
        size_t blocksize_;
        int group_size_; 
        int remainder_;  
    public:
        AZURE_LRC(int k, int r, int p, size_t blocksize);
        // void encode(char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs );
        void generate_globalmatrix(int *&final_matrix_g) override;
        void generate_localmatrix(int *&final_matrix_l) override;
        bool muti_single_decode(int fail_num,int *fail_list,char **data_ptrs,char **global_code_ptr,char **local_ptr,size_t decode_size = 0) override;
        int single_decode_node_need(int fail_one, int *&node_id ,int group_id =0) override;
        bool single_decode(int fail_one, char **data_ptrs, char **code_ptr,size_t decode_size = 0) override;
        bool if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id) override;
        int k() const override { return k_; }
        int r() const override { return r_; }
        int p() const override { return p_; }  
        size_t blocksize() const override { return blocksize_; }
        int group_size() const override { return group_size_;}
        int remainder() const override { return remainder_;}
    };

}
#endif
