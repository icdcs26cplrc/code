#ifndef NEW_LRC_HH
#define NEW_LRC_HH
//#include "../include/metadata.hh"
//#include "../include/common.hh"
#include "lrc_base.hh"

namespace ClientServer {
class NewLRC : public LRC_BASE
{
    private:
    int k_;
    int r_;
    int p_;
    size_t blocksize_;
    int group_size_;
    int remainder_;

public:
    NewLRC(int data, int global, int local, size_t BlockSize);
    // void new_lrc_generate_globalmatrix(int k, int r, int p, int *&final_matrix_g);
    // void new_lrc_generate_localmatrix(int k, int r, int p, int *&final_matrix_l);
    // void new_encode(int k, int r, int p, char **data_ptrs, char **global_coding_ptrs,char **local_coding_ptrs, int blocksize);
    // int new_single_decode_node_need(int k, int r, int p, int fail_one,int *&node_id);
    // bool new_single_decode(int k, int r, int p,int fail_one, char **data_ptrs,char **code_ptr,int blocksize,int group_size);
    // int new_get_group_id(int k, int r, int p, int fail_one);
    // int new_decode_node_need(int k, int r, int p,int fail_number, int *fail_list,int *&node_id);
    // bool new_decode(int k, int r, int p,int fail_num, int *fail_list,char **data_ptrs,char **global_code_ptr,char**local_ptr,int blocksize);
    // int check_decode(int k, int r, int p,int fail_num,int *fail_list_group_id);
    // bool if_need_mds(int k,int r,int p,int fail_num,int * fail_list,int *fail_list_group_id);
    // void find_k_blocks(int k, int r,int locality,int fail_num, int *fail_list, int *&node_id);
    // int get_repair_num(int k, int r, int p, int fail_num, int *fail_list_group_id);
    // bool new_mds_decode(int k, int r,int locality,int fail_num, int *fail_list,char **data_ptrs,char **code_ptr,int blocksize);
    // int new_fail_localparity_num(int k, int r, int p,int fail_num,int *fail_list);
    // int resize_node_id(int k, int count, int fail_num,int *fail_list,int *&node_id);
    // bool mutiple_single_decode(int k, int r, int p,int fail_num,int *fail_list,char **data_ptrs,char **local_ptr,int blocksize);
        void generate_globalmatrix( int *&final_matrix_g) override;
        void generate_localmatrix( int *&final_matrix_l) override;
        int single_decode_node_need( int fail_one, int *&node_id) override;
        bool single_decode(int fail_one, char **data_ptrs, char **code_ptr ,size_t decode_size = 0) override;
        bool muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr,size_t decode_size = 0) override;
        int find_mds_local_parity_blocks(int fail_number,int * fail_list, int *&node_id) override;
        int check_decode( int fail_num, int *fail_list_group_id) override;
        //int muti_decode_node_need(int fail_number, int *fail_list, int *&node_id) override;
        //int find_k_g_blocks(int fail_number, int *fail_list, int *&node_id);
        //bool muti_decode( int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr) override;
        //bool r_2_repair(int group_id, int fail_num, int *fail_list, char **data_ptrs, char **code_ptr);

        int k() const override { return k_; }
        int r() const override { return r_; }
        int p() const override { return p_; }
        size_t blocksize() const override { return blocksize_; }
        int group_size() const override { return group_size_; }
        int remainder() const override { return remainder_; }
};

}
#endif
