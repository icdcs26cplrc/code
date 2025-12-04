#ifndef LRC_BASE_HH
#define LRC_BASE_HH

#include "../include/metadata.hh"
//#include "../include/common.hh"


namespace ClientServer
{
    class LRC_BASE
    {
    protected:
        virtual int k() const = 0;
        virtual int r() const = 0;
        virtual int p() const = 0;
        virtual size_t blocksize() const = 0;
        virtual int group_size() const = 0;
        virtual int remainder() const = 0;

        virtual void generate_globalmatrix(int *&final_matrix_g) = 0;
        virtual void generate_localmatrix(int *&final_matrix_l) = 0;
        virtual int single_decode_node_need(int fail_one, int *&node_id,int group_id =0) = 0;
        virtual bool single_decode(int fail_one, char **data_ptrs, char **code_ptr,size_t decode_size = 0) = 0;
        virtual bool muti_single_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr,size_t decode_size = 0) = 0;
        

    public:
        LRC_BASE() {};
        void encode(char **data_ptrs, char **global_coding_ptrs, char **local_coding_ptrs);
        virtual void find_k_blocks(int fail_num, int *fail_list, int *&node_id);
        virtual void get_group_id(int fail_num, int *fail_list, int *&group_id);
        virtual int muti_decode_node_need(int fail_number, int *fail_list, int *&node_id);
        virtual bool muti_decode(int fail_num, int *fail_list, char **data_ptrs, char **global_code_ptr, char **local_ptr,  size_t decode_size = 0);
        virtual int check_decode(int fail_num, int *fail_list_group_id);
        virtual bool if_need_mds(int fail_num, int *fail_list, int *fail_list_group_id);
        virtual int get_repair_num(int fail_num, int *fail_list_group_id);
        virtual bool mds_decode(int locality, int fail_num, int *fail_list, char **data_ptrs, char **code_ptr, size_t decode_size = 0);
        virtual int find_mds_local_parity_blocks(int fail_number,int * fail_list, int *&node_id);
        virtual int resize_node_id(int num, int fail_num, int *fail_list, int *&node_id);
        virtual void print_data_info(char* data, int size, const char* name) ;
    };

}
#endif
