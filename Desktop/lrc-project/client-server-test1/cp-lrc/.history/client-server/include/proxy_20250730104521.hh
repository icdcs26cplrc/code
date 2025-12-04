#ifndef COORDINATOR_HH
#define COORDINATOR_HH

#include "socket.hh"
#include "common.hh"
#include "metadata.hh"
#include"../include/lrc/azure-lrc.hh"
#include"../include/lrc/azure-lrc_1.hh"
#include"../include/lrc/grade-lrc.hh"
#include"../include/lrc/optimal-lrc.hh"
#include"../include/lrc/unbalaced-lrc.hh"
#include"../include/lrc/uniform-lrc.hh"
#include"../include/lrc/lrc_base.hh"
#include"../include/lrc/sep-azure.hh"

#include "client.hh"
#include "datanode.hh"
#include "config_defination.hh"
#include <mutex>
#include <future>
#include <chrono>
#include <atomic>


namespace ClientServer {
    class Proxy{
    public:
        bool io_repair_opt = false;
        bool read_opt = false;

        Proxy();
        void StartProxy();
        void StartProxy_Data();
        void RunServer();
        void HandleConnection(int client_fd);
        ssize_t RecvCommand(int client_fd, char* buf, size_t max_len); 
        void ProcessSet_Proxy(std::string  value);
        void ProcessGet_Proxy(std::string value);
        void ProcessRepair_Client(std::string value);
        void Repair_degrade(const int stripe_id,std::string message,int num,const int *node_id,const int *offset,const size_t* value_len,string &value, char ** data_already_get);


        //int check_error(int num ,size_t *value_len,size_t *recv_len,int * node_id,int *&error_id);

        //void SendACKToClient(std::string value);
        void SendMassgeToD(std::string destination,std::string value);
        void GetRevData(char **data, int num, size_t *value_len,size_t *&recv_len,bool short_or_long = false);
        //void GetRevDataFromDataNode(char **data, int num, size_t *value_len,size_t *&recv_len);
        size_t HandleGetRev(int one_or_two,char *&data, int connfd, size_t value_len,bool short_or_long = false);
        void SendBlockToDatanode(int node_id, char* data,size_t block_size) ;
        int SendGetRequest(int node_id, int offset, size_t value_len);

        int get_int_from_string (string data);
        void get_int_from_muti_(string data,int*& get,int sum);
        vector<DataNode::NodeStorage> GetNodeList();
        void print_data_info(char* data, int size, const char* name) ;

        //void ProcessGetRequests(char **data,size_t *&recv_len,const std::vector<std::string>& node_ids, const std::vector<int>& offsets, const std::vector<size_t>& value_lens) ;
        bool ProcessGetRequestsWithErrorHandling(char **data_get, size_t *&recv_len, 
            const std::vector<std::string>& node_ids, const std::vector<int>& offsets, 
            const std::vector<size_t>& value_lens, std::vector<bool>& request_success);

    private:

        int compute_degrade_offsets_and_lens(const int fail_num,const size_t blocksize,const std::vector<int>& first_get_node_ids ,const std::vector<int>& fail_node_ids,  std::vector<int>& degrade_get_node_ids ,const std::vector<int>& offsets, const std::vector<size_t>& value_lens, std::vector<int>& offset_vec, std::vector<size_t>& value_len_vec);
        int SendGetRequestWithTimeout(const std::string& node_id, int offset, size_t value_len, std::chrono::seconds timeout) ;  
        int CreateConnectionWithTimeout(std::string node_id,std::chrono::seconds timeout) ;
        void WaitForRequestsWithTimeout(std::vector<std::future<int>>& futures, std::chrono::seconds timeout) ;
        //void GetRevDataWithIsolation(char** data, int num, const size_t* value_lens, size_t*& recv_len, const int* request_status) ;
        //int FindAvailableNodeIndex(const int* request_status, int num, int current_handled) ;
        size_t HandleGetRevWithIsolation(char*& data, int connfd, size_t value_len, int node_index) ;
        void resort_recvdata(char **recv_data, size_t *recv_len, int num);
        int check_error_fixed(int num, size_t *recv_len, size_t *expected_len, int *node_id, int *&error_id, const std::vector<bool>& request_success);
        void GetRevDataWithErrorHandling(const std::vector<std::string>& node_ids,char** data, int num, const size_t* value_lens, size_t*& recv_len, const std::vector<bool>& request_success) ;
        void overlap_resort_recvdata(char **recv_data, size_t *recv_len, int num, const size_t *value_len);
        bool ProcessGetRequestsWithErrorHandling_step(char **data_get, size_t *&recv_len,const std::vector<std::string> &node_ids, const std::vector<int> &offsets,const std::vector<size_t> &value_lens, std::vector<bool> &request_success);
        size_t ReceiveDataFromSingleNode(char *&data, size_t expected_size, int node_index);
        bool ProcessGetRequestsWithErrorHandling_WithTiming(char **data_get, size_t *&recv_len,const std::vector<std::string> &node_ids, const std::vector<int> &offsets, const std::vector<size_t> &value_lens, std::vector<bool> &request_success);
        bool parseEncodeInfo(const std::string& encode_info, int& k, int& r, int& p, size_t& blocksize);

        bool performEncoding(const std::string& encode_type_str, int k, int r, int p, size_t blocksize,char** data_ptr, char** global_ptr, char** local_ptr);

        void sendBlocksParallel(int k, int r, int p, size_t blocksize,const std::vector<char*>& data_ptr,const std::vector<char*>& global_ptr,const std::vector<char*>& local_ptr);
        
        int sock_proxy=-1;
        int sock_proxy_data = -1;
        std::string buffer_data;
        std::atomic<bool> should_stop_{false};
        
    };
}



#endif 
