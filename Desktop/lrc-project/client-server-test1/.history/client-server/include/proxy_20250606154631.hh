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

#include "client.hh"
#include "datanode.hh"
#include "config_defination.hh"
#include <mutex>


namespace ClientServer {
    class Proxy{
    public:
        Proxy();
        void StartProxy();
        void StartProxy_Data();
        void RunServer();
        void HandleConnection(int client_fd);
        ssize_t RecvCommand(int client_fd, char* buf, size_t max_len); 
        void ProcessSet_Proxy(std::string  value);
        void ProcessGet_Proxy(std::string value);
        void ProcessRepair_Client(std::string value);
        void Repair_degrade(std::string message,int num,int *node_id,int *offset,size_t* value_len,string &value);


        int check_error(int num ,size_t *value_len,size_t *recv_len,int * node_id,int *&error_id);

        //void SendACKToClient(std::string value);
        void SendMassgeToD(std::string destination,std::string value);
        void GetRevData(char **data, int num, size_t *value_len,size_t *&recv_len);
        //void GetRevDataFromDataNode(char **data, int num, size_t *value_len,size_t *&recv_len);
        size_t HandleGetRev(char *&data, int connfd, size_t value_len);
        void SendBlockToDatanode(int datanode_port,int node_id,const char* data,size_t block_size) ;
        int SendGetRequest(int node_id, int offset, size_t value_len);

        int get_int_from_string (string data);
        void get_int_from_muti_(string data,int*& get,int sum);
        vector<DataNode::NodeStorage> GetNodeList();
        void print_data_info(char* data, int size, const char* name) ;

    private:
        int sock_proxy=-1;
        int sock_proxy_data = -1;
        std::string buffer_data;
        
    };
}



#endif 
