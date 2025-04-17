#ifndef META_DEFINITION
#define META_DEFINITION
#include"common.hh"
using namespace std;

namespace ClientServer{

  typedef struct NodeItem
  {
    unsigned int Node_id;
    string Node_ip;
    int Node_port;
    // 0:down; 1:up
    int alive;
    unordered_set<int> stripes;
  } NodeItem;
  struct ObjectItem
  {
    std::string key;
    std::vector<int> offset;
    std::vector<int> block_idx; // Changed to std::vector<int> for dynamic integer array
    int object_size;
    int stripe_id;
    // 是否位于coordinator上 1:yes; 0:no;
    int status;
  };ObjectItem;
  typedef struct BlockItem
  {
    unsigned int block_id;
    vector<ObjectItem> objects;
  } BlockItem;

  typedef struct StripeItem
  {
    unsigned int Stripe_id;
    int block_size;
    int k, r, p;
    vector<unsigned int> nodes;
    std::string encodetype;
  } StripeItem;

  typedef struct ECSchema
  {
    ECSchema() = default;

    ECSchema( std::string encodetype,  int k_datablock,
             int real_l_localgroup, int g_m_globalparityblock, int b_datapergoup)
        : encodetype(encodetype), 
          k_datablock(k_datablock), p_localgroup(p_localgroup),
          r_gobalparityblock(r_gobalparityblock){}
    std::string encodetype;
    int k_datablock;
    int p_localgroup;
    int r_gobalparityblock;
   

  } ECSchema;

  

 
} // namespace ClientServer

#endif // META_DEFINITION