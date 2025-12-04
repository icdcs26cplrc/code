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

  typedef struct ObjectItem
  {
    std::string key;
    std::vector<int> offset;
    std::vector<int> block_idx; // Changed to std::vector<int> for dynamic integer array
    int object_size;
    int stripe_id;
    // 是否位于coordinator上 1:yes; 0:no;
    int status;
  } ObjectItem;

  typedef struct BlockItem
  {
    std::string block_id;
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

    ECSchema( std::string encodetype,  uint64_t k_datablock,
              uint64_t r_gobalparityblock, uint64_t p_localgroup, uint64_t block_size)
        : encodetype(encodetype), 
          k_datablock(k_datablock), p_localgroup(p_localgroup),
          r_gobalparityblock(r_gobalparityblock),
          block_size(block_size) {}
    std::string encodetype;
    uint64_t k_datablock;
    uint64_t p_localgroup;
    uint64_t r_gobalparityblock;
    uint64_t block_size;

  } ECSchema;

  

 
} // namespace ClientServer

#endif // META_DEFINITION