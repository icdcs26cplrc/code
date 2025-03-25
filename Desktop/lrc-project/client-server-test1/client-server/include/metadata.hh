#ifndef META_DEFINITION
#define META_DEFINITION
#include"common.hh"
using namespace std;

namespace ClientServer{
  enum EncodeType
  {
    azure_lrc,
    azure_lrc_1,
    optimal,
    uniform,
    new_lrc,
  };
  typedef struct NodeItem
  {
    unsigned int Node_id;
    string Node_ip;
    int Node_port;
    int alive;
    unordered_set<int> stripes;
  } NodeItem;
  typedef struct ObjectItem
  {
    string Object_name;
    int offset ;
    int block_idx;
    int object_size;
    int stripe_id;
  } ObjectItem;

  typedef struct StripeItem
  {
    unsigned int Stripe_id;
    int block_size;
    int n, k, r, p;
    vector<unsigned int> nodes;
    EncodeType encodetype;
  } StripeItem;

  typedef struct ECSchema
  {
    ECSchema() = default;

    ECSchema(bool partial_decoding, EncodeType encodetype,  int k_datablock,
             int real_l_localgroup, int g_m_globalparityblock, int b_datapergoup)
        : partial_decoding(partial_decoding), encodetype(encodetype), 
          k_datablock(k_datablock), l_localgroup(l_localgroup),
          r_gobalparityblock(r_gobalparityblock), b_datapergoup(b_datapergoup){}
    bool partial_decoding;
    EncodeType encodetype;
    int k_datablock;
    int l_localgroup;
    int r_gobalparityblock;
    int b_datapergoup;
  } ECSchema;

  typedef struct Range
  {
    int offset;
    int length;
    Range() = default;
    Range(int offset, int length) : offset(offset), length(length) {}
  } Range;

  typedef struct ShardidxRange
  {
    int shardidx;
    int offset_in_shard;
    int range_length;
    ShardidxRange() = default;
    ShardidxRange(int idx, int offset, int length) : shardidx(idx), offset_in_shard(offset), range_length(length) {}
  };
} // namespace ClientServer

#endif // META_DEFINITION