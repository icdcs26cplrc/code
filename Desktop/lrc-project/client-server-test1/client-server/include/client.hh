#ifndef CLIENT_HH
#define CLIENT_HH

#include "metadata.hh"
#include "common.hh"
#include "../third_party/asio/asio/include/asio.hpp"
#include <asio/ssl.hpp>
#include <vector>

using namespace std;

namespace ClientServer {
class Client {
    public:
    Client();


void SetCoordinator( string addr_port) ;
  string sayHelloToCoordinator(string hello) ;
  bool set(string key, string value);
  bool SetParameter(ECSchema input_ecschema) ;
  bool get(string key, string &value) ;
  bool repair(vector<int> failed_node_list) ;
  void FileToKeyValue(string file_path, std::string key, std::string value) ;
  
private:
  string m_coordinatorIpPort;
  string m_clientIPForGet;
  int m_clientPortForGet;
  // ASIO组件
  asio::streambuf m_buffer;
  asio::io_context m_io_context;
  asio::ip::tcp::acceptor m_acceptor;
  asio::ssl::context m_ssl_context; 
};
} // namespace ClientServer

#endif // CLIENT_H