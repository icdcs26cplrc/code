#ifndef CLIENT_HH
#define CLIENT_HH

#include "socket.hh"
#include "metadata.hh"
#include "common.hh"
#include "../third_party/asio/asio/include/asio.hpp"
#include <asio/ssl.hpp>
#include <vector>
#include "config_defination.hh"
#include <time.h>


using namespace std;

namespace ClientServer {
class Client {
public:
  Client();
  void StartListen( ) ;
  size_t RecvACK(string des,char *&buf);
  string sayHelloToCoordinator(string hello) ;
  bool set(string key, string value);
  bool SetParameter(ECSchema input_ecschema) ;
  bool get(string key, string &value) ;
  bool repair(string failed_node_str) ;
  bool FileToKeyValue(string file_path, std::string& key, std::string& value) ;
  void SendRequestToCoordinator(string value) ;
  void SendValueToProxy(string value) ;
private:
  string m_coordinatorIpPort;
  string m_clientIPForGet;
  int m_clientPortForGet;

  int InitClient();

  int client_socket_connect = -1;
  
};
} // namespace ClientServer

#endif // CLIENT_H