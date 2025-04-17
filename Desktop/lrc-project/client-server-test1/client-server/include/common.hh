#ifndef COMMON_HH
#define COMMON_HH

#include <iostream>
#include <map>
#include <memory>
#include <string.h>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <functional>
#include <algorithm>
#include <unordered_set>
#include <random>
#include <set>
#include <sstream>
#include <thread>
#include <netinet/in.h>
#include <chrono>
#include "../third_party/jerasure/include/jerasure.h"
#include "../third_party/jerasure/include/reed_sol.h"
#include"../third_party/jerasure/include/cauchy.h"

#define CLIENT_PORT 12345
#define COORDINATOR_PORT 12378
#define PROXY_PORT_BASE 23456 
#define DATANODE_PORT_BASE 34567
#define MAX_FILE_SIZE 1048576 // 1MB in bytes
#endif