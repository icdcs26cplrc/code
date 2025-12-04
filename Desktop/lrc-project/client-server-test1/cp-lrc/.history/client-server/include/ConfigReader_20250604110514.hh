#ifndef CONFIG_READER_H
#define CONFIG_READER_H

#include <string>
#include <vector>
#include <map>
#include <memory>

class ConfigReader {
public:
    ConfigReader();
    ~ConfigReader();
    
    // 加载配置文件
    bool loadConfig(const std::string& configFile);
    
    // 获取单个配置值
    std::string getString(const std::string& name, const std::string& defaultValue = "") const;
    int getInt(const std::string& name, int defaultValue = 0) const;
    
    // 获取多个配置值（用于agents.addr这种多值配置）
    std::vector<std::string> getStringList(const std::string& name) const;
    
    // 获取节点地址+端口对列表
    struct NodeEndpoint {
        std::string ip;
        int port;
        std::string toString() const { return ip + ":" + std::to_string(port); }
    };
    std::vector<NodeEndpoint> getNodeEndpoints(const std::string& addrConfigName, const std::string& portConfigName, int basePort = 0) const;
    
    // 检查配置是否存在
    bool hasConfig(const std::string& name) const;
    
    // 打印所有配置（用于调试）
    void printAllConfigs() const;

private:
    // 配置存储：key -> values列表
    std::map<std::string, std::vector<std::string>> configs_;
    
    // 解析XML配置文件
    bool parseXMLConfig(const std::string& configFile);
    
    // 去除字符串两端空白
    std::string trim(const std::string& str) const;
};

// 全局配置实例
extern std::unique_ptr<ConfigReader> g_config;

// 便利宏定义，用于替换原有的#define
#define GET_CONFIG_STRING(name, default_val) (g_config ? g_config->getString(name, default_val) : default_val)
#define GET_CONFIG_INT(name, default_val) (g_config ? g_config->getInt(name, default_val) : default_val)
#define GET_CONFIG_STRING_LIST(name) (g_config ? g_config->getStringList(name) : std::vector<std::string>())

#endif // CONFIG_READER_H