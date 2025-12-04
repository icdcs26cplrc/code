#include "../include/ConfigReader.hh"

#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>

// 简单的XML解析器，适用于你的配置格式
#include <regex>

// 全局配置实例
std::unique_ptr<ConfigReader> g_config = nullptr;

ConfigReader::ConfigReader() {
}

ConfigReader::~ConfigReader() {
}

bool ConfigReader::loadConfig(const std::string& configFile) {
    return parseXMLConfig(configFile);
}

std::string ConfigReader::getString(const std::string& name, const std::string& defaultValue) const {
    auto it = configs_.find(name);
    if (it != configs_.end() && !it->second.empty()) {
        return it->second[0]; // 返回第一个值
    }
    return defaultValue;
}

int ConfigReader::getInt(const std::string& name, int defaultValue) const {
    std::string strValue = getString(name);
    if (strValue.empty()) {
        return defaultValue;
    }
    
    try {
        return std::stoi(strValue);
    } catch (const std::exception& e) {
        std::cerr << "Error converting '" << strValue << "' to int for config '" << name << "'" << std::endl;
        return defaultValue;
    }
}

std::vector<std::string> ConfigReader::getStringList(const std::string& name) const {
    auto it = configs_.find(name);
    if (it != configs_.end()) {
        return it->second;
    }
    return std::vector<std::string>();
}

std::vector<ConfigReader::NodeEndpoint> ConfigReader::getNodeEndpoints(const std::string& addrConfigName, const std::string& portConfigName, int basePort) const {
    std::vector<NodeEndpoint> endpoints;
    
    // 获取IP地址列表
    std::vector<std::string> ips = getStringList(addrConfigName);
    
    // 获取端口列表或基础端口
    std::vector<std::string> portStrings = getStringList(portConfigName);
    std::vector<int> ports;
    
    if (!portStrings.empty()) {
        // 如果配置了端口列表，使用配置的端口
        for (const auto& portStr : portStrings) {
            try {
                ports.push_back(std::stoi(portStr));
            } catch (const std::exception& e) {
                std::cerr << "Error parsing port: " << portStr << std::endl;
            }
        }
    } else if (basePort > 0) {
        // 如果没有配置端口列表但提供了基础端口，为每个IP生成递增端口
        for (size_t i = 0; i < ips.size(); ++i) {
            ports.push_back(basePort + static_cast<int>(i));
        }
    }
    
    // 组合IP和端口
    for (size_t i = 0; i < ips.size(); ++i) {
        NodeEndpoint endpoint;
        endpoint.ip = ips[i];
        
        if (i < ports.size()) {
            endpoint.port = ports[i];
        } else {
            // 如果端口数量不够，使用基础端口+索引
            endpoint.port = basePort > 0 ? basePort + static_cast<int>(i) : 8080 + static_cast<int>(i);
        }
        
        endpoints.push_back(endpoint);
    }
    
    return endpoints;
}

bool ConfigReader::hasConfig(const std::string& name) const {
    return configs_.find(name) != configs_.end();
}

void ConfigReader::printAllConfigs() const {
    std::cout << "=== Configuration Settings ===" << std::endl;
    for (const auto& pair : configs_) {
        std::cout << pair.first << " = ";
        if (pair.second.size() == 1) {
            std::cout << pair.second[0] << std::endl;
        } else {
            std::cout << "[";
            for (size_t i = 0; i < pair.second.size(); ++i) {
                if (i > 0) std::cout << ", ";
                std::cout << pair.second[i];
            }
            std::cout << "]" << std::endl;
        }
    }
    std::cout << "==============================" << std::endl;
}

bool ConfigReader::parseXMLConfig(const std::string& configFile) {
    std::ifstream file(configFile);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open config file: " << configFile << std::endl;
        return false;
    }
    
    std::string line;
    std::string content;
    
    // 读取整个文件内容
    while (std::getline(file, line)) {
        content += line + "\n";
    }
    file.close();
    
    // 使用正则表达式解析attribute标签
    std::regex attr_regex(R"(<attribute>\s*<name>(.*?)</name>(.*?)</attribute>)", std::regex_constants::icase);
    std::regex value_regex(R"(<value>(.*?)</value>)", std::regex_constants::icase);
    
    std::sregex_iterator attr_iter(content.begin(), content.end(), attr_regex);
    std::sregex_iterator attr_end;
    
    while (attr_iter != attr_end) {
        std::smatch attr_match = *attr_iter;
        std::string name = trim(attr_match[1].str());
        std::string value_section = attr_match[2].str();
        
        // 解析value标签
        std::vector<std::string> values;
        std::sregex_iterator value_iter(value_section.begin(), value_section.end(), value_regex);
        std::sregex_iterator value_end;
        
        while (value_iter != value_end) {
            std::smatch value_match = *value_iter;
            std::string value = trim(value_match[1].str());
            if (!value.empty()) {
                values.push_back(value);
            }
            ++value_iter;
        }
        
        if (!values.empty()) {
            configs_[name] = values;
        }
        
        ++attr_iter;
    }
    
    std::cout << "Loaded " << configs_.size() << " configuration entries from " << configFile << std::endl;
    return !configs_.empty();
}

std::string ConfigReader::trim(const std::string& str) const {
    const std::string whitespace = " \t\n\r\f\v";
    size_t start = str.find_first_not_of(whitespace);
    if (start == std::string::npos) {
        return "";
    }
    size_t end = str.find_last_not_of(whitespace);
    return str.substr(start, end - start + 1);
}