#pragma once
#include <iostream>
#include <sstream>
#include <thread>
#include <string>

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

class Logger {
public:
    static Logger& GetInstance() {
        static Logger instance;
        return instance;
    }

    void Log(LogLevel level, const std::string& file, int line, const std::string& msg) {
        std::ostringstream oss;
        switch (level) {
            case LogLevel::DEBUG: oss << "\033[36m[DEBUG]\033[0m "; break;
            case LogLevel::INFO:  oss << "\033[32m[INFO]\033[0m  "; break;
            case LogLevel::WARN:  oss << "\033[33m[WARN]\033[0m  "; break;
            case LogLevel::ERROR: oss << "\033[31m[ERROR]\033[0m "; break;
        }
        
        // Strip path from file to just keep the filename
        std::string filename = file;
        auto pos = filename.find_last_of('/');
        if (pos != std::string::npos) {
            filename = filename.substr(pos + 1);
        }

        oss << "[" << filename << ":" << line << "] "
            << "[" << std::this_thread::get_id() << "] "
            << msg << "\n";
        
        std::cout << oss.str();
    }
};

#define LOG_DEBUG(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    Logger::GetInstance().Log(LogLevel::DEBUG, __FILE__, __LINE__, oss.str()); \
} while(0)

#define LOG_INFO(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    Logger::GetInstance().Log(LogLevel::INFO, __FILE__, __LINE__, oss.str()); \
} while(0)

#define LOG_WARN(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    Logger::GetInstance().Log(LogLevel::WARN, __FILE__, __LINE__, oss.str()); \
} while(0)

#define LOG_ERROR(msg) do { \
    std::ostringstream oss; \
    oss << msg; \
    Logger::GetInstance().Log(LogLevel::ERROR, __FILE__, __LINE__, oss.str()); \
} while(0)
