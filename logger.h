#ifndef __LOGGER_H__
#define __LOGGER_H__

#ifdef __cplusplus
extern "C" {
#endif

enum LogLevelTypes //日志分级
{
    LOG_LEVEL_ERR,   /* error conditions */
    LOG_LEVEL_WARN,  /* warning conditions */
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_INFO,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_VERB
};

typedef struct logger_t {
    int fd;
    int level;
    char* name;
} logger_t;

extern logger_t logger;
extern logger_t lg2;
extern logger_t lg3;

#define _log_error(lg, fmt, ...) \
    do{ \
        if(log_loggable(&lg, LOG_LEVEL_ERR)){ \
            log_write(&lg, LOG_LEVEL_ERR, __FILE__, __LINE__, __FUNCTION__, \
                 fmt, ## __VA_ARGS__); \
        } \
    } while(0)
#define log_error(fmt, ...) _log_error(logger, fmt, ## __VA_ARGS__)

#define _log_warn(lg, fmt, ...) \
    do{ \
        if(log_loggable(&lg, LOG_LEVEL_WARN)){ \
            log_write(&lg, LOG_LEVEL_WARN, __FILE__, __LINE__, __FUNCTION__, \
                 fmt, ## __VA_ARGS__); \
        } \
    } while(0)
#define log_warn(fmt, ...) _log_warn(logger, fmt, ## __VA_ARGS__)

#define _log_notice(lg, fmt, ...) \
    do{ \
        if(log_loggable(&lg, LOG_LEVEL_NOTICE)){ \
            log_write(&lg, LOG_LEVEL_NOTICE, __FILE__, __LINE__, __FUNCTION__, \
                fmt, ## __VA_ARGS__); \
        } \
    } while(0)
#define log_notice(fmt, ...) _log_notice(logger, fmt, ## __VA_ARGS__)

#define _log_info(lg, fmt, ...) \
    do{ \
        if(log_loggable(&lg, LOG_LEVEL_INFO)){ \
            log_write(&lg, LOG_LEVEL_INFO, __FILE__, __LINE__, __FUNCTION__, \
                fmt, ## __VA_ARGS__); \
        } \
    } while(0)
#define log_info(fmt, ...) _log_info(logger, fmt, ## __VA_ARGS__)

#define _log_debug(lg, fmt, ...) \
    do{ \
        if(log_loggable(&lg, LOG_LEVEL_DEBUG)){ \
            log_write(&lg, LOG_LEVEL_DEBUG, __FILE__, __LINE__, __FUNCTION__, \
                 fmt, ## __VA_ARGS__); \
        } \
    } while(0)
#define log_debug(fmt, ...) _log_debug(logger, fmt, ## __VA_ARGS__)

#define _log_verb(lg, fmt, ...) \
    do{ \
        if(log_loggable(&lg, LOG_LEVEL_VERB)){ \
            log_write(&lg, LOG_LEVEL_VERB, __FILE__, __LINE__, __FUNCTION__, \
                 fmt, ## __VA_ARGS__); \
        } \
    } while(0)
#define log_verb(fmt, ...) _log_verb(logger, fmt, ## __VA_ARGS__)

void log_init(logger_t *log, const char *name, int level); 

void log_deinit(logger_t *log);

void log_level_set(logger_t *log, int level);

void log_reopen(logger_t *log);

void log_level_up(logger_t *log);

void log_level_down(logger_t *log);

int  log_loggable(logger_t *log, int level);

void log_write(logger_t *log, int level,
        const char *file, int line,
        const char* func, const char *fmt, ...)
    __attribute__((format(printf, 6, 7)));

#ifdef __cplusplus
}
#endif
#endif 

