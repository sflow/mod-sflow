#ifndef SFLOW_WB_H
#define SFLOW_WB_H 1

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <sys/stat.h>
#include <syslog.h>
#include <signal.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <limits.h>
#include <sysexits.h>
#include <stddef.h>
#include <sys/wait.h>
#include <sys/types.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h> /* for PRIu64 etc. */
#include <pthread.h>

#include "sflow_api.h"

/* #include <stdbool.h> */
#define true 1
#define false 0
typedef u_int8_t bool;

#define SFWB_VERSION "0.9"
#define SFWB_DEFAULT_CONFIGFILE "/etc/hsflowd.auto"
#define SFWB_MAX_TICKS 60
#define SFWB_SEPARATORS " \t\r\n="
#define SFWB_QUOTES "'\" \t\r\n"
/* SFWB_MAX LINE LEN must be enough to hold the whole list of targets */
#define SFWB_MAX_LINELEN 1024
#define SFWB_MAX_COLLECTORS 10
#define SFWB_CHILD_TICK_US 2000000
#define SFWB_CONFIG_CHECK_S 10

typedef struct _SFWBCollector {
  struct sockaddr sa;
  SFLAddress addr;
  uint16_t port;
  uint16_t priority;
} SFWBCollector;

typedef struct _SFWBConfig {
  int error;
  uint32_t sampling_n;
  uint32_t polling_secs;
  SFLAddress agentIP;
  uint32_t num_collectors;
  SFWBCollector collectors[SFWB_MAX_COLLECTORS];
} SFWBConfig;


typedef struct _SFWBChild {
  pthread_mutex_t *mutex;
  void *shared_mem_base; /* may be a different address for each worker */
  SFLAgent *agent;
  SFLReceiver *receiver;
  SFLSampler *sampler;
  SFLCounters_sample_element http_counters;
  apr_time_t lastTickTime;
  apr_pool_t *childPool;
} SFWBChild;

typedef struct _SFWB {
  int enabled;

  /* master process */
  apr_proc_t *sFlowProc;
  apr_pool_t *masterPool;

  /* master config */
  time_t currentTime;
  int configCountDown;
  char *configFile;
  time_t configFile_modTime;
  bool configOK;
  SFWBConfig config;
  SFWBConfig newConfig;

  /* master sFlow agent */
  int socket4;
  int socket6;
  SFLAgent *agent;
  SFLReceiver *receiver;
  SFLSampler *sampler;
  SFLPoller *poller;
  SFLCounters_sample_element http_counters;

  /* pipe for child->master IPC */
  apr_file_t *pipe_read;
  apr_file_t *pipe_write;

  /* shared mem for master->child IPC */
  apr_shm_t *shared_mem;
  void *shared_mem_base;
  size_t shared_bytes_total;
  size_t shared_bytes_used;

  /* per child state */
  SFWBChild *child;
} SFWB;

typedef struct _SFWBShared {
  uint32_t sflow_skip1;
  uint32_t sflow_skip2;
} SFWBShared;

void sflow_sample(SFWB *sm, int soc, SFLHTTP_method method, uint32_t protocol, char *uri, size_t urilen, char *host, size_t hostlen, char *referrer, size_t referrerlen, char *useragent, size_t useragentlen, char *authuser, size_t authuserlen, char *mimetype, size_t mimetypelen, size_t bytes, uint32_t duration_uS, uint32_t status);
#define SFLOW_DURATION_UNKNOWN 0
#define SFLOW_TOKENS_UNKNOWN 0
void sflow_init(SFWB *sm);
void sflow_tick(SFWB *sm);

/* If supported, give compiler hints for branch prediction. */
#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#endif /* SFLOW_WB_H */

