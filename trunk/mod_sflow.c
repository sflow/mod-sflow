/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* Copyright (c) 2002-2010 InMon Corp. Licensed under the terms of the InMon sFlow licence: */
/* http://www.inmon.com/technology/sflowlicense.txt */

/* 
** mod_sflow
** =========
**
**  A binary, random-sampling Apache module designed for:
**       lightweight,
**        centralized,
**         continuous,
**          real-time monitoring of very large and very busy web farms.
**
**
**  For details on compiling, installing and running this module, see the
**  README file that came with the download.
**
**  design
**  ======
**  In order to report the samples and counters from a single sFlow agent
**  with a single sub-agent, the challenge is to bring the data together
**  from the various child processes (and threads within them) that may
**  be handling HTTP requests.
**
**  The post_config hook forks a separate process and open a pipe
**  to it.  This process runs the "master" sFlow agent that will actually
**  read the sFlow configuration and send UDP datagrams to the collector.
**
**  A small shared-memory segment is created too.  Each child process that
**  Apache subsequently forks will inherit handles for both the pipe and the
**  shared memory.
**
**  The pipe is used by each child to send samples to the master,  and the
**  shared memory is used by the master to pass configuration changes to
**  the child processes.
**
**  Each child process uses the sFlow API to create his own private "child"
**  sFlow agent,  since that allows him to take advantage of the code for
**  random sampling and XDR encoding.  (We have to serialize the data onto the
**  pipe anyway so it makes sense to use the XDR encoding and take advantage
**  of the library code to do that).  The "master" agent can simply copy
**  the pre-encoded samples directly into the output packet.
**
**  mutual-exclusion
**  ================
**  Using a pipe here for the many-to-one child-to-master communication was
**  convenient because writing to the pipe also provides mutual-exclusion
**  between the different child process (since the messages are less that 4096
**  bytes the write() calls are guaranteed atomic).  To allow this module to
**  work in servers with MPM=worker (as well as MPM=prefork) an additional mutex
**  was used in each child process.  This allows multiple worker-threads to
**  share the same "child" sFlow agent.
**
*/ 

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

#include "sys/syscall.h" /* just for gettid() */
#define MYGETTID (pid_t)syscall(SYS_gettid)

#include "apr.h"
#include "apr_lib.h"
#include "apr_strings.h"
#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "http_log.h"
#include "ap_config.h"
#include "ap_mpm.h"
#include "sflow_api.h"

/* #include <stdbool.h> */
#define true 1
#define false 0
typedef u_int8_t bool;

/*_________________---------------------------__________________
  _________________   module config data      __________________
  -----------------___________________________------------------
*/

#define MOD_SFLOW_USERDATA_KEY "mod-sflow"
module AP_MODULE_DECLARE_DATA sflow_module;

#define GET_CONFIG_DATA(s) ap_get_module_config((s)->module_config, &sflow_module)

/*_________________---------------------------__________________
  _________________   config parsing defs     __________________
  -----------------___________________________------------------
*/

#define SFWB_DEFAULT_CONFIGFILE "/etc/hsflowd.auto"
#define SFWB_SEPARATORS " \t\r\n="
#define SFWB_QUOTES "'\" \t\r\n"
/* SFWB_MAX LINE LEN must be enough to hold the whole list of targets */
#define SFWB_MAX_LINELEN 1024
#define SFWB_MAX_COLLECTORS 10
#define SFWB_CONFIG_CHECK_S 10

/*_________________---------------------------__________________
  _________________   child sFlow defs        __________________
  -----------------___________________________------------------
*/

#define SFWB_CHILD_TICK_US 2000000

/*_________________---------------------------__________________
  _________________   unknown output defs     __________________
  -----------------___________________________------------------
*/

#define SFLOW_DURATION_UNKNOWN 0
#define SFLOW_TOKENS_UNKNOWN 0

/*_________________---------------------------__________________
  _________________   structure definitions   __________________
  -----------------___________________________------------------
*/

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

/*_________________---------------------------__________________
  _________________   forward declarations    __________________
  -----------------___________________________------------------
*/

static void sflow_init(SFWB *sm);

/*_________________---------------------------__________________
  _________________      mutex utils          __________________
  -----------------___________________________------------------
*/

static bool lockOrDie(pthread_mutex_t *sem) {
    ap_assert(sem == NULL || pthread_mutex_lock(sem) == 0);
    return true;
}

static bool releaseOrDie(pthread_mutex_t *sem) {
    ap_assert(sem == NULL || pthread_mutex_unlock(sem) == 0);
    return true;
}

/*_________________---------------------------__________________
  _________________   alloc in shared mem     __________________
  -----------------___________________________------------------
*/

#if 0 /* not using this now */
static void *sfwb_shared_mem_calloc(SFWB *sm, size_t bytes) {

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "sfwb_shared_mem_calloc - used=%u total=%u",
                 sm->shared_bytes_used,
                 sm->shared_bytes_total);

    size_t roundedup = (((bytes << 4) + 1) >> 4); /* round to 128-bit boundary */
    ap_assert((sm->shared_bytes_used + roundedup) < sm->shared_bytes_total);
    void *ptr = sm->shared_mem_base + sm->shared_bytes_used;
    memset(ptr, 0, roundedup);
    sm->shared_bytes_used += roundedup;
    return ptr;
}
#endif

/*_________________---------------------------__________________
  _________________  master agent callbacks   __________________
  -----------------___________________________------------------
*/

static void *sfwb_cb_alloc(void *magic, SFLAgent *agent, size_t bytes)
{
    SFWB *sm = (SFWB *)magic;
    return apr_pcalloc(sm->masterPool, bytes);
}

static int sfwb_cb_free(void *magic, SFLAgent *agent, void *obj)
{
    /* do nothing - we'll free the whole sub-pool when we are ready */
    return 0;
}

static void sfwb_cb_error(void *magic, SFLAgent *agent, char *msg)
{
    ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "sFlow agent error: %s", msg);
}

static void sfwb_cb_counters(void *magic, SFLPoller *poller, SFL_COUNTERS_SAMPLE_TYPE *cs)
{
    SFWB *sm = (SFWB *)poller->magic;
    {
        
        if(!sm->configOK) {
            /* config is disabled */
            return;
        }
        
        if(sm->config.polling_secs == 0) {
            /* polling is off */
            return;
        }

        /* per-child counters have been accumulated into this shared-memory block, so we can just submit it */
        SFLADD_ELEMENT(cs, &sm->http_counters);
        sfl_poller_writeCountersSample(poller, cs);

    }
}

static void sfwb_cb_sendPkt(void *magic, SFLAgent *agent, SFLReceiver *receiver, u_char *pkt, uint32_t pktLen)
{
    SFWB *sm = (SFWB *)magic;
    size_t socklen = 0;
    int fd = 0;
    int c = 0;
    if(!sm->configOK) {
        /* config is disabled */
        return;
    }

    for(c = 0; c < sm->config.num_collectors; c++) {
        SFWBCollector *coll = &sm->config.collectors[c];
        switch(coll->addr.type) {
        case SFLADDRESSTYPE_UNDEFINED:
            /* skip over it if the forward lookup failed */
            break;
        case SFLADDRESSTYPE_IP_V4:
            {
                struct sockaddr_in *sa = (struct sockaddr_in *)&(coll->sa);
                socklen = sizeof(struct sockaddr_in);
                sa->sin_family = AF_INET;
                sa->sin_port = htons(coll->port);
                fd = sm->socket4;
            }
            break;
        case SFLADDRESSTYPE_IP_V6:
            {
                struct sockaddr_in6 *sa6 = (struct sockaddr_in6 *)&(coll->sa);
                socklen = sizeof(struct sockaddr_in6);
                sa6->sin6_family = AF_INET6;
                sa6->sin6_port = htons(coll->port);
                fd = sm->socket6;
            }
            break;
        }
        
        if(socklen && fd > 0) {
            int result = sendto(fd,
                                pkt,
                                pktLen,
                                0,
                                (struct sockaddr *)&coll->sa,
                                socklen);
            if(result == -1 && errno != EINTR) {
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "socket sendto error");
            }
            if(result == 0) {
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "socket sendto returned 0");
            }
        }
    }
}

/*_________________---------------------------__________________
  _________________   ipv4MappedAddress       __________________
  -----------------___________________________------------------
*/

static bool ipv4MappedAddress(SFLIPv6 *ipv6addr, SFLIPv4 *ip4addr) {
    static char mapped_prefix[] = { 0,0,0,0,0,0,0,0,0,0,0xFF,0xFF };
    static char compat_prefix[] = { 0,0,0,0,0,0,0,0,0,0,0,0 };
    if(!memcmp(ipv6addr->addr, mapped_prefix, 12) ||
       !memcmp(ipv6addr->addr, compat_prefix, 12)) {
        memcpy(ip4addr, ipv6addr->addr + 12, 4);
        return true;
    }
    return false;
}

/*_________________---------------------------__________________
  _________________   sflow_sample_http       __________________
  -----------------___________________________------------------
*/

static void sflow_sample_http(SFLSampler *sampler, struct conn_rec *connection, SFLHTTP_method method, int proto_num, const char *uri, size_t urilen, const char *host, size_t hostlen, const char *referrer, size_t referrerlen, const char *useragent, size_t useragentlen, const char *authuser, size_t authuserlen, const char *mimetype, size_t mimetypelen, uint64_t bytes, uint32_t duration_uS, uint32_t status)
{
    
    SFL_FLOW_SAMPLE_TYPE fs = { 0 };
        
    /* indicate that I am the server by setting the
       destination interface to 0x3FFFFFFF=="internal"
       and leaving the source interface as 0=="unknown" */
    fs.output = 0x3FFFFFFF;
        
    SFLFlow_sample_element httpElem = { 0 };
    httpElem.tag = SFLFLOW_HTTP;
    httpElem.flowType.http.method = method;
    httpElem.flowType.http.protocol = proto_num;
    httpElem.flowType.http.uri.str = uri;
    httpElem.flowType.http.uri.len = (uri ? urilen : 0);
    httpElem.flowType.http.host.str = host;
    httpElem.flowType.http.host.len = (host ? hostlen : 0);
    httpElem.flowType.http.referrer.str = referrer;
    httpElem.flowType.http.referrer.len = (referrer ? referrerlen : 0);
    httpElem.flowType.http.useragent.str = useragent;
    httpElem.flowType.http.useragent.len = (useragent ? useragentlen : 0);
    httpElem.flowType.http.authuser.str = authuser;
    httpElem.flowType.http.authuser.len = (authuser ? authuserlen : 0);
    httpElem.flowType.http.mimetype.str = mimetype;
    httpElem.flowType.http.mimetype.len = (mimetype ? mimetypelen : 0);
    httpElem.flowType.http.bytes = bytes;
    httpElem.flowType.http.uS = duration_uS;
    httpElem.flowType.http.status = status;
    SFLADD_ELEMENT(&fs, &httpElem);
    
    SFLFlow_sample_element socElem = { 0 };
    
    if(connection) {
        /* add a socket structure */
        apr_sockaddr_t *localsoc = connection->local_addr;
        apr_sockaddr_t *peersoc = connection->remote_addr;

        if(localsoc && peersoc) {
            /* two possibilities here... */
            struct sockaddr_in *rsoc4 = &peersoc->sa.sin;
            struct sockaddr_in6 *rsoc6 = &peersoc->sa.sin6;
            
            if(peersoc->ipaddr_len == 4 &&
               peersoc->family == AF_INET &&
               rsoc4->sin_family == AF_INET) {
                struct sockaddr_in *lsoc4 = &localsoc->sa.sin;
                socElem.tag = SFLFLOW_EX_SOCKET4;
                socElem.flowType.socket4.protocol = 6;
                socElem.flowType.socket4.local_ip.addr = lsoc4->sin_addr.s_addr;
                socElem.flowType.socket4.remote_ip.addr = rsoc4->sin_addr.s_addr;
                socElem.flowType.socket4.local_port = ntohs(lsoc4->sin_port);
                socElem.flowType.socket4.remote_port = ntohs(rsoc4->sin_port);
            }
            else if(peersoc->ipaddr_len == 16 &&
                    peersoc->family == AF_INET6 &&
                    rsoc6->sin6_family == AF_INET6) {
                struct sockaddr_in6 *lsoc6 = &localsoc->sa.sin6;
                /* may still decide to export it as an IPv4 connection
                   if the addresses are really IPv4 addresses */
                SFLIPv4 local_ip4addr, remote_ip4addr;
                if(ipv4MappedAddress((SFLIPv6 *)&lsoc6->sin6_addr, &local_ip4addr) &&
                   ipv4MappedAddress((SFLIPv6 *)&rsoc6->sin6_addr, &remote_ip4addr)) {
                    socElem.tag = SFLFLOW_EX_SOCKET4;
                    socElem.flowType.socket4.protocol = 6;
                    socElem.flowType.socket4.local_ip.addr = local_ip4addr.addr;
                    socElem.flowType.socket4.remote_ip.addr = remote_ip4addr.addr;
                    socElem.flowType.socket4.local_port = ntohs(lsoc6->sin6_port);
                    socElem.flowType.socket4.remote_port = ntohs(rsoc6->sin6_port);
                }
                else {
                    socElem.tag = SFLFLOW_EX_SOCKET6;
                    socElem.flowType.socket6.protocol = 6;
                    memcpy(socElem.flowType.socket6.local_ip.addr, lsoc6->sin6_addr.s6_addr, 16);
                    memcpy(socElem.flowType.socket6.remote_ip.addr, rsoc6->sin6_addr.s6_addr, 16);
                    socElem.flowType.socket6.local_port = ntohs(lsoc6->sin6_port);
                    socElem.flowType.socket6.remote_port = ntohs(rsoc6->sin6_port);
                }
            }
            
            if(socElem.tag) {
                SFLADD_ELEMENT(&fs, &socElem);
            }
            else {
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "unexpected socket length or address family");
            }
        }
    }
    
    sfl_sampler_writeFlowSample(sampler, &fs);
}

/*_________________---------------------------__________________
  _________________   address lookup          __________________
  -----------------___________________________------------------
*/

static bool sfwb_lookupAddress(char *name, struct sockaddr *sa, SFLAddress *addr, int family)
{
    struct addrinfo *info = NULL;
    struct addrinfo hints = { 0 };
    hints.ai_socktype = SOCK_DGRAM; /* constrain this so we don't get lots of answers */
    hints.ai_family = family; /* PF_INET, PF_INET6 or 0 */
    int err = getaddrinfo(name, NULL, &hints, &info);
    if(err) {
        switch(err) {
        case EAI_NONAME: break;
        case EAI_NODATA: break;
        case EAI_AGAIN: break; /* loop and try again? */
        default: ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "getaddrinfo() error"); break;
        }
        return false;
    }
    
    if(info == NULL) return false;
    
    if(info->ai_addr) {
        /* answer is now in info - a linked list of answers with sockaddr values. */
        /* extract the address we want from the first one. */
        switch(info->ai_family) {
        case PF_INET:
            {
                struct sockaddr_in *ipsoc = (struct sockaddr_in *)info->ai_addr;
                addr->type = SFLADDRESSTYPE_IP_V4;
                addr->address.ip_v4.addr = ipsoc->sin_addr.s_addr;
                if(sa) memcpy(sa, info->ai_addr, info->ai_addrlen);
            }
            break;
        case PF_INET6:
            {
                struct sockaddr_in6 *ip6soc = (struct sockaddr_in6 *)info->ai_addr;
                addr->type = SFLADDRESSTYPE_IP_V6;
                memcpy(&addr->address.ip_v6, &ip6soc->sin6_addr, 16);
                if(sa) memcpy(sa, info->ai_addr, info->ai_addrlen);
            }
            break;
        default:
            ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "getaddrinfo(): unexpected address family: %d", info->ai_family);
            return false;
            break;
        }
    }
    /* free the dynamically allocated data before returning */
    freeaddrinfo(info);
    return true;
}

/*_________________---------------------------__________________
  _________________   config file parsing     __________________
  -----------------___________________________------------------

read or re-read the sFlow config
*/

static bool sfwb_syntaxOK(SFWBConfig *cfg, uint32_t line, uint32_t tokc, uint32_t tokcMin, uint32_t tokcMax, char *syntax) {
    if(tokc < tokcMin || tokc > tokcMax) {
        cfg->error = true;
        ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "syntax error on line %u: expected %s", line, syntax);
        return false;
    }
    return true;
}

static void sfwb_syntaxError(SFWBConfig *cfg, uint32_t line, char *msg) {
    cfg->error = true;
    ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "syntax error on line %u: %s", line, msg);
}    

static SFWBConfig *sfwb_readConfig(SFWB *sm)
{
    uint32_t rev_start = 0;
    uint32_t rev_end = 0;

    /* avoid heap allocation. Use pre-allocated object */
    SFWBConfig *config = &sm->newConfig;
    memset(config, 0, sizeof(SFWBConfig));

    FILE *cfg = NULL;
    if((cfg = fopen(sm->configFile, "r")) == NULL) {
        ap_log_error(APLOG_MARK, APLOG_INFO, 0, NULL, "cannot open config file %s : %s", sm->configFile, strerror(errno));
        return NULL;
    }
    char line[SFWB_MAX_LINELEN+1];
    uint32_t lineNo = 0;
    char *tokv[5];
    uint32_t tokc;
    while(fgets(line, SFWB_MAX_LINELEN, cfg)) {
        int i;
        char *p = line;
        lineNo++;
        /* comments start with '#' */
        p[strcspn(p, "#")] = '\0';
        /* 1 var and up to 3 value tokens, so detect up to 5 tokens overall */
        /* so we know if there was an extra one that should be flagged as a */
        /* syntax error. */
        tokc = 0;
        for(i = 0; i < 5; i++) {
            size_t len;
            p += strspn(p, SFWB_SEPARATORS);
            if((len = strcspn(p, SFWB_SEPARATORS)) == 0) break;
            tokv[tokc++] = p;
            p += len;
            if(*p != '\0') *p++ = '\0';
        }

        if(tokc >=2) {
            ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "line=%s tokc=%u tokv=<%s> <%s> <%s>",
                         line,
                         tokc,
                         tokc > 0 ? tokv[0] : "",
                         tokc > 1 ? tokv[1] : "",
                         tokc > 2 ? tokv[2] : "");
        }

        if(tokc) {
            if(strcasecmp(tokv[0], "rev_start") == 0
               && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "rev_start=<int>")) {
                rev_start = strtol(tokv[1], NULL, 0);
            }
            else if(strcasecmp(tokv[0], "rev_end") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "rev_end=<int>")) {
                rev_end = strtol(tokv[1], NULL, 0);
            }
            else if(strcasecmp(tokv[0], "sampling") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "sampling=<int>")) {
                config->sampling_n = strtol(tokv[1], NULL, 0);
            }
            else if(strcasecmp(tokv[0], "polling") == 0 
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "polling=<int>")) {
                config->polling_secs = strtol(tokv[1], NULL, 0);
            }
            else if(strcasecmp(tokv[0], "agentIP") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "agentIP=<IP address>|<IPv6 address>")) {
                if(sfwb_lookupAddress(tokv[1],
                                      NULL,
                                      &config->agentIP,
                                      0) == false) {
                    sfwb_syntaxError(config, lineNo, "agent address lookup failed");
                }
            }
            else if(strcasecmp(tokv[0], "collector") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 4, "collector=<IP address>[ <port>[ <priority>]]")) {
                if(config->num_collectors < SFWB_MAX_COLLECTORS) {
                    uint32_t i = config->num_collectors++;
                    if(sfwb_lookupAddress(tokv[1],
                                          &config->collectors[i].sa,
                                          &config->collectors[i].addr,
                                          0) == false) {
                        sfwb_syntaxError(config, lineNo, "collector address lookup failed");
                    }
                    config->collectors[i].port = tokc >= 3 ? strtol(tokv[2], NULL, 0) : 6343;
                    config->collectors[i].priority = tokc >= 4 ? strtol(tokv[3], NULL, 0) : 0;
                }
                else {
                    sfwb_syntaxError(config, lineNo, "exceeded max collectors");
                }
            }
            else if(strcasecmp(tokv[0], "header") == 0) { /* ignore */ }
            else if(strcasecmp(tokv[0], "agent") == 0) { /* ignore */ }
            else {
                sfwb_syntaxError(config, lineNo, "unknown var=value setting");
            }
        }
    }
    fclose(cfg);
    
    /* sanity checks... */
    
    if(config->agentIP.type == SFLADDRESSTYPE_UNDEFINED) {
        sfwb_syntaxError(config, 0, "agentIP=<IP address>|<IPv6 address>");
    }
    
    if((rev_start == rev_end) && !config->error) {
        return config;
    }
    else {
        return NULL;
    }
}

/*_________________---------------------------__________________
  _________________        apply config       __________________
  -----------------___________________________------------------
*/

static void sfwb_apply_config(SFWB *sm, SFWBConfig *config)
{
    if(config == NULL && !sm->configOK) {
        /* no change required */
        return;
    }

    if(config) {
        sm->config = *config; /* structure copy */
        sm->configOK = true;
    }
    else {
        sm->configOK = false;
    }

    if(sm->configOK) {
        sflow_init(sm);
    }
}

/*_________________---------------------------__________________
  _________________      1 second tick        __________________
  -----------------___________________________------------------
*/
        
void sflow_tick(SFWB *sm) {
    if(!sm->enabled) return;

    if(--sm->configCountDown <= 0) {
        sm->configCountDown = SFWB_CONFIG_CHECK_S;
        ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "checking for config file change <%s>", sm->configFile);
        struct stat statBuf;
        if(stat(sm->configFile, &statBuf) != 0) {
            /* config file missing */
            sfwb_apply_config(sm, NULL);
        }
        else if(statBuf.st_mtime != sm->configFile_modTime) {
            /* config file modified */
            ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "config file changed <%s>", sm->configFile);
            SFWBConfig *newConfig = sfwb_readConfig(sm);
            if(newConfig) {
                /* config OK - apply it */
                ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "config file OK <%s>", sm->configFile);
                sfwb_apply_config(sm, newConfig);
                sm->configFile_modTime = statBuf.st_mtime;
            }
            else {
                /* bad config - ignore it (may be in transition) */
                ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "config file parse failed <%s>", sm->configFile);
            }
        }
    }
    
    if(sm->agent && sm->configOK) {
        sfl_agent_tick(sm->agent, sm->currentTime);
    }
}

/*_________________---------------------------__________________
  _________________  master sflow agent init  __________________
  -----------------___________________________------------------
*/

static void sflow_init(SFWB *sm)
{

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "in sflow_init: sFlow=%p pid=%u config=%p",
                 (void *)sm,
                 getpid(),
                 (void *)&sm->config);

    if(sm->configFile == NULL) {
        sm->configFile = SFWB_DEFAULT_CONFIGFILE;
    }

    if(!sm->configOK) return;

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "in sflow_init: building sFlow agent");

    {
        /* create/re-create the agent */
        if(sm->agent) {
            sfl_agent_release(sm->agent);
            apr_pool_clear(sm->masterPool);
        }

        sm->agent = (SFLAgent *)apr_pcalloc(sm->masterPool, sizeof(SFLAgent));

        /* open the sockets - one for v4 and another for v6  - should these
         * be registered with the pool? $$$ */
        if(sm->socket4 <= 0) {
            if((sm->socket4 = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "IPv4 send socket open failed");
        }
        if(sm->socket6 <= 0) {
            if((sm->socket6 = socket(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)) == -1)
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "IPv6 send socket open failed");
        }
        
        /* initialize the agent with it's address, bootime, callbacks etc. */
        sfl_agent_init(sm->agent,
                       &sm->config.agentIP,
                       getpid(), /* subAgentId */
                       sm->currentTime,
                       sm->currentTime,
                       sm,
                       sfwb_cb_alloc,
                       sfwb_cb_free,
                       sfwb_cb_error,
                       sfwb_cb_sendPkt);
        
        /* add a receiver */
        sm->receiver = sfl_agent_addReceiver(sm->agent);
        sfl_receiver_set_sFlowRcvrOwner(sm->receiver, "httpd sFlow Probe");
        sfl_receiver_set_sFlowRcvrTimeout(sm->receiver, 0xFFFFFFFF);
        
        /* no need to configure the receiver further, because we are */
        /* using the sendPkt callback to handle the forwarding ourselves. */
        
        /* add a <logicalEntity> datasource to represent this application instance */
        SFLDataSource_instance dsi;
        /* ds_class = <logicalEntity>, ds_index = 65537, ds_instance = 0 */
        /* $$$ should learn the ds_index from the config file */
        SFL_DS_SET(dsi, SFL_DSCLASS_LOGICAL_ENTITY, 65537, 0);
        
        /* add a poller for the counters */
        sm->poller = sfl_agent_addPoller(sm->agent, &dsi, sm, sfwb_cb_counters);
        sfl_poller_set_sFlowCpInterval(sm->poller, sm->config.polling_secs);
        sfl_poller_set_sFlowCpReceiver(sm->poller, 1 /* receiver index == 1 */);
        
        /* add a sampler for the sampled operations */
        sm->sampler = sfl_agent_addSampler(sm->agent, &dsi);
        sfl_sampler_set_sFlowFsPacketSamplingRate(sm->sampler, sm->config.sampling_n);
        sfl_sampler_set_sFlowFsReceiver(sm->sampler, 1 /* receiver index == 1 */);
        
        if(sm->config.sampling_n) {
            /* IPC to the child processes */
            SFWBShared *shared = (SFWBShared *)sm->shared_mem_base;
            shared->sflow_skip1 = sm->config.sampling_n;
            shared->sflow_skip2 = sm->config.sampling_n;

        }
    }
}


/*_________________---------------------------__________________
  _________________   sFlow master process    __________________
  -----------------___________________________------------------
*/

static int run_sflow_master(apr_pool_t *p, server_rec *s, SFWB *sm)
{
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "run_sflow_master - pid=%u\n", getpid());

    apr_file_pipe_timeout_set(sm->pipe_read, 1000000); /* uS */
    for(;;) {
        sm->currentTime = apr_time_sec(apr_time_now());
        sflow_tick(sm);
        uint32_t msg[4096>>2];
        /* just read the length and type first */
        size_t hdrBytes = 12;
        size_t hdrBytesRead = 0;
        apr_status_t rv = apr_file_read_full(sm->pipe_read, msg, hdrBytes, &hdrBytesRead);
        if(rv == APR_SUCCESS && hdrBytesRead != 0) {
            ap_assert(hdrBytesRead == hdrBytes);
            /* now read the rest */
            size_t msgBytes = msg[0];
            uint32_t msgType = msg[1];
            uint32_t msgId = msg[2];
            ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "in sflow_master - msgType/id = %u/%u msgBytes=%u\n",
                     msgType,
                     msgId,
                     msgBytes);
            ap_assert(msgType == SFLCOUNTERS_SAMPLE || msgType == SFLFLOW_SAMPLE);
            ap_assert(msgBytes <= 4096);
            size_t bodyBytes = msgBytes - hdrBytes;
            size_t bodyBytesRead = 0;
            rv = apr_file_read_full(sm->pipe_read, msg, bodyBytes, &bodyBytesRead);
            ap_assert(rv == APR_SUCCESS);
            ap_assert(bodyBytesRead == bodyBytes);
            ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "in sflow_master - bodyBytes read=%u\n", bodyBytesRead);
            /* we may not have initialized the agent yet,  so the first few samples may end up being ignored */
            if(sm->sampler) {
                uint32_t *datap = msg;
                if(msgType == SFLCOUNTERS_SAMPLE && msgId == SFLCOUNTERS_HTTP) {
                    /* counter block */
                    SFLHTTP_counters c;
                    memcpy(&c, datap, sizeof(c));
                    /* accumulate into my total */
                    sm->http_counters.counterBlock.http.method_option_count += c.method_option_count;
                    sm->http_counters.counterBlock.http.method_get_count += c.method_get_count;
                    sm->http_counters.counterBlock.http.method_head_count += c.method_head_count;
                    sm->http_counters.counterBlock.http.method_post_count += c.method_post_count;
                    sm->http_counters.counterBlock.http.method_put_count += c.method_put_count;
                    sm->http_counters.counterBlock.http.method_delete_count += c.method_delete_count;
                    sm->http_counters.counterBlock.http.method_trace_count += c.method_trace_count;
                    sm->http_counters.counterBlock.http.method_connect_count += c.method_connect_count;
                    sm->http_counters.counterBlock.http.method_other_count += c.method_other_count;
                    sm->http_counters.counterBlock.http.status_1XX_count += c.status_1XX_count;
                    sm->http_counters.counterBlock.http.status_2XX_count += c.status_2XX_count;
                    sm->http_counters.counterBlock.http.status_3XX_count += c.status_3XX_count;
                    sm->http_counters.counterBlock.http.status_4XX_count += c.status_4XX_count;
                    sm->http_counters.counterBlock.http.status_5XX_count += c.status_5XX_count;
                    sm->http_counters.counterBlock.http.status_other_count += c.status_other_count;
                }
                else if(msgType == SFLFLOW_SAMPLE && msgId == SFLFLOW_HTTP) {
                    sm->sampler->samplePool += *datap++;
                    sm->sampler->dropEvents += *datap++;
                    /* next we have a flow sample that we can encode straight into the output,  but we have to put it */
                    /* through our sampler object so that we get the right sequence numbers, pools and data-source ids. */
                    uint32_t sampleBytes = (msg + (bodyBytesRead>>2) - datap) << 2;
                    sfl_sampler_writeEncodedFlowSample(sm->sampler, (char *)datap, sampleBytes);
                }
            }
        }
    }
    return 0;
}


/*_________________---------------------------__________________
  _________________   start master process    __________________
  -----------------___________________________------------------
*/

static int start_sflow_master(apr_pool_t *p, server_rec *s, SFWB *sm) {
    apr_status_t status;

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "start_sflow_master - pid=%u\n", getpid());

    /* create the pipe that the child processes will use to send samples to the master */
    /* wanted to use apr_file_pipe_create_ex(...APR_FULL_NONBLOCK..) but it seems to be a new addition */
    if((status=apr_file_pipe_create(&sm->pipe_read, &sm->pipe_write, p)) != OK) {
        ap_log_error(APLOG_MARK, APLOG_ERR, status, s, "apr_file_pipe_create() failed");
        return HTTP_INTERNAL_SERVER_ERROR;
    }

    /* create anonymous shared memory for the sFlow agent structures and packet buffer */
    sm->shared_bytes_total = 4096; /* sizeof(SFWBShared); */
    if((status = apr_shm_create(&sm->shared_mem, sm->shared_bytes_total, NULL, p)) != OK) {
        ap_log_error(APLOG_MARK, APLOG_ERR, status, s, "apr_shm_create() failed");
        /* may return ENOTIMPL if anon shared mem not supported,  in which case we */
        /* should try again with a filename. $$$ */
        return HTTP_INTERNAL_SERVER_ERROR;
    }
    sm->shared_mem_base = apr_shm_baseaddr_get(sm->shared_mem); /* each child must call again */

    sm->http_counters.tag = SFLCOUNTERS_HTTP;

    sm->sFlowProc = apr_palloc(p, sizeof(apr_proc_t));
    ap_assert(sm->sFlowProc);
    switch(status = apr_proc_fork(sm->sFlowProc, p)) {
    case APR_INCHILD:
        /* close the write-end of the inherited pipe */
        apr_file_close(sm->pipe_write);
        /* and run the master */
        run_sflow_master(p, s, sm);
        exit(1);
        break;
    case APR_INPARENT:
        /* close the read end of the pipe */
        apr_file_close(sm->pipe_read);
        /* make sure apache knows to kill this process too if it is cleaning up */
        apr_pool_note_subprocess(p, sm->sFlowProc, APR_KILL_AFTER_TIMEOUT);
        break;
    default:
        ap_log_error(APLOG_MARK, APLOG_ERR, status, s, "apr_fork() failed");
        return HTTP_INTERNAL_SERVER_ERROR;
        break;
    }
    return OK;
}

/*_________________---------------------------__________________
  _________________   create_sflow_config     __________________
  -----------------___________________________------------------
*/

static void *create_sflow_config(apr_pool_t *p, server_rec *s)
{
    int rc;
    SFWB *sm = apr_pcalloc(p, sizeof(SFWB));
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "create_sflow_config - pid=%u,tid=%u\n", getpid(), MYGETTID);
    sm->configFile = SFWB_DEFAULT_CONFIGFILE;
    sm->enabled = true; /* could be controlled by config cmd (see below) */

    /* a pool to use for the agent so we can recycle the memory easily on a config change */
    if((rc = apr_pool_create(&sm->masterPool, p)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, s, "create_sflow_config: error creating sub-pool");
    }
    return sm;
}

/*_________________---------------------------__________________
  _________________   sflow_post_config       __________________
  -----------------___________________________------------------
*/

static int sflow_post_config(apr_pool_t *p, apr_pool_t *plog, apr_pool_t *ptemp, server_rec *s)
{
    void *flag;
    SFWB *sm = GET_CONFIG_DATA(s);
    int rc;
    int mpm_threaded = 0;

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_post_config - pid=%u,tid=%u\n", getpid(),MYGETTID);

    /* All post_config hooks are called twice, we're only interested in the second call. */
    apr_pool_userdata_get(&flag, MOD_SFLOW_USERDATA_KEY, s->process->pool);
    if (!flag) {
        apr_pool_userdata_set((void*) 1, MOD_SFLOW_USERDATA_KEY, apr_pool_cleanup_null, s->process->pool);
        return OK;
    }

    if((rc = ap_mpm_query(AP_MPMQ_IS_THREADED, &mpm_threaded)) == APR_SUCCESS) {
        /* We could perhaps use this information to decided whether to create the mutex in each child */
        ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_post_config - threaded=%u\n", mpm_threaded);
    }
    else {
        ap_log_error(APLOG_MARK, APLOG_DEBUG, rc, s, "sflow_post_config - ap_mpm_query(AP_MPMQ_IS_THREADED) failed\n");
    }


    if(sm && sm->enabled && sm->sFlowProc == NULL) {
        start_sflow_master(p, s, sm);
    }
    return OK;
}

/*_________________---------------------------__________________
  _________________  child agent callbacks    __________________
  -----------------___________________________------------------
*/

static void *sfwb_childcb_alloc(void *magic, SFLAgent *agent, size_t bytes)
{
    SFWB *sm = (SFWB *)magic;
    return apr_pcalloc(sm->child->childPool, bytes);
}

static int sfwb_childcb_free(void *magic, SFLAgent *agent, void *obj)
{
    /* do nothing - we'll free the whole sub-pool when we are ready */
    return 0;
}

static void sfwb_childcb_error(void *magic, SFLAgent *agent, char *msg)
{
    ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "sFlow child agent error: %s", msg);
}

/*_________________---------------------------__________________
  _________________  child agent init         __________________
  -----------------___________________________------------------
*/

static void sflow_init_child(apr_pool_t *p, server_rec *s)
{
    SFWB *sm = GET_CONFIG_DATA(s);
    if(!sm->enabled) return;
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_init_child - pid=%u,tid=%u\n", getpid(),MYGETTID);
    /* create my own private state, and hang it off the shared state */
    SFWBChild *child = (SFWBChild *)apr_pcalloc(p, sizeof(SFWBChild));
    sm->child = child;
    /* remember the config pool so the allocation callback can use it (no
       need for a sub-pool here because we don't need to recycle) */
    child->childPool = p;
    /* shared_mem base address - may be different for each child, so put in private state */
    child->shared_mem_base = apr_shm_baseaddr_get(sm->shared_mem);

    /* a mutex to allow worker threads in the same child process to avoid tripping over each other */
    if(child->mutex == NULL) {
        child->mutex = (pthread_mutex_t*)apr_pcalloc(p, sizeof(pthread_mutex_t));
        pthread_mutex_init(child->mutex, NULL);
    }

    /* create my own sFlow agent+sampler+receiver just so I can use it to encode XDR messages */
    /* before sending them down the pipe */
    child->agent = (SFLAgent *)apr_pcalloc(p, sizeof(SFLAgent));
    SFLAddress myIP = { 0 }; /* blank address */
    sfl_agent_init(child->agent,
                   &myIP,
                   getpid(), /* subAgentId */
                   sm->currentTime,
                   sm->currentTime,
                   sm,
                   sfwb_childcb_alloc,
                   sfwb_childcb_free,
                   sfwb_childcb_error,
                   NULL);

    child->receiver = sfl_agent_addReceiver(child->agent);
    sfl_receiver_set_sFlowRcvrOwner(child->receiver, "httpd sFlow Probe - child");
    sfl_receiver_set_sFlowRcvrTimeout(child->receiver, 0xFFFFFFFF);
    SFLDataSource_instance dsi;
    memset(&dsi, 0, sizeof(dsi)); /* will be ignored anyway */
    child->sampler = sfl_agent_addSampler(child->agent, &dsi);
    sfl_sampler_set_sFlowFsReceiver(child->sampler, 1 /* receiver index*/);
    /* seed the random number generator */
    sfl_random_init(getpid());
    /* we'll pick up the sampling_rate later. Don't want to insist
     * on it being present at startup - don't want to delay the
     * startup if we can avoid it.  Just set it to 0 so we check for
     * it. Otherwise it will start out as the default (400) */
    sfl_sampler_set_sFlowFsPacketSamplingRate(child->sampler, 0);
}

/*_________________---------------------------__________________
  _________________  read from shared mem     __________________
  -----------------___________________________------------------
*/

static int read_shared_sampling_n(SFWBChild *child)
{
    SFWBShared *shared = (SFWBShared *)child->shared_mem_base;
    /* read it twice to avoid requiring a lock (does this work?) */
    uint32_t sflow_skip1 = shared->sflow_skip1;
    uint32_t sflow_skip2 = shared->sflow_skip2;
    /* if it's not stable, just use the value we had before */
    return (sflow_skip1 == sflow_skip2) ? sflow_skip1 : -1;
}

/*_________________---------------------------__________________
  _________________   check sampling rate     __________________
  -----------------___________________________------------------
*/

static void sflow_set_random_skip(SFWBChild *child)
{
    int n = read_shared_sampling_n(child);
    if(n >= 0) {
        /* got a valid setting */
        if(n != sfl_sampler_get_sFlowFsPacketSamplingRate(child->sampler)) {
            /* it has changed */
            sfl_sampler_set_sFlowFsPacketSamplingRate(child->sampler, n);
        }
    }
}

/*_________________---------------------------__________________
  _________________   method numbers          __________________
  -----------------___________________________------------------
*/

static SFLHTTP_method methodNumberLookup(int method)
{
    /* SFHTTP_HEAD is reported when request_req has the "header_only" flag
       set, otherwise we map from method number to sFlow method number here. */
    switch(method) {
    case M_GET: return SFHTTP_GET;
    case M_PUT: return SFHTTP_PUT;
    case M_POST: return SFHTTP_POST;
    case M_DELETE: return SFHTTP_DELETE;
    case M_CONNECT: return SFHTTP_CONNECT;
    case M_OPTIONS: return SFHTTP_OPTIONS;
    case M_TRACE: return SFHTTP_TRACE;
    case M_PATCH:
    case M_PROPFIND:
    case M_PROPPATCH:
    case M_MKCOL:
    case M_COPY:
    case M_MOVE:
    case M_LOCK:
    case M_UNLOCK:
    case M_VERSION_CONTROL:
    case M_CHECKOUT:
    case M_UNCHECKOUT:
    case M_CHECKIN:
    case M_UPDATE:
    case M_LABEL:
    case M_REPORT:
    case M_MKWORKSPACE:
    case M_MKACTIVITY:
    case M_BASELINE_CONTROL:
    case M_MERGE:
    case M_INVALID:
    default: return SFHTTP_OTHER;
    }
}

/*_________________-----------------------------__________________
  _________________ sflow_multi_log_transaction __________________
  -----------------_____________________________------------------
*/

static int my_strlen(const char *s) { return s ? strlen(s) : 0; }

static int sflow_multi_log_transaction(request_rec *r)
{
    SFWB *sm = GET_CONFIG_DATA(r->server);
    SFWBChild *child = sm->child;
    apr_time_t now_uS = apr_time_now();
    ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "sflow_multi_log_transaction (sampler->skip=%u)\n", child->sampler->skip);
    uint32_t method = r->header_only ? SFHTTP_HEAD : methodNumberLookup(r->method_number);

    /* For now, just lock this whole step.  Most times through here we do very little anyway.
       It looks like we should switch to a probabililty-test for the sampling rather than a
       countdown, since that would make it easier to only lock the mutex when we are actually taking
       a sample.  It may also be necessary to change the counter/sample-pool accumulation so that
       the shared counters only ever increment and the delta is calculated when we send the latest
       counters.  Otherwise the clearing of the counters when we take a sample would require that
       we lock just to increment them here,  even if we use apr_atomic_inc32() to do it? */
    lockOrDie(child->mutex);

    SFLHTTP_counters *ctrs = &child->http_counters.counterBlock.http;
    switch(method) {
    case SFHTTP_HEAD: ctrs->method_head_count++; break;
    case SFHTTP_GET: ctrs->method_get_count++; break;
    case SFHTTP_PUT: ctrs->method_put_count++; break;
    case SFHTTP_POST: ctrs->method_post_count++; break;
    case SFHTTP_DELETE: ctrs->method_delete_count++; break;
    case SFHTTP_CONNECT: ctrs->method_connect_count++; break;
    case SFHTTP_OPTIONS: ctrs->method_option_count++; break;
    case SFHTTP_TRACE: ctrs->method_trace_count++; break;
    default: ctrs->method_other_count++; break;
    }
    if(r->status < 100) ctrs->status_other_count++;
    else if(r->status < 200) ctrs->status_1XX_count++;
    else if(r->status < 300) ctrs->status_2XX_count++;
    else if(r->status < 400) ctrs->status_3XX_count++;
    else if(r->status < 500) ctrs->status_4XX_count++;
    else if(r->status < 600) ctrs->status_5XX_count++;    
    else ctrs->status_other_count++;
   
    if(unlikely(sfl_sampler_get_sFlowFsPacketSamplingRate(child->sampler) == 0)) {
        /* don't have a sampling-rate setting yet. Check to see... */
        sflow_set_random_skip(child);
    }
    else if(unlikely(sfl_sampler_takeSample(child->sampler))) {
        ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "sflow take sample: r->method_number=%u\n", r->method_number);
        /* point to the start of the datagram */
        uint32_t *msg = child->receiver->sampleCollector.datap;
        /* msglen, msgType, sample pool and drops */
        sfl_receiver_put32(child->receiver, 0); /* we'll come back and fill this in later */
        sfl_receiver_put32(child->receiver, SFLFLOW_SAMPLE);
        sfl_receiver_put32(child->receiver, SFLFLOW_HTTP);
        sfl_receiver_put32(child->receiver, child->sampler->samplePool);
        sfl_receiver_put32(child->receiver, child->sampler->dropEvents);
        /* and reset so they can be accumulated by the other process */
        child->sampler->samplePool = 0;
        child->sampler->dropEvents = 0;
        /* accumulate the pktlen here too, to satisfy a sanity-check in the sflow library (receiver) */
        child->receiver->sampleCollector.pktlen += 20;

        const char *referer = apr_table_get(r->headers_in, "Referer");
        const char *useragent = apr_table_get(r->headers_in, "User-Agent");
        const char *contentType = apr_table_get(r->headers_in, "Content-Type");
        /* encode the transaction sample next */
        sflow_sample_http(child->sampler,
                          r->connection,
                          method,
                          r->proto_num,
                          r->uri, my_strlen(r->uri), /* r->the_request ? */
                          r->hostname, my_strlen(r->hostname), /* r->server->server_hostname ?*/
                          referer, my_strlen(referer),
                          useragent, my_strlen(useragent),
                          r->user, my_strlen(r->user),
                          contentType, my_strlen(contentType),
                          r->bytes_sent,
                          now_uS - r->request_time,
                          r->status);

        /* get the message bytes including the sample */
        uint32_t msgBytes = (child->receiver->sampleCollector.datap - msg) << 2;
        /* write this in as the first 32-bit word */
        *msg = msgBytes;
        /* if greater than 4096 the pipe write will not be atomic. Should never happen, */
        /* but can't risk it, since we are relying on this as the synchronization mechanism */
        if(msgBytes > 4096) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "msgBytes=%u exceeds 4096-byte limit for atomic write", msgBytes);
            /* this counts as an sFlow drop-event */
            child->sampler->dropEvents++;
        }
        else if(apr_file_write_full(sm->pipe_write, msg, msgBytes, &msgBytes) != APR_SUCCESS) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "error in apr_file_write_full()\n");
            /* this counts as an sFlow drop-event */
            child->sampler->dropEvents++;
        }
        sfl_receiver_resetSampleCollector(child->receiver);
    }

    
    if((now_uS - child->lastTickTime) > SFWB_CHILD_TICK_US) {
        child->lastTickTime = now_uS;
        ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "child tick - sending counters\n");
        /* point to the start of the datagram */
        uint32_t *msg = child->receiver->sampleCollector.datap;
        /* msglen, msgType, msgId */
        sfl_receiver_put32(child->receiver, 0); /* we'll come back and fill this in later */
        sfl_receiver_put32(child->receiver, SFLCOUNTERS_SAMPLE);
        sfl_receiver_put32(child->receiver, SFLCOUNTERS_HTTP);
        sfl_receiver_putOpaque(child->receiver, (char *)ctrs, sizeof(*ctrs));
        /* now reset my private counter block so that we only send the delta each time */
        memset(ctrs, 0, sizeof(*ctrs));
        /* get the msg bytes */
        uint32_t msgBytes = (child->receiver->sampleCollector.datap - msg) << 2;
        /* write this in as the first 32-bit word */
        *msg = msgBytes;

        /* if greater than 4096 the pipe write will not be atomic. Should never happen, */
        /* but can't risk it, since we are relying on this as the synchronization mechanism */
        if(msgBytes > 4096) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "msgBytes=%u exceeds 4096-byte limit for atomic write", msgBytes);
            /* this counts as an sFlow drop-event */
            child->sampler->dropEvents++;
        }
        else if(apr_file_write_full(sm->pipe_write, msg, msgBytes, &msgBytes) != APR_SUCCESS) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "error in apr_file_write_full()\n");
            /* this counts as an sFlow drop-event */
            child->sampler->dropEvents++;
        }
        sfl_receiver_resetSampleCollector(child->receiver);

        /* check in case the sampling-rate setting has changed. */
        sflow_set_random_skip(child);
    }

    releaseOrDie(child->mutex);

    return OK;
}

/*_________________---------------------------__________________
  _________________   sflow_register_hooks    __________________
  -----------------___________________________------------------
*/

static void sflow_register_hooks(apr_pool_t *p)
{
    ap_hook_post_config(sflow_post_config,NULL,NULL,APR_HOOK_MIDDLE);
    ap_hook_child_init(sflow_init_child,NULL,NULL,APR_HOOK_MIDDLE);
    ap_hook_log_transaction(sflow_multi_log_transaction,NULL,NULL,APR_HOOK_MIDDLE);
}

/*_________________---------------------------__________________
  _________________   Module API hooks        __________________
  -----------------___________________________------------------
*/

module AP_MODULE_DECLARE_DATA sflow_module = {
    STANDARD20_MODULE_STUFF, 
    NULL,                  /* create per-dir config structures        */
    NULL,                  /* merge  per-dir config structures        */
    create_sflow_config,   /* create per-server config structures     */
    NULL,                  /* merge  virtual-server config structures */
    NULL,                  /* table of config file commands           */
    sflow_register_hooks,  /* register hooks                          */
};

