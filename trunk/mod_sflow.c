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
**  the pre-encoded samples directly into the output datagram.
**
**  mutual-exclusion
**  ================
**  Using a pipe here for the many-to-one child-to-master communication was
**  convenient because writing to the pipe also provides mutual-exclusion
**  between the different child process (when the messages are less that PIPE_BUF
**  bytes the write() calls are guaranteed atomic).  To allow this module to
**  work in servers with MPM=worker (as well as MPM=prefork) an additional mutex
**  was used in each child process.  This allows multiple worker-threads to
**  share the same "child" sFlow agent.
**
*/ 

/* Apache Runtime Library */
#include "apr.h"

/* Apache HTTPD includes */
#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "http_log.h"

/* make sure we have a value for PIPE_BUF, the max
   size of an atomic write on a pipe. Currently Linux
   has 4096, Solaris has 5120, OSX/FreeBSD has 512.
   _POSIX_PIPE_BUF sets the minimum to be 512.  We
   are unlikely to hit even that given the current
   sFlow HTTP spec, so fall back on 512 if all else
   fails. */
#ifdef APR_HAVE_LIMITS_H
#include <limits.h>
#endif
#ifndef PIPE_BUF
#define PIPE_BUF 512
#endif

/* sFlow library */
#include "sflow_api.h"

#ifdef SFWB_DEBUG
#include "sys/syscall.h" /* just for gettid() */
#define MYGETTID (pid_t)syscall(SYS_gettid)
#include "ap_mpm.h"
#endif

/* #include <stdbool.h> */
#define true 1
#define false 0
typedef uint8_t bool;

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
    apr_sockaddr_t *sa;
    uint16_t priority;
} SFWBCollector;

typedef struct _SFWBConfig {
    int error;
    uint32_t sampling_n;
    uint32_t polling_secs;
    bool got_sampling_n_http;
    bool got_polling_secs_http;
    SFLAddress agentIP;
    uint32_t num_collectors;
    SFWBCollector collectors[SFWB_MAX_COLLECTORS];
    apr_pool_t *pool;
} SFWBConfig;


typedef struct _SFWBChild {
    apr_thread_mutex_t *mutex;
    void *shared_mem_base; /* may be a different address for each worker */
    SFLAgent *agent;
    SFLReceiver *receiver;
    SFLSampler *sampler;
    SFLCounters_sample_element http_counters;
    apr_time_t lastTickTime;
    apr_pool_t *childPool;
} SFWBChild;

typedef struct _SFWB {
#ifdef SFWB_DEBUG
    int mpm_threaded;
#endif

    /* master process */
    apr_proc_t *sFlowProc;
    apr_pool_t *masterPool;
    apr_pool_t *configPool;

    /* master config */
    time_t currentTime;
    int configCountDown;
    char *configFile;
    time_t configFile_modTime;
    SFWBConfig *config;

    /* master sFlow agent */
    apr_socket_t *socket4;
    apr_socket_t *socket6;
    SFLAgent *agent;
    SFLReceiver *receiver;
    SFLSampler *sampler;
    SFLPoller *poller;

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
    uint32_t sflow_skip;
    SFLCounters_sample_element http_counters;
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

static bool lockOrDie(apr_thread_mutex_t *sem) {
    ap_assert(sem == NULL || apr_thread_mutex_lock(sem) == 0);
    return true;
}

static bool releaseOrDie(apr_thread_mutex_t *sem) {
    ap_assert(sem == NULL || apr_thread_mutex_unlock(sem) == 0);
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
    SFWBShared *shared = (SFWBShared *)sm->shared_mem_base;
        
    if(sm->config == NULL) {
        /* config is disabled */
        return;
    }
    
    if(sm->config->polling_secs == 0) {
        /* polling is off */
        return;
    }

    /* per-child counters have been accumulated into this shared-memory block, so we can just submit it */
    SFLADD_ELEMENT(cs, &shared->http_counters);
    sfl_poller_writeCountersSample(poller, cs);
}

static void sfwb_cb_sendPkt(void *magic, SFLAgent *agent, SFLReceiver *receiver, u_char *pkt, uint32_t pktLen)
{
    SFWB *sm = (SFWB *)magic;
    apr_socket_t *soc = NULL;
    int c = 0;
    if(!sm->config) {
        /* config is disabled */
        return;
    }

    for(c = 0; c < sm->config->num_collectors; c++) {
        SFWBCollector *coll = &sm->config->collectors[c];
        if(coll->sa) {
            soc = (coll->sa->family == APR_INET6) ? sm->socket6 : sm->socket4;
            apr_size_t len = (apr_size_t)pktLen;
            int rc = apr_socket_sendto(soc, coll->sa, 0, (char *)pkt, &len);
            if(rc != APR_SUCCESS && errno != EINTR) {
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "socket sendto error");
            }
            if(len == 0) {
                ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "socket sendto transmitted 0 bytes");
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
            if(peersoc->ipaddr_len == 4 &&
               peersoc->family == APR_INET) {
                socElem.tag = SFLFLOW_EX_SOCKET4;
                socElem.flowType.socket4.protocol = 6; /* TCP */
                memcpy(&socElem.flowType.socket4.local_ip.addr, localsoc->ipaddr_ptr, 4);
                memcpy(&socElem.flowType.socket4.remote_ip.addr, peersoc->ipaddr_ptr, 4);
                socElem.flowType.socket4.local_port = ntohs(localsoc->port);
                socElem.flowType.socket4.remote_port = ntohs(peersoc->port);
            }
            else if(peersoc->ipaddr_len == 16 &&
                    peersoc->family == APR_INET6) {
                /* may still decide to export it as an IPv4 connection
                   if the addresses are really IPv4 addresses */
                SFLIPv4 local_ip4addr, remote_ip4addr;
                if(ipv4MappedAddress((SFLIPv6 *)localsoc->ipaddr_ptr, &local_ip4addr) &&
                   ipv4MappedAddress((SFLIPv6 *)peersoc->ipaddr_ptr, &remote_ip4addr)) {
                    socElem.tag = SFLFLOW_EX_SOCKET4;
                    socElem.flowType.socket4.protocol = 6; /* TCP */
                    socElem.flowType.socket4.local_ip.addr = local_ip4addr.addr;
                    socElem.flowType.socket4.remote_ip.addr = remote_ip4addr.addr;
                    socElem.flowType.socket4.local_port = ntohs(localsoc->port);
                    socElem.flowType.socket4.remote_port = ntohs(peersoc->port);
                }
                else {
                    socElem.tag = SFLFLOW_EX_SOCKET6;
                    socElem.flowType.socket6.protocol = 6; /* TCP */
                    memcpy(socElem.flowType.socket6.local_ip.addr, localsoc->ipaddr_ptr, 16);
                    memcpy(socElem.flowType.socket6.remote_ip.addr, peersoc->ipaddr_ptr, 16);
                    socElem.flowType.socket6.local_port = ntohs(localsoc->port);
                    socElem.flowType.socket6.remote_port = ntohs(peersoc->port);
                }
            }
            
            if(socElem.tag) {
                SFLADD_ELEMENT(&fs, &socElem);
            }
            else {
                /* something odd here - don't add the socElem. We can still send the sample below */
                ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "unexpected socket length or address family");
            }
        }
    }
    
    sfl_sampler_writeFlowSample(sampler, &fs);
}

/*_________________---------------------------__________________
  _________________   address lookup          __________________
  -----------------___________________________------------------

Look up an IP address and write into the SFLAddress slot provided.
Discard everything else.
*/

static bool sfwb_lookupAddress(char *name, SFLAddress *addr, apr_pool_t *configPool)
{
    int rc;
    apr_sockaddr_t *sa = NULL;
    apr_pool_t *pool = NULL;
    int ans = false;

    ap_assert(name != NULL);

    if((rc = apr_pool_create(&pool, configPool)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "create_sflow_config: error creating lookupaddress sub-pool");
        return false;
    }

    if((rc = apr_sockaddr_info_get(&sa, name, APR_UNSPEC, 0, APR_IPV4_ADDR_OK, pool)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "sfwb_lookupaddress: apr_sockaddr_info_get(%s) failed", name);
    }
    else if(sa) {
        switch(sa->family) {
        case APR_INET:
            addr->type = SFLADDRESSTYPE_IP_V4;
            memcpy(&addr->address.ip_v4.addr, sa->ipaddr_ptr, 4);
            ans = true;
            break;
        case APR_INET6:
            addr->type = SFLADDRESSTYPE_IP_V6;
            memcpy(&addr->address.ip_v6, sa->ipaddr_ptr, 16);
            ans = true;
            break;
        default:
            ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "getaddrinfo(): unexpected address family: %u", sa->family);
            break;
        }
    }
    apr_pool_destroy(pool);
    return ans;
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
    int rc;
    apr_pool_t *pool;

    /* create a sub-pool to allocate this new config from */
    if((rc = apr_pool_create(&pool, sm->configPool)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "create_sflow_config: error creating new config sub-pool");
        return NULL;
    }
    
    SFWBConfig *config = apr_pcalloc(pool, sizeof(SFWBConfig));

    /* remember my own subpool */
    config->pool = pool;

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
                if(!config->got_sampling_n_http) {
                    config->sampling_n = strtol(tokv[1], NULL, 0);
                }
            }
            else if(strcasecmp(tokv[0], "sampling.http") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "sampling.http=<int>")) {
                /* sampling.http takes precedence over sampling */
                config->sampling_n = strtol(tokv[1], NULL, 0);
                config->got_sampling_n_http = true;
            }
            else if(strcasecmp(tokv[0], "polling") == 0 
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "polling=<int>")) {
                if(!config->got_polling_secs_http) {
                    config->polling_secs = strtol(tokv[1], NULL, 0);
                }
            }
            else if(strcasecmp(tokv[0], "polling.http") == 0 
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "polling.http=<int>")) {
                /* polling.http takes precedence over polling */
                config->polling_secs = strtol(tokv[1], NULL, 0);
                config->got_polling_secs_http = true;
            }
            else if(strcasecmp(tokv[0], "agentIP") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 2, "agentIP=<IP address>|<IPv6 address>")) {
                if(sfwb_lookupAddress(tokv[1], &config->agentIP, pool) == false) {
                    sfwb_syntaxError(config, lineNo, "agent address lookup failed");
                }
            }
            else if(strcasecmp(tokv[0], "collector") == 0
                    && sfwb_syntaxOK(config, lineNo, tokc, 2, 4, "collector=<IP address>[ <port>[ <priority>]]")) {
                if(config->num_collectors < SFWB_MAX_COLLECTORS) {
                    uint32_t i = config->num_collectors++;
                    uint32_t port = tokc >= 3 ? strtol(tokv[2], NULL, 0) : 6343;
                    config->collectors[i].priority = tokc >= 4 ? strtol(tokv[3], NULL, 0) : 0;
                    if((rc = apr_sockaddr_info_get(&config->collectors[i].sa,
                                                   tokv[1],
                                                   APR_UNSPEC,
                                                   port,
                                                   APR_IPV4_ADDR_OK,
                                                   pool)) != APR_SUCCESS) {
                        ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "create_sflow_config: error allocating collector socket address");
                    }
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
        apr_pool_destroy(pool);
        return NULL;
    }
}

/*_________________---------------------------__________________
  _________________        apply config       __________________
  -----------------___________________________------------------
*/

static void sfwb_apply_config(SFWB *sm, SFWBConfig *config)
{
    SFWBConfig *oldConfig = sm->config;

    if(config) {
        /* apply the new one */
        sm->config = config;
        sflow_init(sm);
    }

    if(oldConfig) {
        /* free the old one */
        apr_pool_destroy(oldConfig->pool);
    }
}

/*_________________---------------------------__________________
  _________________   config file mod-time    __________________
  -----------------___________________________------------------
*/
        
apr_time_t configModified(SFWB *sm) {
    int rc;
    apr_finfo_t configFileInfo;
    apr_pool_t *p;
    apr_time_t mtime = 0;
    /* a pool for temporary allocation */
    if((rc = apr_pool_create(&p, sm->configPool)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, " apr_pool_create() failed");
    }
    if((rc = apr_stat(&configFileInfo, sm->configFile, APR_FINFO_MTIME, p)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "apr_stat() failed");
    }
    else {
        mtime = configFileInfo.mtime;
    }
    apr_pool_destroy(p);
    return mtime;
}

/*_________________---------------------------__________________
  _________________      1 second tick        __________________
  -----------------___________________________------------------
*/
        
void sflow_tick(SFWB *sm) {
    if(--sm->configCountDown <= 0) {
        apr_time_t modTime = configModified(sm);
        sm->configCountDown = SFWB_CONFIG_CHECK_S;
        ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "checking for config file change <%s>", sm->configFile);

        if(modTime == 0) {
            /* config file missing */
            sfwb_apply_config(sm, NULL);
        }
        else if(modTime != sm->configFile_modTime) {
            /* config file modified */
            ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "config file changed <%s>", sm->configFile);
            SFWBConfig *newConfig = sfwb_readConfig(sm);
            if(newConfig) {
                /* config OK - apply it */
                ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "config file OK <%s>", sm->configFile);
                sfwb_apply_config(sm, newConfig);
                sm->configFile_modTime = modTime;
            }
            else {
                /* bad config - ignore it (may be in transition) */
                ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "config file parse failed <%s>", sm->configFile);
            }
        }
    }
    
    if(sm->agent && sm->config) {
        sfl_agent_tick(sm->agent, sm->currentTime);
    }
}

/*_________________---------------------------__________________
  _________________  master sflow agent init  __________________
  -----------------___________________________------------------
*/

static void sflow_init(SFWB *sm)
{
    int rc;
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "in sflow_init: sFlow=%p pid=%u config=%p",
                 (void *)sm,
                 getpid(),
                 (void *)&sm->config);

    if(sm->configFile == NULL) {
        sm->configFile = SFWB_DEFAULT_CONFIGFILE;
    }

    if(sm->config == NULL) return;

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "in sflow_init: building sFlow agent");

    {
        /* create or re-create the agent */
        if(sm->agent) {
            sfl_agent_release(sm->agent);
            apr_pool_clear(sm->masterPool);
            sm->socket4 = NULL;
            sm->socket6 = NULL;
        }

        sm->agent = (SFLAgent *)apr_pcalloc(sm->masterPool, sizeof(SFLAgent));

        /* open the send sockets - one for v4 and another for v6 */
        if(!sm->socket4) {
            if((rc = apr_socket_create(&sm->socket4, APR_INET, SOCK_DGRAM, APR_PROTO_UDP, sm->masterPool)) != APR_SUCCESS)
                ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "IPv4 send socket open failed");
        }
        if(!sm->socket6) {
            if((rc = apr_socket_create(&sm->socket6, APR_INET6, SOCK_DGRAM, APR_PROTO_UDP, sm->masterPool)) != APR_SUCCESS)
                ap_log_error(APLOG_MARK, APLOG_ERR, rc, NULL, "IPv6 send socket open failed");
        }
        
        /* initialize the agent with it's address, bootime, callbacks etc. */
        sfl_agent_init(sm->agent,
                       &sm->config->agentIP,
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
        sfl_poller_set_sFlowCpInterval(sm->poller, sm->config->polling_secs);
        sfl_poller_set_sFlowCpReceiver(sm->poller, 1 /* receiver index == 1 */);
        
        /* add a sampler for the sampled operations */
        sm->sampler = sfl_agent_addSampler(sm->agent, &dsi);
        sfl_sampler_set_sFlowFsPacketSamplingRate(sm->sampler, sm->config->sampling_n);
        sfl_sampler_set_sFlowFsReceiver(sm->sampler, 1 /* receiver index == 1 */);
        
        if(sm->config->sampling_n) {
            /* IPC to the child processes */
            SFWBShared *shared = (SFWBShared *)sm->shared_mem_base;
            shared->sflow_skip = sm->config->sampling_n;
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
    apr_status_t rc;
    
    /* with the pipe in non-blocking mode, we now poll it's descriptor with a timeout */

    /* I don't see any functions for assembling an apr_pollfd_t object,
       so just build it manually. */
    apr_pollfd_t *my_pollfd = apr_pcalloc(p, sizeof(apr_pollfd_t));
    my_pollfd->desc_type = APR_POLL_FILE;
    my_pollfd->desc.f = sm->pipe_read;
    my_pollfd->reqevents = APR_POLLIN;
    
    /* There's only one, so we can just use apr_poll, rather than build a pollset */
    /*     apr_pollset_t *my_pollset = NULL; */
    /*     rc = apr_pollset_create(&my_pollset, 1, p, 0); */
    /*     ap_assert(rc == APR_SUCCESS); */
    /*     rc = apr_pollset_add(my_pollset, &my_pollfd); */
    /*     ap_assert(rc == APR_SUCCESS); */

    /* now loop forever */
    for(;;) {
        apr_time_t now = apr_time_sec(apr_time_now());
        if(sm->currentTime != now) {
            sflow_tick(sm);
            sm->currentTime = now;
        }
        apr_int32_t nsds = 0;
        /* poll with timeout just under a second so that the sflow_tick can be issued every second.*/
        rc = apr_poll(my_pollfd, 1, &nsds, 900000 /* in uS */);
        ap_assert(rc == APR_SUCCESS || APR_STATUS_IS_TIMEUP(rc));
        if(rc == APR_SUCCESS && my_pollfd->rtnevents > 0) {
            uint32_t msg[PIPE_BUF / sizeof(uint32_t)];
            /* just read the length and type first */
            size_t hdrBytes = 12;
            size_t hdrBytesRead = 0;
            rc = apr_file_read_full(sm->pipe_read, msg, hdrBytes, &hdrBytesRead);
            if(rc == APR_SUCCESS && hdrBytesRead != 0) {
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
                ap_assert(msgBytes <= PIPE_BUF);
                size_t bodyBytes = msgBytes - hdrBytes;
                size_t bodyBytesRead = 0;
                rc = apr_file_read_full(sm->pipe_read, msg, bodyBytes, &bodyBytesRead);
                ap_assert(rc == APR_SUCCESS);
                ap_assert(bodyBytesRead == bodyBytes);
                ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "in sflow_master - bodyBytes read=%u\n", bodyBytesRead);
                /* we may not have initialized the agent yet,  so the first few samples may end up being ignored */
                if(sm->sampler) {
                    uint32_t *datap = msg;
                    if(msgType == SFLCOUNTERS_SAMPLE && msgId == SFLCOUNTERS_HTTP) {
                        /* counter block */
                        SFWBShared *shared = (SFWBShared *)sm->shared_mem_base;
                        SFLHTTP_counters c;
                        memcpy(&c, datap, sizeof(c));
                        /* accumulate into my total */
                        shared->http_counters.counterBlock.http.method_option_count += c.method_option_count;
                        shared->http_counters.counterBlock.http.method_get_count += c.method_get_count;
                        shared->http_counters.counterBlock.http.method_head_count += c.method_head_count;
                        shared->http_counters.counterBlock.http.method_post_count += c.method_post_count;
                        shared->http_counters.counterBlock.http.method_put_count += c.method_put_count;
                        shared->http_counters.counterBlock.http.method_delete_count += c.method_delete_count;
                        shared->http_counters.counterBlock.http.method_trace_count += c.method_trace_count;
                        shared->http_counters.counterBlock.http.method_connect_count += c.method_connect_count;
                        shared->http_counters.counterBlock.http.method_other_count += c.method_other_count;
                        shared->http_counters.counterBlock.http.status_1XX_count += c.status_1XX_count;
                        shared->http_counters.counterBlock.http.status_2XX_count += c.status_2XX_count;
                        shared->http_counters.counterBlock.http.status_3XX_count += c.status_3XX_count;
                        shared->http_counters.counterBlock.http.status_4XX_count += c.status_4XX_count;
                        shared->http_counters.counterBlock.http.status_5XX_count += c.status_5XX_count;
                        shared->http_counters.counterBlock.http.status_other_count += c.status_other_count;
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
    
    /* The pipe must be non-blocking to ensure that worker-threads never block on write.
       setting the "timeout" to 0 seems to be equivalent to setting O_NONBLOCK? */
    apr_file_pipe_timeout_set(sm->pipe_read, 0);
    apr_file_pipe_timeout_set(sm->pipe_write, 0);

    /* create anonymous shared memory for the sFlow agent structures and packet buffer */
    sm->shared_bytes_total = sizeof(SFWBShared);
    if((status = apr_shm_create(&sm->shared_mem, sm->shared_bytes_total, NULL, p)) != OK) {
        ap_log_error(APLOG_MARK, APLOG_ERR, status, s, "apr_shm_create() failed");
        /* may return ENOTIMPL if anon shared mem not supported,  in which case we */
        /* should try again with a filename. $$$ */
        return HTTP_INTERNAL_SERVER_ERROR;
    }
    sm->shared_mem_base = apr_shm_baseaddr_get(sm->shared_mem); /* each child must call again */

    SFWBShared *shared = (SFWBShared *)sm->shared_mem_base;
    shared->http_counters.tag = SFLCOUNTERS_HTTP;

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
#ifdef SFWB_DEBUG
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "create_sflow_config - pid=%u,tid=%u\n", getpid(), MYGETTID);
#endif
    sm->configFile = SFWB_DEFAULT_CONFIGFILE;

    /* a pool to use for the agent so we can recycle the memory easily on a config change */
    if((rc = apr_pool_create(&sm->masterPool, p)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, s, "create_sflow_config: error creating master agent sub-pool");
    }
    /* a pool to use for the config so we can allocate apr_sockaddr_t objects */
    if((rc = apr_pool_create(&sm->configPool, p)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, s, "create_sflow_config: error creating config sub-pool");
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

#ifdef SFWB_DEBUG
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_post_config - pid=%u,tid=%u\n", getpid(),MYGETTID);
#endif

    /* All post_config hooks are called twice, we're only interested in the second call. */
    apr_pool_userdata_get(&flag, MOD_SFLOW_USERDATA_KEY, s->process->pool);
    if (!flag) {
        apr_pool_userdata_set((void*) 1, MOD_SFLOW_USERDATA_KEY, apr_pool_cleanup_null, s->process->pool);
        return OK;
    }

    if(sm) {
        
#ifdef SFWB_DEBUG
        if((rc = ap_mpm_query(AP_MPMQ_IS_THREADED, &sm->mpm_threaded)) == APR_SUCCESS) {
            /* We could use this information to decided whether to create the mutex in each child */
            ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_post_config - threaded=%u\n", sm->mpm_threaded);
        }
        else {
            ap_log_error(APLOG_MARK, APLOG_DEBUG, rc, s, "sflow_post_config - ap_mpm_query(AP_MPMQ_IS_THREADED) failed\n");
        }
#endif
        
        if(sm->sFlowProc == NULL) {
            start_sflow_master(p, s, sm);
        }
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
    int rc;
    SFWB *sm = GET_CONFIG_DATA(s);
#ifdef SFWB_DEBUG
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_init_child - pid=%u,tid=%u\n", getpid(),MYGETTID);
#endif
    /* create my own private state, and hang it off the shared state */
    SFWBChild *child = (SFWBChild *)apr_pcalloc(p, sizeof(SFWBChild));
    sm->child = child;
    /* remember the config pool so the allocation callback can use it (no
       need for a sub-pool here because we don't need to recycle) */
    child->childPool = p;
    /* shared_mem base address - may be different for each child, so put in private state */
    child->shared_mem_base = apr_shm_baseaddr_get(sm->shared_mem);

    if(child->mutex == NULL) {
        /* Create a mutex to allow worker threads in the same child process to share state */

        /* this mutex may not be necessary if (sm->mpm_threaded==false), but the overhead is
           low and we want the mutex code to be excercised and tested so just create one every time
           whether it is needed or not. */

        if((rc = apr_thread_mutex_create(&child->mutex, APR_THREAD_MUTEX_DEFAULT, p)) != APR_SUCCESS) {
            ap_log_error(APLOG_MARK, APLOG_DEBUG, rc, s, "sflow_init_child - apr_thread_mutex_create() failed\n");
        }
    }

    /* create my own sFlow agent+sampler+receiver just so I can use it to encode XDR messages */
    /* before sending them on the pipe */
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
    /* it's a 32-bit aligned read, so we don't need a lock */
    return shared->sflow_skip;
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

    /* The simplest thing here is just to mutex-lock this whole step.
       Most times through here we do very little anyway.  The alternative
       would be to use atomic operations for the increments/decrements/tests that
       we do every time, and only grab the mutex when we decide to take a sample,
       but it's not clear if that would help or not.  It could easily end up
       costing more. */
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
        /* if greater than PIPE_BUF the pipe write will not be atomic. Should never happen, */
        /* but can't risk it, since we are relying on this as the synchronization mechanism. */
        if(msgBytes > PIPE_BUF) {
            ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "msgBytes=%u exceeds %u-byte limit for atomic write", msgBytes, PIPE_BUF);
            /* this counts as an sFlow drop-event */
            child->sampler->dropEvents++;
        }
        else {
            apr_size_t msgBytes2 = (apr_size_t)msgBytes;
            if(apr_file_write_full(sm->pipe_write, msg, msgBytes2, &msgBytes2) != APR_SUCCESS) {
                /* this can happen if the pipe is full - e.g. under high load conditions with
                   agressive sampling.  The pipe is non-blocking so we'll get EAGAIN or EWOULDBLOCK */
                ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "error in apr_file_write_full()\n");
                /* this counts as an sFlow drop-event */
                child->sampler->dropEvents++;
            }
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

        /* if greater than PIPE_BUF the pipe write will not be atomic. Should never happen, */
        /* but can't risk it, since we are relying on this as the synchronization mechanism */
        if(msgBytes > PIPE_BUF) {
            ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "msgBytes=%u exceeds %u-byte limit for atomic write", msgBytes, PIPE_BUF);
            /* this counts as an sFlow drop-event */
            child->sampler->dropEvents++;
        }
        else {
            apr_size_t msgBytes2 = (apr_size_t)msgBytes;
            if(apr_file_write_full(sm->pipe_write, msg, msgBytes2, &msgBytes2) != APR_SUCCESS) {
                /* this can happen if the pipe is full - e.g. under high load conditions with
                   agressive sampling.  The pipe is non-blocking so we'll get EAGAIN or EWOULDBLOCK */
                ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "error in apr_file_write_full()\n");
                /* this counts as an sFlow drop-event */
                child->sampler->dropEvents++;
            }
        }
        sfl_receiver_resetSampleCollector(child->receiver);

        /* check in case the sampling-rate setting has changed. */
        sflow_set_random_skip(child);
    }

    releaseOrDie(child->mutex);

    return OK;
}

/*_________________---------------------------__________________
  _________________      sflow_hander         __________________
  -----------------___________________________------------------
*/

static int sflow_handler(request_rec *r)
{
    if (strcmp(r->handler, "sflow")) {
        return DECLINED;
    }
    r->content_type = "text/plain";      

    if (!r->header_only) {
        if(r->server) {
            SFWB *sm = GET_CONFIG_DATA(r->server);
            if(sm) {
                SFWBShared *shared = (SFWBShared *)sm->child->shared_mem_base;
                /* aligned 32-bit reads.  Assume atomic.  No locking required */
                ap_rprintf(r, "counter method_option_count %u\n", shared->http_counters.counterBlock.http.method_option_count);
                ap_rprintf(r, "counter method_get_count %u\n", shared->http_counters.counterBlock.http.method_get_count);
                ap_rprintf(r, "counter method_head_count %u\n", shared->http_counters.counterBlock.http.method_head_count);
                ap_rprintf(r, "counter method_post_count %u\n", shared->http_counters.counterBlock.http.method_post_count);
                ap_rprintf(r, "counter method_put_count %u\n", shared->http_counters.counterBlock.http.method_put_count);
                ap_rprintf(r, "counter method_delete_count %u\n", shared->http_counters.counterBlock.http.method_delete_count);
                ap_rprintf(r, "counter method_trace_count %u\n", shared->http_counters.counterBlock.http.method_trace_count);
                ap_rprintf(r, "counter method_connect_count %u\n", shared->http_counters.counterBlock.http.method_connect_count);
                ap_rprintf(r, "counter method_other_count %u\n", shared->http_counters.counterBlock.http.method_other_count);
                ap_rprintf(r, "counter status_1XX_count %u\n", shared->http_counters.counterBlock.http.status_1XX_count);
                ap_rprintf(r, "counter status_2XX_count %u\n", shared->http_counters.counterBlock.http.status_2XX_count);
                ap_rprintf(r, "counter status_3XX_count %u\n", shared->http_counters.counterBlock.http.status_3XX_count);
                ap_rprintf(r, "counter status_4XX_count %u\n", shared->http_counters.counterBlock.http.status_4XX_count);
                ap_rprintf(r, "counter status_5XX_count %u\n", shared->http_counters.counterBlock.http.status_5XX_count);
                ap_rprintf(r, "counter status_other_count %u\n", shared->http_counters.counterBlock.http.status_other_count);
                /* extra info */
                ap_rprintf(r, "string hostname %s\n", r->hostname);
                ap_rprintf(r, "gauge sampling_n %u\n", shared->sflow_skip);
            }
        }
    }

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
    ap_hook_handler(sflow_handler, NULL, NULL, APR_HOOK_MIDDLE);
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

