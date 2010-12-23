/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "sflow_wb.h"
static int sfwb_debug = 0;

static void sfwb_log(int syslogType, char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    if(sfwb_debug) {
        vfprintf(stderr, fmt, args);
        fprintf(stderr, "\n");
    }
    else {
        vsyslog(syslogType, fmt, args);
    }
}
  
static void *sfwb_calloc(size_t bytes)
{
    void *mem = calloc(1, bytes);
    if(mem == NULL) {
        sfwb_log(LOG_ERR, "calloc() failed : %s", strerror(errno));
        // if(sfwb_debug) malloc_stats();
        exit(EXIT_FAILURE);
    }
    return mem;
}

static  bool lockOrDie(pthread_mutex_t *sem) {
    if(sem && pthread_mutex_lock(sem) != 0) {
        sfwb_log(LOG_ERR, "failed to lock semaphore!");
        exit(EXIT_FAILURE);
    }
    return true;
}

static bool releaseOrDie(pthread_mutex_t *sem) {
    if(sem && pthread_mutex_unlock(sem) != 0) {
        sfwb_log(LOG_ERR, "failed to unlock semaphore!");
        exit(EXIT_FAILURE);
    }
    return true;
}

#define DYNAMIC_LOCAL(VAR) VAR
#define SEMLOCK_DO(_sem) for(int DYNAMIC_LOCAL(_ctrl)=1; DYNAMIC_LOCAL(_ctrl) && lockOrDie(_sem); DYNAMIC_LOCAL(_ctrl)=0, releaseOrDie(_sem))

static void *sfwb_cb_alloc(void *magic, SFLAgent *agent, size_t bytes)
{
    return sfwb_calloc(bytes);
}

static int sfwb_cb_free(void *magic, SFLAgent *agent, void *obj)
{
    free(obj);
    return 0;
}

static void sfwb_cb_error(void *magic, SFLAgent *agent, char *msg)
{
    sfwb_log(LOG_ERR, "sflow agent error: %s", msg);
}

static void sfwb_cb_counters(void *magic, SFLPoller *poller, SFL_COUNTERS_SAMPLE_TYPE *cs)
{
    SFWB *sm = (SFWB *)poller->magic;
    SEMLOCK_DO(sm->mutex) {
        
        if(sm->config == NULL) {
            /* config is disabled */
            return;
        }
        
        if(sm->config->polling_secs == 0) {
            /* polling is off */
            return;
        }

        sfwb_log(LOG_INFO, "in sfwb_cb_counters!");

        SFLCounters_sample_element mcElem = { 0 };
        mcElem.tag = SFLCOUNTERS_HTTP;
        mcElem.counterBlock.http.method_option_count = 0;
        mcElem.counterBlock.http.method_get_count = 0;
        mcElem.counterBlock.http.method_head_count = 0;
        mcElem.counterBlock.http.method_post_count = 0;
        mcElem.counterBlock.http.method_put_count = 0;
        mcElem.counterBlock.http.method_delete_count = 0;
        mcElem.counterBlock.http.method_trace_count = 0;
        mcElem.counterBlock.http.methd_connect_count = 0;
        mcElem.counterBlock.http.method_other_count = 0;
        mcElem.counterBlock.http.status_1XX_count = 0;
        mcElem.counterBlock.http.status_2XX_count = 0;
        mcElem.counterBlock.http.status_3XX_count = 0;
        mcElem.counterBlock.http.status_4XX_count = 0;
        mcElem.counterBlock.http.status_5XX_count = 0;
        mcElem.counterBlock.http.status_other_count = 0;
        SFLADD_ELEMENT(cs, &mcElem);
        sfl_poller_writeCountersSample(poller, cs);
    }
}

void sflow_sample(SFWB *sm, struct conn *c, SFLMemcache_prot prot, SFLMemcache_cmd cmd, char *key, size_t keylen, uint32_t nkeys, size_t value_bytes, uint32_t duration_uS, uint32_t status)
{
    SEMLOCK_DO(sm->mutex) {
        
        if(sm->config == NULL) {
            /* config is disabled */
            return;
        }
        
        if(sm->config->sampling_n == 0) {
            /* sampling is off */
            return;
        }

        SFLSampler *sampler = sm->agent->samplers;
        if(sampler == NULL) {
            return;
        }
        
        /* update the all-important sample_pool */
        sampler->samplePool += c->thread->sflow_last_skip;
        
        SFL_FLOW_SAMPLE_TYPE fs = { 0 };
        
        /* indicate that I am the server by setting the
           destination interface to 0x3FFFFFFF=="internal"
           and leaving the source interface as 0=="unknown" */
        fs.output = 0x3FFFFFFF;
        
        sfwb_log(LOG_INFO, "in sfwb_sample_operation!");
        
        SFLFlow_sample_element mcopElem = { 0 };
        mcopElem.tag = SFLFLOW_MEMCACHE;
        mcopElem.flowType.memcache.protocol = prot;
        mcopElem.flowType.memcache.command = cmd;
        mcopElem.flowType.memcache.key.str = key;
        mcopElem.flowType.memcache.key.len = (key ? keylen : 0);
        mcopElem.flowType.memcache.nkeys = (nkeys == SFLOW_TOKENS_UNKNOWN) ? 1 : nkeys;
        mcopElem.flowType.memcache.value_bytes = value_bytes;
        mcopElem.flowType.memcache.duration_uS = duration_uS;
        mcopElem.flowType.memcache.status = status;
        SFLADD_ELEMENT(&fs, &mcopElem);
        
        SFLFlow_sample_element socElem = { 0 };
        
        if(c->transport == tcp_transport ||
           c->transport == udp_transport) {
            /* add a socket structure */
            struct sockaddr_storage localsoc;
            socklen_t localsoclen = sizeof(localsoc);
            struct sockaddr_storage peersoc;
            socklen_t peersoclen = sizeof(peersoc);
            
            /* ask the fd for the local socket - may have wildcards, but
               at least we may learn the local port */
            getsockname(c->sfd, (struct sockaddr *)&localsoc, &localsoclen);
            /* for tcp the socket can tell us the peer info */
            if(c->transport == tcp_transport) {
                getpeername(c->sfd, (struct sockaddr *)&peersoc, &peersoclen);
            }
            else {
                /* for UDP the peer can be different for every packet, but
                   this info is capture in the recvfrom() and given to us */
                memcpy(&peersoc, &c->request_addr, c->request_addr_size);
            }
            
            /* two possibilities here... */
            struct sockaddr_in *soc4 = (struct sockaddr_in *)&peersoc;
            struct sockaddr_in6 *soc6 = (struct sockaddr_in6 *)&peersoc;
            
            if(peersoclen == sizeof(*soc4) && soc4->sin_family == AF_INET) {
                struct sockaddr_in *lsoc4 = (struct sockaddr_in *)&localsoc;
                socElem.tag = SFLFLOW_EX_SOCKET4;
                socElem.flowType.socket4.protocol = (c->transport == tcp_transport ? 6 : 17);
                socElem.flowType.socket4.local_ip.addr = lsoc4->sin_addr.s_addr;
                socElem.flowType.socket4.remote_ip.addr = soc4->sin_addr.s_addr;
                socElem.flowType.socket4.local_port = ntohs(lsoc4->sin_port);
                socElem.flowType.socket4.remote_port = ntohs(soc4->sin_port);
            }
            else if(peersoclen == sizeof(*soc6) && soc6->sin6_family == AF_INET6) {
                struct sockaddr_in6 *lsoc6 = (struct sockaddr_in6 *)&localsoc;
                socElem.tag = SFLFLOW_EX_SOCKET6;
                socElem.flowType.socket6.protocol = (c->transport == tcp_transport ? 6 : 17);
                memcpy(socElem.flowType.socket6.local_ip.addr, lsoc6->sin6_addr.s6_addr, 16);
                memcpy(socElem.flowType.socket6.remote_ip.addr, soc6->sin6_addr.s6_addr, 16);
                socElem.flowType.socket6.local_port = ntohs(lsoc6->sin6_port);
                socElem.flowType.socket6.remote_port = ntohs(soc6->sin6_port);
            }
            if(socElem.tag) {
                SFLADD_ELEMENT(&fs, &socElem);
            }
            else {
                sfwb_log(LOG_ERR, "unexpected socket length or address family");
            }
        }
        
        sfl_sampler_writeFlowSample(sampler, &fs);
        
        /* set the next random skip */
        c->thread->sflow_skip = sfl_random((2 * sm->config->sampling_n) - 1);
        c->thread->sflow_last_skip = c->thread->sflow_skip;
    }
}


static void sfwb_cb_sendPkt(void *magic, SFLAgent *agent, SFLReceiver *receiver, u_char *pkt, uint32_t pktLen)
{
    SFWB *sm = (SFWB *)magic;
    size_t socklen = 0;
    int fd = 0;
    
    if(sm->config == NULL) {
        /* config is disabled */
        return;
    }

    for(int c = 0; c < sm->config->num_collectors; c++) {
        SFWBCollector *coll = &sm->config->collectors[c];
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
                sfwb_log(LOG_ERR, "socket sendto error: %s", strerror(errno));
            }
            if(result == 0) {
                sfwb_log(LOG_ERR, "socket sendto returned 0: %s", strerror(errno));
            }
        }
    }
}

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
        default: sfwb_log(LOG_ERR, "getaddrinfo() error: %s", gai_strerror(err)); break;
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
            sfwb_log(LOG_ERR, "get addrinfo: unexpected address family: %d", info->ai_family);
            return false;
            break;
        }
    }
    /* free the dynamically allocated data before returning */
    freeaddrinfo(info);
    return true;
}

static bool sfwb_syntaxOK(SFWBConfig *cfg, uint32_t line, uint32_t tokc, uint32_t tokcMin, uint32_t tokcMax, char *syntax) {
    if(tokc < tokcMin || tokc > tokcMax) {
        cfg->error = true;
        sfwb_log(LOG_ERR, "syntax error on line %u: expected %s",
                 line,
                 syntax);
        return false;
    }
    return true;
}

static void sfwb_syntaxError(SFWBConfig *cfg, uint32_t line, char *msg) {
    cfg->error = true;
    sfwb_log(LOG_ERR, "syntax error on line %u: %s",
             line,
             msg);
}    

static SFWBConfig *sfwb_readConfig(SFWB *sm)
{
    uint32_t rev_start = 0;
    uint32_t rev_end = 0;
    SFWBConfig *config = (SFWBConfig *)sfwb_calloc(sizeof(SFWBConfig));
    FILE *cfg = NULL;
    if((cfg = fopen(sm->configFile, "r")) == NULL) {
        sfwb_log(LOG_ERR,"cannot open config file %s : %s", sm->configFile, strerror(errno));
        return NULL;
    }
    char line[SFWB_MAX_LINELEN+1];
    uint32_t lineNo = 0;
    char *tokv[5];
    uint32_t tokc;
    while(fgets(line, SFWB_MAX_LINELEN, cfg)) {
        lineNo++;
        char *p = line;
        /* comments start with '#' */
        p[strcspn(p, "#")] = '\0';
        /* 1 var and up to 3 value tokens, so detect up to 5 tokens overall */
        /* so we know if there was an extra one that should be flagged as a */
        /* syntax error. */
        tokc = 0;
        for(int i = 0; i < 5; i++) {
            size_t len;
            p += strspn(p, SFWB_SEPARATORS);
            if((len = strcspn(p, SFWB_SEPARATORS)) == 0) break;
            tokv[tokc++] = p;
            p += len;
            if(*p != '\0') *p++ = '\0';
        }

        if(tokc >=2) {
            sfwb_log(LOG_INFO,"line=%s tokc=%u tokv=<%s> <%s> <%s>",
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
        free(config);
        return NULL;
    }
}

static void sfwb_apply_config(SFWB *sm, SFWBConfig *config)
{
    if(sm->config == config) return;
    SFWBConfig *oldConfig = sm->config;
    SEMLOCK_DO(sm->mutex) {
        sm->config = config;
    }
    if(oldConfig) free(oldConfig);
    if(config) sflow_init(sm);
}
    
        
void sflow_tick(SFWB *sm) {
    
    if(!sm->enabled) return;
    
    if(sm->configTests == 0 || (current_time % 10 == 0)) {
        sm->configTests++;
        sfwb_log(LOG_INFO, "checking for config file change <%s>", sm->configFile);
        struct stat statBuf;
        if(stat(sm->configFile, &statBuf) != 0) {
            /* config file missing */
            sfwb_apply_config(sm, NULL);
        }
        else if(statBuf.st_mtime != sm->configFile_modTime) {
            /* config file modified */
            sfwb_log(LOG_INFO, "config file changed");
            SFWBConfig *newConfig = sfwb_readConfig(sm);
            if(newConfig) {
                /* config OK - apply it */
                sfwb_log(LOG_INFO, "config OK");
                sfwb_apply_config(sm, newConfig);
                sm->configFile_modTime = statBuf.st_mtime;
            }
            else {
                /* bad config - ignore it (may be in transition) */
                sfwb_log(LOG_INFO, "config failed");
            }
        }
    }
    
    if(sm->agent && sm->config) {
        sfl_agent_tick(sm->agent, (time_t)current_time);
    }
}

void sflow_init(SFWB *sm) {

    if(sm->configFile == NULL) {
        sm->configFile = SFWB_DEFAULT_CONFIGFILE;
    }

    if(sm->config == NULL) return;

    if(sm->mutex == NULL) {
        sm->mutex = (pthread_mutex_t*)sfwb_calloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(sm->mutex, NULL);
    }

    SEMLOCK_DO(sm->mutex) {
        /* create/re-create the agent */
        if(sm->agent) {
            sfl_agent_release(sm->agent);
            free(sm->agent);
        }
        sm->agent = (SFLAgent *)sfwb_calloc(sizeof(SFLAgent));
        
        /* open the sockets - one for v4 and another for v6 */
        if(sm->socket4 <= 0) {
            if((sm->socket4 = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
                sfwb_log(LOG_ERR, "IPv4 send socket open failed : %s", strerror(errno));
        }
        if(sm->socket6 <= 0) {
            if((sm->socket6 = socket(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)) == -1)
                sfwb_log(LOG_ERR, "IPv6 send socket open failed : %s", strerror(errno));
        }
        
        /* initialize the agent with it's address, bootime, callbacks etc. */
        sfl_agent_init(sm->agent,
                       &sm->config->agentIP,
                       0, /* subAgentId */
                       current_time,
                       current_time,
                       sm,
                       sfwb_cb_alloc,
                       sfwb_cb_free,
                       sfwb_cb_error,
                       sfwb_cb_sendPkt);
        
        /* add a receiver */
        SFLReceiver *receiver = sfl_agent_addReceiver(sm->agent);
        sfl_receiver_set_sFlowRcvrOwner(receiver, "memcached sFlow Probe");
        sfl_receiver_set_sFlowRcvrTimeout(receiver, 0xFFFFFFFF);
        
        /* no need to configure the receiver further, because we are */
        /* using the sendPkt callback to handle the forwarding ourselves. */
        
        /* add a <logicalEntity> datasource to represent this application instance */
        SFLDataSource_instance dsi;
        /* ds_class = <logicalEntity>, ds_index = 65537, ds_instance = 0 */
        /* $$$ should learn the ds_index from the config file */
        SFL_DS_SET(dsi, SFL_DSCLASS_LOGICAL_ENTITY, 65537, 0);

        /* add a poller for the counters */
        SFLPoller *poller = sfl_agent_addPoller(sm->agent, &dsi, sm, sfwb_cb_counters);
        sfl_poller_set_sFlowCpInterval(poller, sm->config->polling_secs);
        sfl_poller_set_sFlowCpReceiver(poller, 1 /* receiver index*/);
        
        /* add a sampler for the sampled operations */
        SFLSampler *sampler = sfl_agent_addSampler(sm->agent, &dsi);
        sfl_sampler_set_sFlowFsPacketSamplingRate(sampler, sm->config->sampling_n);
        sfl_sampler_set_sFlowFsReceiver(sampler, 1 /* receiver index*/);

        if(sm->config->sampling_n) {
            /* seed the random number generator */
            uint32_t hash = current_time;
            u_char *addr = sm->config->agentIP.address.ip_v6.addr;
            for(int i = 0; i < 16; i += 2) {
                hash *= 3;
                hash += ((addr[i] << 8) | addr[i+1]);
            }
            sfl_random_init(hash);
            /* generate the first sampling skips */
            uint32_t *thread_skips = (uint32_t *)sfwb_calloc(settings.num_threads * sizeof(uint32_t));
            for(int ii = 0; ii < settings.num_threads; ii++) {
                thread_skips[ii] = sfl_random((2 * sm->config->sampling_n) - 1);
            }
            /* and push them out to the threads */
            sampler->samplePool += sflow_skip_init(thread_skips);
            free(thread_skips);
        }
    }
}

/* SFLMemcache_operation_status sflow_map_status(enum store_item_type ret) { */
/*     SFLMemcache_operation_status sflret = SFWB_OP_UNKNOWN; */
/*     switch(ret) { */
/*     case NOT_STORED: sflret = SFWB_OP_NOT_STORED; break; */
/*     case STORED: sflret = SFWB_OP_STORED; break; */
/*     case EXISTS: sflret = SFWB_OP_EXISTS; break; */
/*     case NOT_FOUND: sflret = SFWB_OP_NOT_FOUND; break; */
/*     } */
/*     return sflret; */
/* } */

/* SFLMemcache_cmd sflow_map_nread(int cmd) { */
/*     SFLMemcache_cmd sflcmd = SFWB_CMD_OTHER; */
/*     switch(cmd) { */
/*     case NREAD_ADD: sflcmd=SFWB_CMD_ADD; break; */
/*     case NREAD_REPLACE: sflcmd = SFWB_CMD_REPLACE; break; */
/*     case NREAD_APPEND: sflcmd = SFWB_CMD_APPEND; break; */
/*     case NREAD_PREPEND: sflcmd = SFWB_CMD_PREPEND; break; */
/*     case NREAD_SET: sflcmd = SFWB_CMD_SET; break; */
/*     case NREAD_CAS: sflcmd = SFWB_CMD_CAS; break; */
/*     } */
/*     return sflcmd; */
/* } */

