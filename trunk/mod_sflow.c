/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* Copyright (c) 2002-2010 InMon Corp. Licensed under the terms of the InMon sFlow licence: */
/* http://www.inmon.com/technology/sflowlicense.txt */

/* 
**  mod_sflow.c -- Apache sample sflow module.
**
**  A binary, random-sampling logger designed for:
**       lightweight,
**        centralized,
**         continuous,
**          real-time monitoring of very large and very busy web farms.
**
**  To play with this sample module first compile it into a
**  DSO file and install it into Apache's modules directory 
**  by running:
**
**    $ apxs -c -i mod_sflow.c
**
**  Then activate it in Apache's httpd.conf file for instance
**  for the URL /sflow in as follows:
**
**    #   httpd.conf
**    LoadModule sflow_module modules/mod_sflow.so
**    <Location /sflow>
**    SetHandler sflow
**    </Location>
**
**  Then after restarting Apache via
**
**    $ apachectl restart
**
**  you immediately can request the URL /sflow and watch for the
**  output of this module. This can be achieved for instance via:
**
**    $ lynx -mime_header http://localhost/sflow 
**
**  This module reads it sFlow configuration from the /etc/hsflowd.auto
**  file that is generated automatically when you run the host-sflow
**  daemon hsflowd on the same server:
**
**  http://host-sflow.sourceforge.net
**
**  The hsflowd daemon includes a DNS-SD client so it can learn the
**  sFlow configuration automatically,  and the /etc/hsflowd.auto file
**  is then a way to share it with other sFlow sub-agents running on
**  the same host, such as this apache module.
**
**  The sFlow output goes to a UDP port on your sFlow collector host.
**  There you can examine it using a number of tools,  including the
**  freeware "sflowtool", which can be downloaded as source code from:
**  
**  http://www.inmon.com/technology/sflowTools.php
*/ 

#include "apr.h"
#include "apr_lib.h"
#include "apr_strings.h"
#include "httpd.h"
#include "http_config.h"
#include "http_protocol.h"
#include "http_log.h"
#include "ap_config.h"

#include "sflow_wb.h"

/* ==========================================================*/
/* ==========================================================*/
/* ======================= sFlow API ======================== */
/* ==========================================================*/

#include "sflow_api.h"

/* ===================================================*/
/* ===================== AGENT =======================*/


static void * sflAlloc(SFLAgent *agent, size_t bytes);
static void sflFree(SFLAgent *agent, void *obj);
static void sfl_agent_jumpTableAdd(SFLAgent *agent, SFLSampler *sampler);
static void sfl_agent_jumpTableRemove(SFLAgent *agent, SFLSampler *sampler);

/*_________________---------------------------__________________
  _________________       alloc and free      __________________
  -----------------___________________________------------------
*/

static void * sflAlloc(SFLAgent *agent, size_t bytes)
{
    if(agent->allocFn) return (*agent->allocFn)(agent->magic, agent, bytes);
    else return SFL_ALLOC(bytes);
}

static void sflFree(SFLAgent *agent, void *obj)
{
    if(agent->freeFn) (*agent->freeFn)(agent->magic, agent, obj);
    else SFL_FREE(obj);
}
  
/*_________________---------------------------__________________
  _________________       error logging       __________________
  -----------------___________________________------------------
*/
#define MAX_ERRMSG_LEN 1000

void sfl_agent_error(SFLAgent *agent, char *modName, char *msg)
{
    char errm[MAX_ERRMSG_LEN];
    sprintf(errm, "sfl_agent_error: %s: %s\n", modName, msg);
    if(agent->errorFn) (*agent->errorFn)(agent->magic, agent, errm);
    else {
        fprintf(stderr, "%s\n", errm);
        fflush(stderr);
    }
}

void sfl_agent_sysError(SFLAgent *agent, char *modName, char *msg)
{
    char errm[MAX_ERRMSG_LEN];
    sprintf(errm, "sfl_agent_sysError: %s: %s (errno = %d - %s)\n",
            modName,
            msg,
            errno,
            strerror(errno));
    if(agent->errorFn) (*agent->errorFn)(agent->magic, agent, errm);
    else {
        fprintf(stderr, "%s\n", errm);
        fflush(stderr);
    }
}

/*________________--------------------------__________________
  ________________    sfl_agent_init        __________________
  ----------------__________________________------------------
*/

void sfl_agent_init(SFLAgent *agent,
                    SFLAddress *myIP, /* IP address of this agent in net byte order */
                    uint32_t subId,  /* agent_sub_id */
                    time_t bootTime,  /* agent boot time */
                    time_t now,       /* time now */
                    void *magic,      /* ptr to pass back in logging and alloc fns */
                    allocFn_t allocFn,
                    freeFn_t freeFn,
                    errorFn_t errorFn,
                    sendFn_t sendFn)
{
    /* first clear everything */
    memset(agent, 0, sizeof(*agent));
    /* now copy in the parameters */
    agent->myIP = *myIP; /* structure copy */
    agent->subId = subId;
    agent->bootTime = bootTime;
    agent->now = now;
    agent->magic = magic;
    agent->allocFn = allocFn;
    agent->freeFn = freeFn;
    agent->errorFn = errorFn;
    agent->sendFn = sendFn;

#ifdef SFLOW_DO_SOCKET  
    if(sendFn == NULL) {
        /* open the socket - really need one for v4 and another for v6? */
        if((agent->receiverSocket4 = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1)
            sfl_agent_sysError(agent, "agent", "IPv4 socket open failed");
        if((agent->receiverSocket6 = socket(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)) == -1)
            sfl_agent_sysError(agent, "agent", "IPv6 socket open failed");
    }
#endif
}

/*_________________---------------------------__________________
  _________________   sfl_agent_release       __________________
  -----------------___________________________------------------
*/

void sfl_agent_release(SFLAgent *agent)
{
 
    SFLSampler *sm;
    SFLPoller *pl;
    SFLReceiver *rcv;
    /* release and free the samplers */
    for(sm = agent->samplers; sm != NULL; ) {
        SFLSampler *nextSm = sm->nxt;
        sflFree(agent, sm);
        sm = nextSm;
    }
    agent->samplers = NULL;

    /* release and free the pollers */
    for( pl= agent->pollers; pl != NULL; ) {
        SFLPoller *nextPl = pl->nxt;
        sflFree(agent, pl);
        pl = nextPl;
    }
    agent->pollers = NULL;

    /* release and free the receivers */
    for( rcv = agent->receivers; rcv != NULL; ) {
        SFLReceiver *nextRcv = rcv->nxt;
        sflFree(agent, rcv);
        rcv = nextRcv;
    }
    agent->receivers = NULL;

#ifdef SFLOW_DO_SOCKET
    /* close the sockets */
    if(agent->receiverSocket4 > 0) close(agent->receiverSocket4);
    if(agent->receiverSocket6 > 0) close(agent->receiverSocket6);
#endif
}

/*_________________---------------------------__________________
  _________________   sfl_agent_tick          __________________
  -----------------___________________________------------------
*/

void sfl_agent_tick(SFLAgent *agent, time_t now)
{
    SFLReceiver *rcv;
    SFLSampler *sm;
    SFLPoller *pl;

    agent->now = now;
    /* receivers use ticks to flush send data */
    for( rcv = agent->receivers; rcv != NULL; rcv = rcv->nxt) sfl_receiver_tick(rcv, now);
    /* samplers use ticks to decide when they are sampling too fast */
    for( sm = agent->samplers; sm != NULL; sm = sm->nxt) sfl_sampler_tick(sm, now);
    /* pollers use ticks to decide when to ask for counters */
    for( pl = agent->pollers; pl != NULL; pl = pl->nxt) sfl_poller_tick(pl, now);
}

/*_________________---------------------------__________________
  _________________   sfl_agent_addReceiver   __________________
  -----------------___________________________------------------
*/

SFLReceiver *sfl_agent_addReceiver(SFLAgent *agent)
{
    SFLReceiver *rcv, *r, *prev;

    prev = NULL;
    rcv = (SFLReceiver *)sflAlloc(agent, sizeof(SFLReceiver));
    sfl_receiver_init(rcv, agent);
    /* add to end of list - to preserve the receiver index numbers for existing receivers */
 
    for(r = agent->receivers; r != NULL; prev = r, r = r->nxt);
    if(prev) prev->nxt = rcv;
    else agent->receivers = rcv;
    rcv->nxt = NULL;
    return rcv;
}

/*_________________---------------------------__________________
  _________________     sfl_dsi_compare       __________________
  -----------------___________________________------------------

  Note that if there is a mixture of ds_classes for this agent, then
  the simple numeric comparison may not be correct - the sort order (for
  the purposes of the SNMP MIB) should really be determined by the OID
  that these numeric ds_class numbers are a shorthand for.  For example,
  ds_class == 0 means ifIndex, which is the oid "1.3.6.1.2.1.2.2.1"
*/

static int sfl_dsi_compare(SFLDataSource_instance *pdsi1, SFLDataSource_instance *pdsi2) {
    /* could have used just memcmp(),  but not sure if that would */
    /* give the right answer on little-endian platforms. Safer to be explicit... */
    int cmp = pdsi2->ds_class - pdsi1->ds_class;
    if(cmp == 0) cmp = pdsi2->ds_index - pdsi1->ds_index;
    if(cmp == 0) cmp = pdsi2->ds_instance - pdsi1->ds_instance;
    return cmp;
}

/*_________________---------------------------__________________
  _________________   sfl_agent_addSampler    __________________
  -----------------___________________________------------------
*/

SFLSampler *sfl_agent_addSampler(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    SFLSampler *newsm, *prev, *sm, *test;

    prev = NULL;
    sm = agent->samplers;
    /* keep the list sorted */
    for(; sm != NULL; prev = sm, sm = sm->nxt) {
        int64_t cmp = sfl_dsi_compare(pdsi, &sm->dsi);
        if(cmp == 0) return sm;  /* found - return existing one */
        if(cmp < 0) break;       /* insert here */
    }
    /* either we found the insert point, or reached the end of the list... */
    newsm = (SFLSampler *)sflAlloc(agent, sizeof(SFLSampler));
    sfl_sampler_init(newsm, agent, pdsi);
    if(prev) prev->nxt = newsm;
    else agent->samplers = newsm;
    newsm->nxt = sm;

    /* see if we should go in the ifIndex jumpTable */
    if(SFL_DS_CLASS(newsm->dsi) == 0) {
        test = sfl_agent_getSamplerByIfIndex(agent, SFL_DS_INDEX(newsm->dsi));
        if(test && (SFL_DS_INSTANCE(newsm->dsi) < SFL_DS_INSTANCE(test->dsi))) {
            /* replace with this new one because it has a lower ds_instance number */
            sfl_agent_jumpTableRemove(agent, test);
            test = NULL;
        }
        if(test == NULL) sfl_agent_jumpTableAdd(agent, newsm);
    }
    return newsm;
}

/*_________________---------------------------__________________
  _________________   sfl_agent_addPoller     __________________
  -----------------___________________________------------------
*/

SFLPoller *sfl_agent_addPoller(SFLAgent *agent,
                               SFLDataSource_instance *pdsi,
                               void *magic,         /* ptr to pass back in getCountersFn() */
                               getCountersFn_t getCountersFn)
{
    SFLPoller *newpl;

    /* keep the list sorted */
    SFLPoller *prev = NULL, *pl = agent->pollers;
    for(; pl != NULL; prev = pl, pl = pl->nxt) {
        int64_t cmp = sfl_dsi_compare(pdsi, &pl->dsi);
        if(cmp == 0) return pl;  /* found - return existing one */
        if(cmp < 0) break;       /* insert here */
    }
    /* either we found the insert point, or reached the end of the list... */
    newpl = (SFLPoller *)sflAlloc(agent, sizeof(SFLPoller));
    sfl_poller_init(newpl, agent, pdsi, magic, getCountersFn);
    if(prev) prev->nxt = newpl;
    else agent->pollers = newpl;
    newpl->nxt = pl;
    return newpl;
}

/*_________________---------------------------__________________
  _________________  sfl_agent_removeSampler  __________________
  -----------------___________________________------------------
*/

int sfl_agent_removeSampler(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    SFLSampler *prev, *sm;

    /* find it, unlink it and free it */
    for(prev = NULL, sm = agent->samplers; sm != NULL; prev = sm, sm = sm->nxt) {
        if(sfl_dsi_compare(pdsi, &sm->dsi) == 0) {
            if(prev == NULL) agent->samplers = sm->nxt;
            else prev->nxt = sm->nxt;
            sfl_agent_jumpTableRemove(agent, sm);
            sflFree(agent, sm);
            return 1;
        }
    }
    /* not found */
    return 0;
}

/*_________________---------------------------__________________
  _________________  sfl_agent_removePoller   __________________
  -----------------___________________________------------------
*/

int sfl_agent_removePoller(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    SFLPoller *prev, *pl;
    /* find it, unlink it and free it */
    for(prev = NULL, pl = agent->pollers; pl != NULL; prev = pl, pl = pl->nxt) {
        if(sfl_dsi_compare(pdsi, &pl->dsi) == 0) {
            if(prev == NULL) agent->pollers = pl->nxt;
            else prev->nxt = pl->nxt;
            sflFree(agent, pl);
            return 1;
        }
    }
    /* not found */
    return 0;
}

/*_________________--------------------------------__________________
  _________________  sfl_agent_jumpTableAdd        __________________
  -----------------________________________________------------------
*/

static void sfl_agent_jumpTableAdd(SFLAgent *agent, SFLSampler *sampler)
{
    uint32_t hashIndex = SFL_DS_INDEX(sampler->dsi) % SFL_HASHTABLE_SIZ;
    sampler->hash_nxt = agent->jumpTable[hashIndex];
    agent->jumpTable[hashIndex] = sampler;
}

/*_________________--------------------------------__________________
  _________________  sfl_agent_jumpTableRemove     __________________
  -----------------________________________________------------------
*/

static void sfl_agent_jumpTableRemove(SFLAgent *agent, SFLSampler *sampler)
{
    uint32_t hashIndex = SFL_DS_INDEX(sampler->dsi) % SFL_HASHTABLE_SIZ;
    SFLSampler *search = agent->jumpTable[hashIndex], *prev = NULL;
    for( ; search != NULL; prev = search, search = search->hash_nxt) if(search == sampler) break;
    if(search) {
        /* found - unlink */
        if(prev) prev->hash_nxt = search->hash_nxt;
        else agent->jumpTable[hashIndex] = search->hash_nxt;
        search->hash_nxt = NULL;
    }
}

/*_________________--------------------------------__________________
  _________________  sfl_agent_getSamplerByIfIndex __________________
  -----------------________________________________------------------
  fast lookup (pointers cached in hash table).  If there are multiple
  sampler instances for a given ifIndex, then this fn will return
  the one with the lowest instance number.  Since the samplers
  list is sorted, this means the other instances will be accesible
  by following the sampler->nxt pointer (until the ds_class
  or ds_index changes).  This is helpful if you need to offer
  the same flowSample to multiple samplers.
*/

SFLSampler *sfl_agent_getSamplerByIfIndex(SFLAgent *agent, uint32_t ifIndex)
{
    SFLSampler *search = agent->jumpTable[ifIndex % SFL_HASHTABLE_SIZ];
    for( ; search != NULL; search = search->hash_nxt) if(SFL_DS_INDEX(search->dsi) == ifIndex) break;
    return search;
}

/*_________________---------------------------__________________
  _________________  sfl_agent_getSampler     __________________
  -----------------___________________________------------------
*/

SFLSampler *sfl_agent_getSampler(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    SFLSampler *sm;

    /* find it and return it */
    for( sm = agent->samplers; sm != NULL; sm = sm->nxt)
        if(sfl_dsi_compare(pdsi, &sm->dsi) == 0) return sm;
    /* not found */
    return NULL;
}

/*_________________---------------------------__________________
  _________________  sfl_agent_getPoller      __________________
  -----------------___________________________------------------
*/

SFLPoller *sfl_agent_getPoller(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    SFLPoller *pl;

    /* find it and return it */
    for( pl = agent->pollers; pl != NULL; pl = pl->nxt)
        if(sfl_dsi_compare(pdsi, &pl->dsi) == 0) return pl;
    /* not found */
    return NULL;
}

/*_________________---------------------------__________________
  _________________  sfl_agent_getReceiver    __________________
  -----------------___________________________------------------
*/

SFLReceiver *sfl_agent_getReceiver(SFLAgent *agent, uint32_t receiverIndex)
{
    SFLReceiver *rcv;

    uint32_t rcvIdx = 0;
    for( rcv = agent->receivers; rcv != NULL; rcv = rcv->nxt)
        if(receiverIndex == ++rcvIdx) return rcv;

    /* not found - ran off the end of the table */
    return NULL;
}

/*_________________---------------------------__________________
  _________________ sfl_agent_getNextSampler  __________________
  -----------------___________________________------------------
*/

SFLSampler *sfl_agent_getNextSampler(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    /* return the one lexograpically just after it - assume they are sorted
       correctly according to the lexographical ordering of the object ids */
    SFLSampler *sm = sfl_agent_getSampler(agent, pdsi);
    return sm ? sm->nxt : NULL;
}

/*_________________---------------------------__________________
  _________________ sfl_agent_getNextPoller   __________________
  -----------------___________________________------------------
*/

SFLPoller *sfl_agent_getNextPoller(SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    /* return the one lexograpically just after it - assume they are sorted
       correctly according to the lexographical ordering of the object ids */
    SFLPoller *pl = sfl_agent_getPoller(agent, pdsi);
    return pl ? pl->nxt : NULL;
}

/*_________________---------------------------__________________
  _________________ sfl_agent_getNextReceiver __________________
  -----------------___________________________------------------
*/

SFLReceiver *sfl_agent_getNextReceiver(SFLAgent *agent, uint32_t receiverIndex)
{
    return sfl_agent_getReceiver(agent, receiverIndex + 1);
}


/*_________________---------------------------__________________
  _________________ sfl_agent_resetReceiver   __________________
  -----------------___________________________------------------
*/

void sfl_agent_resetReceiver(SFLAgent *agent, SFLReceiver *receiver)
{
    SFLReceiver *rcv;
    SFLSampler *sm;
    SFLPoller *pl;

    /* tell samplers and pollers to stop sending to this receiver */
    /* first get his receiverIndex */
    uint32_t rcvIdx = 0;
    for( rcv = agent->receivers; rcv != NULL; rcv = rcv->nxt) {
        rcvIdx++; /* thanks to Diego Valverde for pointing out this bugfix */
        if(rcv == receiver) {
            /* now tell anyone that is using it to stop */
            for( sm = agent->samplers; sm != NULL; sm = sm->nxt)
                if(sfl_sampler_get_sFlowFsReceiver(sm) == rcvIdx) sfl_sampler_set_sFlowFsReceiver(sm, 0);
      
            for( pl = agent->pollers; pl != NULL; pl = pl->nxt)
                if(sfl_poller_get_sFlowCpReceiver(pl) == rcvIdx) sfl_poller_set_sFlowCpReceiver(pl, 0);

            break;
        }
    }
}




/* ===================================================*/
/* ===================== SAMPLER =====================*/

/*_________________--------------------------__________________
  _________________   sfl_sampler_init       __________________
  -----------------__________________________------------------
*/

void sfl_sampler_init(SFLSampler *sampler, SFLAgent *agent, SFLDataSource_instance *pdsi)
{
    /* copy the dsi in case it points to sampler->dsi, which we are about to clear.
       (Thanks to Jagjit Choudray of Force 10 Networks for pointing out this bug) */
    SFLDataSource_instance dsi = *pdsi;

    /* preserve the *nxt pointer too, in case we are resetting this poller and it is
       already part of the agent's linked list (thanks to Matt Woodly for pointing this out) */
    SFLSampler *nxtPtr = sampler->nxt;
  
    /* clear everything */
    memset(sampler, 0, sizeof(*sampler));
  
    /* restore the linked list ptr */
    sampler->nxt = nxtPtr;
  
    /* now copy in the parameters */
    sampler->agent = agent;
    sampler->dsi = dsi;
  
    /* set defaults */
    sfl_sampler_set_sFlowFsMaximumHeaderSize(sampler, SFL_DEFAULT_HEADER_SIZE);
    sfl_sampler_set_sFlowFsPacketSamplingRate(sampler, SFL_DEFAULT_SAMPLING_RATE);
}

/*_________________--------------------------__________________
  _________________       reset              __________________
  -----------------__________________________------------------
*/

static void resetSampler(SFLSampler *sampler)
{
    SFLDataSource_instance dsi = sampler->dsi;
    sfl_sampler_init(sampler, sampler->agent, &dsi);
}

/*_________________---------------------------__________________
  _________________      MIB access           __________________
  -----------------___________________________------------------
*/
uint32_t sfl_sampler_get_sFlowFsReceiver(SFLSampler *sampler) {
    return sampler->sFlowFsReceiver;
}

void sfl_sampler_set_sFlowFsReceiver(SFLSampler *sampler, uint32_t sFlowFsReceiver) {
    sampler->sFlowFsReceiver = sFlowFsReceiver;
    if(sFlowFsReceiver == 0) resetSampler(sampler);
    else {
        /* retrieve and cache a direct pointer to my receiver */
        sampler->myReceiver = sfl_agent_getReceiver(sampler->agent, sampler->sFlowFsReceiver);
    }
}

uint32_t sfl_sampler_get_sFlowFsPacketSamplingRate(SFLSampler *sampler) {
    return sampler->sFlowFsPacketSamplingRate;
}

void sfl_sampler_set_sFlowFsPacketSamplingRate(SFLSampler *sampler, uint32_t sFlowFsPacketSamplingRate) {
    sampler->sFlowFsPacketSamplingRate = sFlowFsPacketSamplingRate;
    /* initialize the skip count too */
    sampler->skip = sFlowFsPacketSamplingRate ? sfl_random(sFlowFsPacketSamplingRate) : 0;
}

uint32_t sfl_sampler_get_sFlowFsMaximumHeaderSize(SFLSampler *sampler) {
    return sampler->sFlowFsMaximumHeaderSize;
}

void sfl_sampler_set_sFlowFsMaximumHeaderSize(SFLSampler *sampler, uint32_t sFlowFsMaximumHeaderSize) {
    sampler->sFlowFsMaximumHeaderSize = sFlowFsMaximumHeaderSize;
}

/* call this to set a maximum samples-per-second threshold. If the sampler reaches this
   threshold it will automatically back off the sampling rate. A value of 0 disables the
   mechanism */

void sfl_sampler_set_backoffThreshold(SFLSampler *sampler, uint32_t samplesPerSecond) {
    sampler->backoffThreshold = samplesPerSecond;
}

uint32_t sfl_sampler_get_backoffThreshold(SFLSampler *sampler) {
    return sampler->backoffThreshold;
}

uint32_t sfl_sampler_get_samplesLastTick(SFLSampler *sampler) {
    return sampler->samplesLastTick;
}

/*_________________---------------------------------__________________
  _________________   sequence number reset         __________________
  -----------------_________________________________------------------
  Used by the agent to indicate a samplePool discontinuity
  so that the sflow collector will know to ignore the next delta.
*/
void sfl_sampler_resetFlowSeqNo(SFLSampler *sampler) { sampler->flowSampleSeqNo = 0; }


/*_________________---------------------------__________________
  _________________    sfl_sampler_tick       __________________
  -----------------___________________________------------------
*/

void sfl_sampler_tick(SFLSampler *sampler, time_t now)
{
    if(sampler->backoffThreshold && sampler->samplesThisTick > sampler->backoffThreshold) {
        /* automatic backoff.  If using hardware sampling then this is where you have to */
        /* call out to change the sampling rate and make sure that any other registers/variables */
        /* that hold this value are updated. */
        sampler->sFlowFsPacketSamplingRate *= 2;
    }
    sampler->samplesLastTick = sampler->samplesThisTick;
    sampler->samplesThisTick = 0;
}



/*_________________------------------------------__________________
  _________________ sfl_sampler_writeFlowSample  __________________
  -----------------______________________________------------------
*/

void sfl_sampler_writeFlowSample(SFLSampler *sampler, SFL_FLOW_SAMPLE_TYPE *fs)
{
    if(fs == NULL) return;
    sampler->samplesThisTick++;
    /* increment the sequence number */
    fs->sequence_number = ++sampler->flowSampleSeqNo;
    /* copy the other header fields in */
#ifdef SFL_USE_32BIT_INDEX
    fs->ds_class = SFL_DS_CLASS(sampler->dsi);
    fs->ds_index = SFL_DS_INDEX(sampler->dsi);
#else
    fs->source_id = SFL_DS_DATASOURCE(sampler->dsi);
#endif
    /* the sampling rate may have been set already. */
    if(fs->sampling_rate == 0) fs->sampling_rate = sampler->sFlowFsPacketSamplingRate;
    /* the samplePool may be maintained upstream too. */
    if(fs->sample_pool == 0) fs->sample_pool = sampler->samplePool;
    /* and the same for the drop event counter */
    if(fs->drops == 0) fs->drops = sampler->dropEvents;
    /* sent to my receiver */
    if(sampler->myReceiver) sfl_receiver_writeFlowSample(sampler->myReceiver, fs);
}

/*_________________-------------------------------------__________________
  _________________ sfl_sampler_writeEncodedFlowSample  __________________
  -----------------_____________________________________------------------
*/

void sfl_sampler_writeEncodedFlowSample(SFLSampler *sampler, char *xdrBytes, uint32_t len)
{
    SFL_FLOW_SAMPLE_TYPE fs = { 0 };
    sampler->samplesThisTick++;
    /* increment the sequence number */
    fs.sequence_number = ++sampler->flowSampleSeqNo;
    /* copy the other header fields in */
#ifdef SFL_USE_32BIT_INDEX
    fs.ds_class = SFL_DS_CLASS(sampler->dsi);
    fs.ds_index = SFL_DS_INDEX(sampler->dsi);
#else
    fs.source_id = SFL_DS_DATASOURCE(sampler->dsi);
#endif
    fs.sampling_rate = sampler->sFlowFsPacketSamplingRate;
    fs.sample_pool = sampler->samplePool;
    fs.drops = sampler->dropEvents;
    if(sampler->myReceiver) sfl_receiver_writeEncodedFlowSample(sampler->myReceiver, &fs, xdrBytes, len);
}

/*_________________---------------------------__________________
  _________________     sfl_random            __________________
  -----------------___________________________------------------
  Gerhard's generator
*/

static uint32_t SFLRandom = 1;

uint32_t sfl_random(uint32_t lim) {
    SFLRandom = ((SFLRandom * 32719) + 3) % 32749;
    return ((SFLRandom % lim) + 1);
} 

void sfl_random_init(uint32_t seed) {
    SFLRandom = seed;
} 

/*_________________---------------------------__________________
  _________________  sfl_sampler_takeSample   __________________
  -----------------___________________________------------------
*/

int sfl_sampler_takeSample(SFLSampler *sampler)
{
    /* increment the samplePool */
    sampler->samplePool++;

    if(--sampler->skip == 0) {
        /* reached zero. Set the next skip and return true. */
        sampler->skip = sfl_random((2 * sampler->sFlowFsPacketSamplingRate) - 1);
        return 1;
    }
    return 0;
}



/* ===================================================*/
/* ===================== POLLER ======================*/

/*_________________--------------------------__________________
  _________________    sfl_poller_init       __________________
  -----------------__________________________------------------
*/

void sfl_poller_init(SFLPoller *poller,
                     SFLAgent *agent,
                     SFLDataSource_instance *pdsi,
                     void *magic,         /* ptr to pass back in getCountersFn() */
                     getCountersFn_t getCountersFn)
{
    /* copy the dsi in case it points to poller->dsi, which we are about to clear */
    SFLDataSource_instance dsi = *pdsi;

    /* preserve the *nxt pointer too, in case we are resetting this poller and it is
       already part of the agent's linked list (thanks to Matt Woodly for pointing this out) */
    SFLPoller *nxtPtr = poller->nxt;

    /* clear everything */
    memset(poller, 0, sizeof(*poller));
  
    /* restore the linked list ptr */
    poller->nxt = nxtPtr;
  
    /* now copy in the parameters */
    poller->agent = agent;
    poller->dsi = dsi; /* structure copy */
    poller->magic = magic;
    poller->getCountersFn = getCountersFn;
}

/*_________________--------------------------__________________
  _________________       reset              __________________
  -----------------__________________________------------------
*/

static void resetPoller(SFLPoller *poller)
{
    SFLDataSource_instance dsi = poller->dsi;
    sfl_poller_init(poller, poller->agent, &dsi, poller->magic, poller->getCountersFn);
}

/*_________________---------------------------__________________
  _________________      MIB access           __________________
  -----------------___________________________------------------
*/
uint32_t sfl_poller_get_sFlowCpReceiver(SFLPoller *poller) {
    return poller->sFlowCpReceiver;
}

void sfl_poller_set_sFlowCpReceiver(SFLPoller *poller, uint32_t sFlowCpReceiver) {
    poller->sFlowCpReceiver = sFlowCpReceiver;
    if(sFlowCpReceiver == 0) resetPoller(poller);
    else {
        /* retrieve and cache a direct pointer to my receiver */
        poller->myReceiver = sfl_agent_getReceiver(poller->agent, poller->sFlowCpReceiver);
    }
}

uint32_t sfl_poller_get_sFlowCpInterval(SFLPoller *poller) {
    return (uint32_t)poller->sFlowCpInterval;
}

void sfl_poller_set_sFlowCpInterval(SFLPoller *poller, uint32_t sFlowCpInterval) {
    poller->sFlowCpInterval = sFlowCpInterval;
    /* Set the countersCountdown to be a randomly selected value between 1 and
       sFlowCpInterval. That way the counter polling would be desynchronised
       (on a 200-port switch, polling all the counters in one second could be harmful). */
    poller->countersCountdown = sfl_random(sFlowCpInterval);
}

/*_________________---------------------------------__________________
  _________________   sequence number reset         __________________
  -----------------_________________________________------------------
  Used to indicate a counter discontinuity
  so that the sflow collector will know to ignore the next delta.
*/
void sfl_poller_resetCountersSeqNo(SFLPoller *poller) {  poller->countersSampleSeqNo = 0; }

/*_________________---------------------------__________________
  _________________    sfl_poller_tick        __________________
  -----------------___________________________------------------
*/

void sfl_poller_tick(SFLPoller *poller, time_t now)
{
    if(poller->countersCountdown == 0) return; /* counters retrieval was not enabled */
    if(poller->sFlowCpReceiver == 0) return;

    if(--poller->countersCountdown == 0) {
        if(poller->getCountersFn != NULL) {
            /* call out for counters */
            SFL_COUNTERS_SAMPLE_TYPE cs;
            memset(&cs, 0, sizeof(cs));
            poller->getCountersFn(poller->magic, poller, &cs);
            /* this countersFn is expected to fill in some counter block elements */
            /* and then call sfl_poller_writeCountersSample(poller, &cs); */
        }
        /* reset the countdown */
        poller->countersCountdown = poller->sFlowCpInterval;
    }
}

/*_________________---------------------------------__________________
  _________________ sfl_poller_writeCountersSample  __________________
  -----------------_________________________________------------------
*/

void sfl_poller_writeCountersSample(SFLPoller *poller, SFL_COUNTERS_SAMPLE_TYPE *cs)
{
    /* fill in the rest of the header fields, and send to the receiver */
    cs->sequence_number = ++poller->countersSampleSeqNo;
#ifdef SFL_USE_32BIT_INDEX
    cs->ds_class = SFL_DS_CLASS(poller->dsi);
    cs->ds_index = SFL_DS_INDEX(poller->dsi);
#else
    cs->source_id = SFL_DS_DATASOURCE(poller->dsi);
#endif
    /* sent to my receiver */
    if(poller->myReceiver) sfl_receiver_writeCountersSample(poller->myReceiver, cs);
}





/* ===================================================*/
/* ===================== RECEIVER ====================*/

static void resetSampleCollector(SFLReceiver *receiver);
static void sendSample(SFLReceiver *receiver);
static void receiverError(SFLReceiver *receiver, char *errm);
static void putNet32(SFLReceiver *receiver, uint32_t val);
static void putAddress(SFLReceiver *receiver, SFLAddress *addr);
static void putOpaque(SFLReceiver *receiver, char *val, int len);
#ifdef SFLOW_DO_SOCKET
static void initSocket(SFLReceiver *receiver);
#endif

/*_________________--------------------------__________________
  _________________    sfl_receiver_init     __________________
  -----------------__________________________------------------
*/

void sfl_receiver_init(SFLReceiver *receiver, SFLAgent *agent)
{
    /* first clear everything */
    memset(receiver, 0, sizeof(*receiver));

    /* now copy in the parameters */
    receiver->agent = agent;

    /* set defaults */
    receiver->sFlowRcvrMaximumDatagramSize = SFL_DEFAULT_DATAGRAM_SIZE;
    receiver->sFlowRcvrPort = SFL_DEFAULT_COLLECTOR_PORT;

#ifdef SFLOW_DO_SOCKET
    /* initialize the socket address */
    initSocket(receiver);
#endif

    /* prepare to receive the first sample */
    resetSampleCollector(receiver);
}

/*_________________---------------------------__________________
  _________________      reset                __________________
  -----------------___________________________------------------

  called on timeout, or when owner string is cleared
*/

static void resetReceiver(SFLReceiver *receiver) {
    /* ask agent to tell samplers and pollers to stop sending samples */
    sfl_agent_resetReceiver(receiver->agent, receiver);
    /* reinitialize */
    sfl_receiver_init(receiver, receiver->agent);
}

#ifdef SFLOW_DO_SOCKET
/*_________________---------------------------__________________
  _________________      initSocket           __________________
  -----------------___________________________------------------
*/

static void initSocket(SFLReceiver *receiver) {
    if(receiver->sFlowRcvrAddress.type == SFLADDRESSTYPE_IP_V6) {
        struct sockaddr_in6 *sa6 = &receiver->receiver6;
        sa6->sin6_port = htons((uint16_t)receiver->sFlowRcvrPort);
        sa6->sin6_family = AF_INET6;
        sa6->sin6_addr = receiver->sFlowRcvrAddress.address.ip_v6;
    }
    else {
        struct sockaddr_in *sa4 = &receiver->receiver4;
        sa4->sin_port = htons((uint16_t)receiver->sFlowRcvrPort);
        sa4->sin_family = AF_INET;
        sa4->sin_addr = receiver->sFlowRcvrAddress.address.ip_v4;
    }
}
#endif

/*_________________----------------------------------------_____________
  _________________          MIB Vars                      _____________
  -----------------________________________________________-------------
*/

char * sfl_receiver_get_sFlowRcvrOwner(SFLReceiver *receiver) {
    return receiver->sFlowRcvrOwner;
}
void sfl_receiver_set_sFlowRcvrOwner(SFLReceiver *receiver, char *sFlowRcvrOwner) {
    receiver->sFlowRcvrOwner = sFlowRcvrOwner;
    if(sFlowRcvrOwner == NULL || sFlowRcvrOwner[0] == '\0') {
        /* reset condition! owner string was cleared */
        resetReceiver(receiver);
    }
}
time_t sfl_receiver_get_sFlowRcvrTimeout(SFLReceiver *receiver) {
    return receiver->sFlowRcvrTimeout;
}
void sfl_receiver_set_sFlowRcvrTimeout(SFLReceiver *receiver, time_t sFlowRcvrTimeout) {
    receiver->sFlowRcvrTimeout =sFlowRcvrTimeout;
} 
uint32_t sfl_receiver_get_sFlowRcvrMaximumDatagramSize(SFLReceiver *receiver) {
    return receiver->sFlowRcvrMaximumDatagramSize;
}
void sfl_receiver_set_sFlowRcvrMaximumDatagramSize(SFLReceiver *receiver, uint32_t sFlowRcvrMaximumDatagramSize) {
    uint32_t mdz = sFlowRcvrMaximumDatagramSize;
    if(mdz < SFL_MIN_DATAGRAM_SIZE) mdz = SFL_MIN_DATAGRAM_SIZE;
    receiver->sFlowRcvrMaximumDatagramSize = mdz;
}
SFLAddress *sfl_receiver_get_sFlowRcvrAddress(SFLReceiver *receiver) {
    return &receiver->sFlowRcvrAddress;
}
void sfl_receiver_set_sFlowRcvrAddress(SFLReceiver *receiver, SFLAddress *sFlowRcvrAddress) {
    if(sFlowRcvrAddress) receiver->sFlowRcvrAddress = *sFlowRcvrAddress; /* structure copy */
#ifdef SFLOW_DO_SOCKET
    initSocket(receiver);
#endif
}
uint32_t sfl_receiver_get_sFlowRcvrPort(SFLReceiver *receiver) {
    return receiver->sFlowRcvrPort;
}
void sfl_receiver_set_sFlowRcvrPort(SFLReceiver *receiver, uint32_t sFlowRcvrPort) {
    receiver->sFlowRcvrPort = sFlowRcvrPort;
    /* update the socket structure */
#ifdef SFLOW_DO_SOCKET
    initSocket(receiver);
#endif
}

/*_________________---------------------------__________________
  _________________   sfl_receiver_tick       __________________
  -----------------___________________________------------------
*/

void sfl_receiver_tick(SFLReceiver *receiver, time_t now)
{
    /* if there are any samples to send, flush them now */
    if(receiver->sampleCollector.numSamples > 0) sendSample(receiver);
    /* check the timeout */
    if(receiver->sFlowRcvrTimeout && (uint32_t)receiver->sFlowRcvrTimeout != 0xFFFFFFFF) {
        /* count down one tick and reset if we reach 0 */
        if(--receiver->sFlowRcvrTimeout == 0) resetReceiver(receiver);
    }
}

/*_________________-----------------------------__________________
  _________________   receiver write utilities  __________________
  -----------------_____________________________------------------
*/
 
static void put32(SFLReceiver *receiver, uint32_t val)
{
    *receiver->sampleCollector.datap++ = val;
}

static void putNet32(SFLReceiver *receiver, uint32_t val)
{
    *receiver->sampleCollector.datap++ = htonl(val);
}

static void putNet64(SFLReceiver *receiver, uint64_t val64)
{
    uint32_t *firstQuadPtr = receiver->sampleCollector.datap;
    /* first copy the bytes in */
    memcpy((u_char *)firstQuadPtr, &val64, 8);
    if(htonl(1) != 1) {
        /* swap the bytes, and reverse the quads too */
        uint32_t tmp = *receiver->sampleCollector.datap++;
        *firstQuadPtr = htonl(*receiver->sampleCollector.datap);
        *receiver->sampleCollector.datap++ = htonl(tmp);
    }
    else receiver->sampleCollector.datap += 2;
}

static void put128(SFLReceiver *receiver, u_char *val)
{
    memcpy(receiver->sampleCollector.datap, val, 16);
    receiver->sampleCollector.datap += 4;
}

static void putString(SFLReceiver *receiver, SFLString *s)
{
    putNet32(receiver, s->len);
    memcpy(receiver->sampleCollector.datap, s->str, s->len);
    receiver->sampleCollector.datap += (s->len + 3) / 4; /* pad to 4-byte boundary */
}

static uint32_t stringEncodingLength(SFLString *s) {
    /* answer in bytes,  so remember to mulitply by 4 after rounding up to nearest 4-byte boundary */
    return 4 + (((s->len + 3) / 4) * 4);
}

static void putAddress(SFLReceiver *receiver, SFLAddress *addr)
{
    /* encode unspecified addresses as IPV4:0.0.0.0 - or should we flag this as an error? */
    if(addr->type == 0) {
        putNet32(receiver, SFLADDRESSTYPE_IP_V4);
        put32(receiver, 0);
    }
    else {
        putNet32(receiver, addr->type);
        if(addr->type == SFLADDRESSTYPE_IP_V4) put32(receiver, addr->address.ip_v4.addr);
        else put128(receiver, addr->address.ip_v6.addr);
    }
}

static void putOpaque(SFLReceiver *receiver, char *val, int len)
{
    memcpy((char *)receiver->sampleCollector.datap, val, len);
    receiver->sampleCollector.datap += ((len+3)/4);
}

static uint32_t memcacheOpEncodingLength(SFLSampled_memcache *mcop) {
    uint32_t elemSiz = stringEncodingLength(&mcop->key);
    elemSiz += 24; /* protocol, cmd, nkeys, value_bytes, duration_uS, status */
    return elemSiz;
}

static uint32_t httpOpEncodingLength(SFLSampled_http *op) {
  uint32_t elemSiz = stringEncodingLength(&op->uri);
  elemSiz += stringEncodingLength(&op->host);
  elemSiz += stringEncodingLength(&op->referrer);
  elemSiz += stringEncodingLength(&op->useragent);
  elemSiz += stringEncodingLength(&op->authuser);
  elemSiz += stringEncodingLength(&op->mimetype);
  elemSiz += 24; /* method, protocol, bytes, uS, status */
  return elemSiz;
}

static void putSocket4(SFLReceiver *receiver, SFLExtended_socket_ipv4 *socket4) {
    putNet32(receiver, socket4->protocol);
    put32(receiver, socket4->local_ip.addr);
    put32(receiver, socket4->remote_ip.addr);
    putNet32(receiver, socket4->local_port);
    putNet32(receiver, socket4->remote_port);
}

static void putSocket6(SFLReceiver *receiver, SFLExtended_socket_ipv6 *socket6) {
    putNet32(receiver, socket6->protocol);
    put128(receiver, socket6->local_ip.addr);
    put128(receiver, socket6->remote_ip.addr);
    putNet32(receiver, socket6->local_port);
    putNet32(receiver, socket6->remote_port);
}


/*_________________-----------------------------__________________
  _________________      computeFlowSampleSize  __________________
  -----------------_____________________________------------------
*/

static int computeFlowSampleSize(SFLReceiver *receiver, SFL_FLOW_SAMPLE_TYPE *fs)
{
    SFLFlow_sample_element *elem;
    uint32_t elemSiz;
#ifdef SFL_USE_32BIT_INDEX
    uint siz = 52; /* tag, length, sequence_number, ds_class, ds_index, sampling_rate,
                      sample_pool, drops, inputFormat, input, outputFormat, output, number of elements */
#else
    uint32_t siz = 40; /* tag, length, sequence_number, source_id, sampling_rate,
                          sample_pool, drops, input, output, number of elements */
#endif

    /* hard code the wire-encoding sizes, in case the structures are expanded to be 64-bit aligned */

    fs->num_elements = 0; /* we're going to count them again even if this was set by the client */
    for(elem = fs->elements; elem != NULL; elem = elem->nxt) {
        fs->num_elements++;
        siz += 8; /* tag, length */
        elemSiz = 0;
        switch(elem->tag) {
        case SFLFLOW_MEMCACHE: elemSiz = memcacheOpEncodingLength(&elem->flowType.memcache);  break;
        case SFLFLOW_HTTP: elemSiz = httpOpEncodingLength(&elem->flowType.http);  break;
        case SFLFLOW_EX_SOCKET4: elemSiz = XDRSIZ_SFLEXTENDED_SOCKET4;  break;
        case SFLFLOW_EX_SOCKET6: elemSiz = XDRSIZ_SFLEXTENDED_SOCKET6;  break;
        default:
            {
                char errm[128];
                sprintf(errm, "computeFlowSampleSize(): unexpected tag (%u)", elem->tag);
                receiverError(receiver, errm);
                return -1;
            }
            break;
        }
        /* cache the element size, and accumulate it into the overall FlowSample size */
        elem->length = elemSiz;
        siz += elemSiz;
    }

    return siz;
}

/*_________________-------------------------------__________________
  _________________ sfl_receiver_writeFlowSample  __________________
  -----------------_______________________________------------------
*/

int sfl_receiver_writeFlowSample(SFLReceiver *receiver, SFL_FLOW_SAMPLE_TYPE *fs)
{
    int packedSize;
    SFLFlow_sample_element *elem;
    uint32_t encodingSize;

    if(fs == NULL) return -1;
    if((packedSize = computeFlowSampleSize(receiver, fs)) == -1) return -1;

    /* check in case this one sample alone is too big for the datagram */
    if(packedSize > (int)(receiver->sFlowRcvrMaximumDatagramSize)) {
        receiverError(receiver, "flow sample too big for datagram");
        return -1;
    }

    /* if the sample pkt is full enough so that this sample might put */
    /* it over the limit, then we should send it now before going on. */
    if((receiver->sampleCollector.pktlen + packedSize) >= receiver->sFlowRcvrMaximumDatagramSize)
        sendSample(receiver);
    
    receiver->sampleCollector.numSamples++;

#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, SFLFLOW_SAMPLE_EXPANDED);
#else
    putNet32(receiver, SFLFLOW_SAMPLE);
#endif

    putNet32(receiver, packedSize - 8); /* don't include tag and len */
    putNet32(receiver, fs->sequence_number);

#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, fs->ds_class);
    putNet32(receiver, fs->ds_index);
#else
    putNet32(receiver, fs->source_id);
#endif

    putNet32(receiver, fs->sampling_rate);
    putNet32(receiver, fs->sample_pool);
    putNet32(receiver, fs->drops);

#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, fs->inputFormat);
    putNet32(receiver, fs->input);
    putNet32(receiver, fs->outputFormat);
    putNet32(receiver, fs->output);
#else
    putNet32(receiver, fs->input);
    putNet32(receiver, fs->output);
#endif

    putNet32(receiver, fs->num_elements);

    for(elem = fs->elements; elem != NULL; elem = elem->nxt) {

        putNet32(receiver, elem->tag);
        putNet32(receiver, elem->length); /* length cached in computeFlowSampleSize() */

        switch(elem->tag) {
        case SFLFLOW_EX_SOCKET4: putSocket4(receiver, &elem->flowType.socket4); break;
        case SFLFLOW_EX_SOCKET6: putSocket6(receiver, &elem->flowType.socket6); break;
        case SFLFLOW_MEMCACHE:
            putNet32(receiver, elem->flowType.memcache.protocol);
            putNet32(receiver, elem->flowType.memcache.command);
            putString(receiver, &elem->flowType.memcache.key);
            putNet32(receiver, elem->flowType.memcache.nkeys);
            putNet32(receiver, elem->flowType.memcache.value_bytes);
            putNet32(receiver, elem->flowType.memcache.duration_uS);
            putNet32(receiver, elem->flowType.memcache.status);
            break;
        case SFLFLOW_HTTP:
            putNet32(receiver, elem->flowType.http.method);
            putNet32(receiver, elem->flowType.http.protocol);
            putString(receiver, &elem->flowType.http.uri);
            putString(receiver, &elem->flowType.http.host);
            putString(receiver, &elem->flowType.http.referrer);
            putString(receiver, &elem->flowType.http.useragent);
            putString(receiver, &elem->flowType.http.authuser);
            putString(receiver, &elem->flowType.http.mimetype);
            putNet64(receiver, elem->flowType.http.bytes);
            putNet32(receiver, elem->flowType.http.uS);
            putNet32(receiver, elem->flowType.http.status);
            break;
        default:
            {
                char errm[128];
                sprintf(errm, "sfl_receiver_writeFlowSample: unexpected tag (%u)", elem->tag);
                receiverError(receiver, errm);
                return -1;
            }
            break;
        }
    }

    /* sanity check */
    encodingSize = (u_char *)receiver->sampleCollector.datap
        - (u_char *)receiver->sampleCollector.data
        - receiver->sampleCollector.pktlen;

    if(encodingSize != (uint32_t)packedSize) {
        char errm[128];
        sprintf(errm, "sfl_receiver_writeFlowSample: encoding_size(%u) != expected_size(%u)",
                encodingSize,
                packedSize);
        receiverError(receiver, errm);
        return -1;
    }
      
    /* update the pktlen */
    receiver->sampleCollector.pktlen = (u_char *)receiver->sampleCollector.datap - (u_char *)receiver->sampleCollector.data;
    return packedSize;
}

/*_________________--------------------------------------__________________
  _________________ sfl_receiver_writeEncodedFlowSample  __________________
  -----------------______________________________________------------------
*/

int sfl_receiver_writeEncodedFlowSample(SFLReceiver *receiver, SFL_FLOW_SAMPLE_TYPE *fs, char *xdrBytes, uint32_t packedSize)
{
    uint32_t encodingSize;
    uint32_t overrideEncodingSize;
    uint32_t xdrHdrStrip;

    /* check in case this one sample alone is too big for the datagram */
    if(packedSize > (int)(receiver->sFlowRcvrMaximumDatagramSize)) {
        receiverError(receiver, "flow sample too big for datagram");
        return -1;
    }

    /* if the sample pkt is full enough so that this sample might put */
    /* it over the limit, then we should send it now before going on. */
    if((receiver->sampleCollector.pktlen + packedSize) >= receiver->sFlowRcvrMaximumDatagramSize)
        sendSample(receiver);
    
    receiver->sampleCollector.numSamples++;

#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, SFLFLOW_SAMPLE_EXPANDED);
#else
    putNet32(receiver, SFLFLOW_SAMPLE);
#endif

    putNet32(receiver, packedSize - 8); /* don't include tag and len bytes in the length */
    putNet32(receiver, fs->sequence_number);

#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, fs->ds_class);
    putNet32(receiver, fs->ds_index);
#else
    putNet32(receiver, fs->source_id);
#endif

    putNet32(receiver, fs->sampling_rate);
    putNet32(receiver, fs->sample_pool);
    putNet32(receiver, fs->drops);

    /* sanity check */
    overrideEncodingSize = (u_char *)receiver->sampleCollector.datap
        - (u_char *)receiver->sampleCollector.data
        - receiver->sampleCollector.pktlen;

#ifdef SFL_USE_32BIT_INDEX
    xdrHdrStrip = 32; /* tag, length, sequence_number, ds_class, ds_index, sampling_rate,
                         sample_pool, drops, [inputFormat, input, outputFormat, output, number of elements...] */
#else
    xdrHdrStrip = 28; /* tag, length, sequence_number, source_id, sampling_rate,
                         sample_pool, drops, [input, output, number of elements...] */
#endif

    memcpy(receiver->sampleCollector.datap, xdrBytes + xdrHdrStrip, packedSize - xdrHdrStrip);
    receiver->sampleCollector.datap += ((packedSize - xdrHdrStrip) >> 2);

    /* sanity check */
    encodingSize = (u_char *)receiver->sampleCollector.datap
        - (u_char *)receiver->sampleCollector.data
        - receiver->sampleCollector.pktlen;

    if(encodingSize != (uint32_t)packedSize) {
        char errm[128];
        sprintf(errm, "sfl_receiver_writeEncodedFlowSample: encoding_size(%u) != expected_size(%u) [overrideEncodingSize=%u xdrHeaderStrip=%u pktlen=%u]",
                encodingSize,
                packedSize,
                overrideEncodingSize,
                xdrHdrStrip,
                receiver->sampleCollector.pktlen);
        receiverError(receiver, errm);
        return -1;
    }
      
    /* update the pktlen */
    receiver->sampleCollector.pktlen = (u_char *)receiver->sampleCollector.datap - (u_char *)receiver->sampleCollector.data;
    return packedSize;
}

/*_________________-----------------------------__________________
  _________________ computeCountersSampleSize   __________________
  -----------------_____________________________------------------
*/

static int computeCountersSampleSize(SFLReceiver *receiver, SFL_COUNTERS_SAMPLE_TYPE *cs)
{
    SFLCounters_sample_element *elem;
    uint32_t elemSiz;

#ifdef SFL_USE_32BIT_INDEX
    uint siz = 24; /* tag, length, sequence_number, ds_class, ds_index, number of elements */
#else
    uint32_t siz = 20; /* tag, length, sequence_number, source_id, number of elements */
#endif

    cs->num_elements = 0; /* we're going to count them again even if this was set by the client */
    for( elem = cs->elements; elem != NULL; elem = elem->nxt) {
        cs->num_elements++;
        siz += 8; /* tag, length */
        elemSiz = 0;

        /* hard code the wire-encoding sizes rather than use sizeof() -- in case the
           structures are expanded to be 64-bit aligned */

        switch(elem->tag) {
        case SFLCOUNTERS_MEMCACHE: elemSiz = XDRSIZ_SFLMEMCACHE_COUNTERS /*sizeof(elem->counterBlock.memcache)*/;  break;
        case SFLCOUNTERS_HTTP: elemSiz = XDRSIZ_SFLHTTP_COUNTERS /*sizeof(elem->counterBlock.http)*/;  break;
        default:
            {
                char errm[128];
                sprintf(errm, "computeCounterSampleSize(): unexpected counters tag (%u)", elem->tag);
                receiverError(receiver, errm);
                return -1;
            }
            break;
        }
        /* cache the element size, and accumulate it into the overall FlowSample size */
        elem->length = elemSiz;
        siz += elemSiz;
    }
    return siz;
}

/*_________________----------------------------------__________________
  _________________ sfl_receiver_writeCountersSample __________________
  -----------------__________________________________------------------
*/

int sfl_receiver_writeCountersSample(SFLReceiver *receiver, SFL_COUNTERS_SAMPLE_TYPE *cs)
{
    int packedSize;
    SFLCounters_sample_element *elem;
    uint32_t encodingSize;

    if(cs == NULL) return -1;
    /* if the sample pkt is full enough so that this sample might put */
    /* it over the limit, then we should send it now. */
    if((packedSize = computeCountersSampleSize(receiver, cs)) == -1) return -1;
  
    /* check in case this one sample alone is too big for the datagram */
    /* in fact - if it is even half as big then we should ditch it. Very */
    /* important to avoid overruning the packet buffer. */
    if(packedSize > (int)(receiver->sFlowRcvrMaximumDatagramSize / 2)) {
        receiverError(receiver, "counters sample too big for datagram");
        return -1;
    }
  
    if((receiver->sampleCollector.pktlen + packedSize) >= receiver->sFlowRcvrMaximumDatagramSize)
        sendSample(receiver);
  
    receiver->sampleCollector.numSamples++;
  
#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, SFLCOUNTERS_SAMPLE_EXPANDED);
#else
    putNet32(receiver, SFLCOUNTERS_SAMPLE);
#endif

    putNet32(receiver, packedSize - 8); /* tag and length not included */
    putNet32(receiver, cs->sequence_number);

#ifdef SFL_USE_32BIT_INDEX
    putNet32(receiver, cs->ds_class);
    putNet32(receiver, cs->ds_index);
#else
    putNet32(receiver, cs->source_id);
#endif

    putNet32(receiver, cs->num_elements);
  
    for(elem = cs->elements; elem != NULL; elem = elem->nxt) {
    
        putNet32(receiver, elem->tag);
        putNet32(receiver, elem->length); /* length cached in computeCountersSampleSize() */
    
        switch(elem->tag) {
        case SFLCOUNTERS_MEMCACHE:
            putNet32(receiver, elem->counterBlock.memcache.uptime);
            putNet32(receiver, elem->counterBlock.memcache.rusage_user);
            putNet32(receiver, elem->counterBlock.memcache.rusage_system);
            putNet32(receiver, elem->counterBlock.memcache.curr_connections);
            putNet32(receiver, elem->counterBlock.memcache.total_connections);
            putNet32(receiver, elem->counterBlock.memcache.connection_structures);
            putNet32(receiver, elem->counterBlock.memcache.cmd_get);
            putNet32(receiver, elem->counterBlock.memcache.cmd_set);
            putNet32(receiver, elem->counterBlock.memcache.cmd_flush);
            putNet32(receiver, elem->counterBlock.memcache.get_hits);
            putNet32(receiver, elem->counterBlock.memcache.get_misses);
            putNet32(receiver, elem->counterBlock.memcache.delete_misses);
            putNet32(receiver, elem->counterBlock.memcache.delete_hits);
            putNet32(receiver, elem->counterBlock.memcache.incr_misses);
            putNet32(receiver, elem->counterBlock.memcache.incr_hits);
            putNet32(receiver, elem->counterBlock.memcache.decr_misses);
            putNet32(receiver, elem->counterBlock.memcache.decr_hits);
            putNet32(receiver, elem->counterBlock.memcache.cas_misses);
            putNet32(receiver, elem->counterBlock.memcache.cas_hits);
            putNet32(receiver, elem->counterBlock.memcache.cas_badval);
            putNet32(receiver, elem->counterBlock.memcache.auth_cmds);
            putNet32(receiver, elem->counterBlock.memcache.auth_errors);
            putNet64(receiver, elem->counterBlock.memcache.bytes_read);
            putNet64(receiver, elem->counterBlock.memcache.bytes_written);
            putNet32(receiver, elem->counterBlock.memcache.limit_maxbytes);
            putNet32(receiver, elem->counterBlock.memcache.accepting_conns);
            putNet32(receiver, elem->counterBlock.memcache.listen_disabled_num);
            putNet32(receiver, elem->counterBlock.memcache.threads);
            putNet32(receiver, elem->counterBlock.memcache.conn_yields);
            putNet64(receiver, elem->counterBlock.memcache.bytes);
            putNet32(receiver, elem->counterBlock.memcache.curr_items);
            putNet32(receiver, elem->counterBlock.memcache.total_items);
            putNet32(receiver, elem->counterBlock.memcache.evictions);
            break;
        case SFLCOUNTERS_HTTP:
            putNet32(receiver, elem->counterBlock.http.method_option_count);
            putNet32(receiver, elem->counterBlock.http.method_get_count);
            putNet32(receiver, elem->counterBlock.http.method_head_count);
            putNet32(receiver, elem->counterBlock.http.method_post_count);
            putNet32(receiver, elem->counterBlock.http.method_put_count);
            putNet32(receiver, elem->counterBlock.http.method_delete_count);
            putNet32(receiver, elem->counterBlock.http.method_trace_count);
            putNet32(receiver, elem->counterBlock.http.method_connect_count);
            putNet32(receiver, elem->counterBlock.http.method_other_count);
            putNet32(receiver, elem->counterBlock.http.status_1XX_count);
            putNet32(receiver, elem->counterBlock.http.status_2XX_count);
            putNet32(receiver, elem->counterBlock.http.status_3XX_count);
            putNet32(receiver, elem->counterBlock.http.status_4XX_count);
            putNet32(receiver, elem->counterBlock.http.status_5XX_count);
            putNet32(receiver, elem->counterBlock.http.status_other_count);
            break;
        default:
            {
                char errm[128];
                sprintf(errm, "unexpected counters tag (%u)", elem->tag);
                receiverError(receiver, errm);
                return -1;
            }
            break;
        }
    }
    /* sanity check */
    encodingSize = (u_char *)receiver->sampleCollector.datap
        - (u_char *)receiver->sampleCollector.data
        - receiver->sampleCollector.pktlen;
    if(encodingSize != (uint32_t)packedSize) {
        char errm[128];
        sprintf(errm, "sfl_receiver_writeCountersSample: encoding_size(%u) != expected_size(%u)",
                encodingSize,
                packedSize);
        receiverError(receiver, errm);
        return -1;
    }

    /* update the pktlen */
    receiver->sampleCollector.pktlen = (u_char *)receiver->sampleCollector.datap - (u_char *)receiver->sampleCollector.data;
    return packedSize;
}

/*_________________---------------------------------__________________
  _________________ sfl_receiver_samplePacketsSent  __________________
  -----------------_________________________________------------------
*/

uint32_t sfl_receiver_samplePacketsSent(SFLReceiver *receiver)
{
    return receiver->sampleCollector.packetSeqNo;
}

/*_________________---------------------------__________________
  _________________     sendSample            __________________
  -----------------___________________________------------------
*/

static void sendSample(SFLReceiver *receiver)
{  
    /* construct and send out the sample, then reset for the next one... */
    SFLAgent *agent = receiver->agent;
  
    /* go back and fill in the header */
    receiver->sampleCollector.datap = receiver->sampleCollector.data;
    putNet32(receiver, SFLDATAGRAM_VERSION5);
    putAddress(receiver, &agent->myIP);
    putNet32(receiver, agent->subId);
    putNet32(receiver, ++receiver->sampleCollector.packetSeqNo);
    putNet32(receiver,  (uint32_t)((agent->now - agent->bootTime) * 1000));
    putNet32(receiver, receiver->sampleCollector.numSamples);
  
    /* send */
    if(agent->sendFn) (*agent->sendFn)(agent->magic,
                                       agent,
                                       receiver,
                                       (u_char *)receiver->sampleCollector.data, 
                                       receiver->sampleCollector.pktlen);
    else {
#ifdef SFLOW_DO_SOCKET
        /* send it myself */
        if (receiver->sFlowRcvrAddress.type == SFLADDRESSTYPE_IP_V6) {
            uint32_t soclen = sizeof(struct sockaddr_in6);
            int result = sendto(agent->receiverSocket6,
                                receiver->sampleCollector.data,
                                receiver->sampleCollector.pktlen,
                                0,
                                (struct sockaddr *)&receiver->receiver6,
                                soclen);
            if(result == -1 && errno != EINTR) sfl_agent_sysError(agent, "receiver", "IPv6 socket sendto error");
            if(result == 0) sfl_agent_error(agent, "receiver", "IPv6 socket sendto returned 0");
        }
        else {
            uint32_t soclen = sizeof(struct sockaddr_in);
            int result = sendto(agent->receiverSocket4,
                                receiver->sampleCollector.data,
                                receiver->sampleCollector.pktlen,
                                0,
                                (struct sockaddr *)&receiver->receiver4,
                                soclen);
            if(result == -1 && errno != EINTR) sfl_agent_sysError(agent, "receiver", "socket sendto error");
            if(result == 0) sfl_agent_error(agent, "receiver", "socket sendto returned 0");
        }
#endif
    }

    /* reset for the next time */
    resetSampleCollector(receiver);
}

/*_________________---------------------------__________________
  _________________   resetSampleCollector    __________________
  -----------------___________________________------------------
*/

static void resetSampleCollector(SFLReceiver *receiver)
{
    receiver->sampleCollector.pktlen = 0;
    receiver->sampleCollector.numSamples = 0;

    /* clear the buffer completely (ensures that pad bytes will always be zeros - thank you CW) */
    memset((u_char *)receiver->sampleCollector.data, 0, (SFL_SAMPLECOLLECTOR_DATA_QUADS * 4));

    /* point the datap to just after the header */
    receiver->sampleCollector.datap = (receiver->agent->myIP.type == SFLADDRESSTYPE_IP_V6) ?
        (receiver->sampleCollector.data + 10) :
        (receiver->sampleCollector.data + 7);

    /* start pktlen with the right value */
    receiver->sampleCollector.pktlen = (u_char *)receiver->sampleCollector.datap - (u_char *)receiver->sampleCollector.data;
}

/*_________________---------------------------__________________
  _________________    receiverError          __________________
  -----------------___________________________------------------
*/

static void receiverError(SFLReceiver *receiver, char *msg)
{
    sfl_agent_error(receiver->agent, "receiver", msg);
    resetSampleCollector(receiver);
}


/* ==========================================================*/
/* ==========================================================*/
/* ====================== sFlow Web ======================== */
/* ==========================================================*/


static bool lockOrDie(pthread_mutex_t *sem) {
    ap_assert(sem == NULL || pthread_mutex_lock(sem) == 0);
    return true;
}

static bool releaseOrDie(pthread_mutex_t *sem) {
    ap_assert(sem == NULL || pthread_mutex_unlock(sem) == 0);
    return true;
}

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

/* ================ master agent callbacks ===========================*/

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
    lockOrDie(sm->mutex);
    {
        
        if(!sm->configOK) {
            /* config is disabled */
            return;
        }
        
        if(sm->config.polling_secs == 0) {
            /* polling is off */
            return;
        }

        /* per-worker counters have been accumulated into this shared-memory block, so we can just submit it */
        SFLADD_ELEMENT(cs, &sm->http_counters);
        sfl_poller_writeCountersSample(poller, cs);

    }
	releaseOrDie(sm->mutex);
}

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

void sflow_sample_http(SFLSampler *sampler, struct conn_rec *connection, SFLHTTP_method method, int proto_num, const char *uri, size_t urilen, const char *host, size_t hostlen, const char *referrer, size_t referrerlen, const char *useragent, size_t useragentlen, const char *authuser, size_t authuserlen, const char *mimetype, size_t mimetypelen, uint64_t bytes, uint32_t duration_uS, uint32_t status)
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

/* ============= config file parsing ================== */
/* A separate config file is used rather than httpd/conf.d/sflow
 * because it can be written and changed dynamically by another
 * daemon that is propagating the global sFlow config (e.g. hsflowd)
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

static void sfwb_apply_config(SFWB *sm, SFWBConfig *config)
{
    if(config == NULL && !sm->configOK) {
        /* no change required */
        return;
    }

    lockOrDie(sm->mutex);
    if(config) {
        sm->config = *config; /* structure copy */
        sm->configOK = true;
    }
    else {
        sm->configOK = false;
    }
    releaseOrDie(sm->mutex);

    if(sm->configOK) {
        sflow_init(sm);
    }
}
    
/* normally a 1-second tick */
        
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

void sflow_init(SFWB *sm) {

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "in sflow_init: sFlow=%p pid=%u config=%p", sm, getpid(), sm->config);

    if(sm->configFile == NULL) {
        sm->configFile = SFWB_DEFAULT_CONFIGFILE;
    }

    if(!sm->configOK) return;

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "in sflow_init: building sFlow agent");

    lockOrDie(sm->mutex);
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
            /* IPC to the workers */
            SFWBShared *shared = (SFWBShared *)sm->shared_mem_base;
            shared->sflow_skip1 = sm->config.sampling_n;
            shared->sflow_skip2 = sm->config.sampling_n;

        }
    }
    releaseOrDie(sm->mutex);
}

/* ==========================================================*/
/* ==========================================================*/
/* ======================= mod sFlow ======================== */
/* ==========================================================*/


#define MOD_SFLOW_USERDATA_KEY "mod-sflow"
module AP_MODULE_DECLARE_DATA sflow_module;

#define GET_CONFIG_DATA(s) ap_get_module_config((s)->module_config, &sflow_module)

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
                    SFLHTTP_counters *c = (SFLHTTP_counters *)datap;
                    /* accumulate into my total */
                    sm->http_counters.counterBlock.http.method_option_count += c->method_option_count;
                    sm->http_counters.counterBlock.http.method_get_count += c->method_get_count;
                    sm->http_counters.counterBlock.http.method_head_count += c->method_head_count;
                    sm->http_counters.counterBlock.http.method_post_count += c->method_post_count;
                    sm->http_counters.counterBlock.http.method_put_count += c->method_put_count;
                    sm->http_counters.counterBlock.http.method_delete_count += c->method_delete_count;
                    sm->http_counters.counterBlock.http.method_trace_count += c->method_trace_count;
                    sm->http_counters.counterBlock.http.method_connect_count += c->method_connect_count;
                    sm->http_counters.counterBlock.http.method_other_count += c->method_other_count;
                    sm->http_counters.counterBlock.http.status_1XX_count += c->status_1XX_count;
                    sm->http_counters.counterBlock.http.status_2XX_count += c->status_2XX_count;
                    sm->http_counters.counterBlock.http.status_3XX_count += c->status_3XX_count;
                    sm->http_counters.counterBlock.http.status_4XX_count += c->status_4XX_count;
                    sm->http_counters.counterBlock.http.status_5XX_count += c->status_5XX_count;
                    sm->http_counters.counterBlock.http.status_other_count += c->status_other_count;
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


static int start_sflow_master(apr_pool_t *p, server_rec *s, SFWB *sm) {
    apr_status_t status;

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "start_sflow_master - pid=%u\n", getpid());

    /* create the pipe that the workers will use to send samples to the master */
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
    sm->shared_mem_base = apr_shm_baseaddr_get(sm->shared_mem); /* each worker must call again */

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

static void *create_sflow_config(apr_pool_t *p, server_rec *s)
{
    int rc;
    SFWB *sm = apr_pcalloc(p, sizeof(SFWB));
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "create_sflow_config - pid=%u\n", getpid());
    sm->configFile = SFWB_DEFAULT_CONFIGFILE;
    sm->enabled = true; /* could be controlled by config cmd (see below) */

    /* a mutex for sync */
    if(sm->mutex == NULL) {
        sm->mutex = (pthread_mutex_t*)apr_pcalloc(p, sizeof(pthread_mutex_t));
        pthread_mutex_init(sm->mutex, NULL);
    }

    /* a pool to use for the agent so we can recycle the memory easily on a config change */
    if((rc = apr_pool_create(&sm->masterPool, p)) != APR_SUCCESS) {
        ap_log_error(APLOG_MARK, APLOG_ERR, rc, s, "create_sflow_config: error creating sub-pool");
    }
    return sm;
}

static int sflow_pre_config(apr_pool_t *p, apr_pool_t *plog, apr_pool_t *ptemp)
{
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, NULL, "sflow_pre_config - pid=%u\n", getpid());
    return OK;
}

static int sflow_post_config(apr_pool_t *p, apr_pool_t *plog, apr_pool_t *ptemp, server_rec *s)
{
    void *flag;
    SFWB *sm = GET_CONFIG_DATA(s);

    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_post_config - pid=%u\n", getpid());

    /* All post_config hooks are called twice, we're only interested in the second call. */
    apr_pool_userdata_get(&flag, MOD_SFLOW_USERDATA_KEY, s->process->pool);
    if (!flag) {
        apr_pool_userdata_set((void*) 1, MOD_SFLOW_USERDATA_KEY, apr_pool_cleanup_null, s->process->pool);
        return OK;
    }
    
    if(sm && sm->enabled && sm->sFlowProc == NULL) {
        start_sflow_master(p, s, sm);
    }
    return OK;
}

static void *sfwb_workercb_alloc(void *magic, SFLAgent *agent, size_t bytes)
{
    SFWB *sm = (SFWB *)magic;
    return apr_pcalloc(sm->wk->workerPool, bytes);
}

static int sfwb_workercb_free(void *magic, SFLAgent *agent, void *obj)
{
    /* do nothing - we'll free the whole sub-pool when we are ready */
    return 0;
}

static void sfwb_workercb_error(void *magic, SFLAgent *agent, char *msg)
{
    ap_log_error(APLOG_MARK, APLOG_ERR, 0, NULL, "sFlow worker agent error: %s", msg);
}

static void sflow_init_worker(apr_pool_t *p, server_rec *s)
{
    int rc;
    SFWB *sm = GET_CONFIG_DATA(s);
    if(!sm->enabled) return;
    ap_log_error(APLOG_MARK, APLOG_DEBUG, 0, s, "sflow_init_worker - pid=%u\n", getpid());
    /* create my own private state, and hang it off the shared state */
    SFWBWorker *wk = (SFWBWorker *)apr_pcalloc(p, sizeof(SFWBWorker));
    sm->wk = wk;
    /* remember the config pool so the allocation callback can use it (no
       need for a sub-pool here because we don't need to recycle) */
    wk->workerPool = p;
    /* shared_mem base address - may be different for each worker, so put in private state */
    wk->shared_mem_base = apr_shm_baseaddr_get(sm->shared_mem);
    /* create my own sFlow agent+sampler+receiver just so I can use it to encode XDR messages */
    /* before sending them down the pipe */
    wk->agent = (SFLAgent *)apr_pcalloc(p, sizeof(SFLAgent));
    SFLAddress myIP = { 0 }; /* blank address */
    sfl_agent_init(wk->agent,
                   &myIP,
                   getpid(), /* subAgentId */
                   sm->currentTime,
                   sm->currentTime,
                   sm,
                   sfwb_workercb_alloc,
                   sfwb_workercb_free,
                   sfwb_workercb_error,
                   NULL);

    wk->receiver = sfl_agent_addReceiver(wk->agent);
    sfl_receiver_set_sFlowRcvrOwner(wk->receiver, "httpd sFlow Probe - worker");
    sfl_receiver_set_sFlowRcvrTimeout(wk->receiver, 0xFFFFFFFF);
    SFLDataSource_instance dsi;
    memset(&dsi, 0, sizeof(dsi)); /* will be ignored anyway */
    wk->sampler = sfl_agent_addSampler(wk->agent, &dsi);
    sfl_sampler_set_sFlowFsReceiver(wk->sampler, 1 /* receiver index*/);
    /* seed the random number generator */
    sfl_random_init(getpid());
    /* we'll pick up the sampling_rate later. Don't want to insist
     * on it being present at startup - don't want to delay the
     * startup if we can avoid it.  Just set it to 0 so we check for
     * it. Otherwise it will start out as the default (400) */
    sfl_sampler_set_sFlowFsPacketSamplingRate(wk->sampler, 0);
}

static int sflow_handler_test(request_rec *r)
{
    if (strcmp(r->handler, "sflow")) {
        return DECLINED;
    }
    r->content_type = "text/html";      

    if (!r->header_only) {
        ap_rputs("mod_sflow page", r);
        if(r->server) {
            SFWB *sm = GET_CONFIG_DATA(r->server);
            if(sm) {
                /* could add some stats output here, but it would have to
                   be written back into the shared memory before we could see it */
            }
        }
    }

    return OK;
}

static int sflow_handler(request_rec *r)
{
    ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "sflow_handler - pid=%u\n", getpid());
    return OK;
}

static int read_shared_sampling_n(SFWBWorker *wk)
{
    SFWBShared *shared = (SFWBShared *)wk->shared_mem_base;
    /* read it twice to avoid requiring a lock (does this work?) */
    uint32_t sflow_skip1 = shared->sflow_skip1;
    uint32_t sflow_skip2 = shared->sflow_skip2;
    /* if it's not stable, just use the value we had before */
    return (sflow_skip1 == sflow_skip2) ? sflow_skip1 : -1;
}

static void sflow_set_random_skip(SFWBWorker *wk)
{
    int n = read_shared_sampling_n(wk);
    if(n >= 0) {
        /* got a valid setting */
        if(n != sfl_sampler_get_sFlowFsPacketSamplingRate(wk->sampler)) {
            /* it has changed */
            sfl_sampler_set_sFlowFsPacketSamplingRate(wk->sampler, n);
        }
    }
}

static int my_strlen(const char *s) { return s ? strlen(s) : 0; }

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

static int sflow_multi_log_transaction(request_rec *r)
{
    int n;
    SFWB *sm = GET_CONFIG_DATA(r->server);
    SFWBWorker *wk = sm->wk;
    apr_time_t now_uS = apr_time_now();
    ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "sflow_multi_log_transaction (sampler->skip=%u)\n", wk->sampler->skip);
    uint32_t method = r->header_only ? SFHTTP_HEAD : methodNumberLookup(r->method_number);
    SFLHTTP_counters *ctrs = &wk->http_counters.counterBlock.http;
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
   
    if(sfl_sampler_get_sFlowFsPacketSamplingRate(wk->sampler) == 0) {
        /* don't have a sampling-rate setting yet. Check to see... */
        sflow_set_random_skip(wk);
    }
    else if(sfl_sampler_takeSample(wk->sampler)) {
        ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "sflow take sample: r->method_number=%u\n", r->method_number);
        /* point to the start of the datagram */
        uint32_t *msg = wk->receiver->sampleCollector.datap;
        /* msglen, msgType, sample pool and drops */
        put32(wk->receiver, 0); /* we'll come back and fill this in later */
        put32(wk->receiver, SFLFLOW_SAMPLE);
        put32(wk->receiver, SFLFLOW_HTTP);
        put32(wk->receiver, wk->sampler->samplePool);
        put32(wk->receiver, wk->sampler->dropEvents);
        /* and reset so they can be accumulated by the other process */
        wk->sampler->samplePool = 0;
        wk->sampler->dropEvents = 0;
        /* accumulate the pktlen here too, to satisfy a sanity-check in the sflow library (receiver) */
        wk->receiver->sampleCollector.pktlen += 20;

        const char *referer = apr_table_get(r->headers_in, "Referer");
        const char *useragent = apr_table_get(r->headers_in, "User-Agent");
        const char *contentType = apr_table_get(r->headers_in, "Content-Type");
        /* encode the transaction sample next */
        sflow_sample_http(wk->sampler,
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
        uint32_t msgBytes = (wk->receiver->sampleCollector.datap - msg) << 2;
        /* write this in as the first 32-bit word */
        *msg = msgBytes;
        /* if greater than 4096 the pipe write will not be atomic. Should never happen, */
        /* but can't risk it, since we are relying on this as the synchronization mechanism */
        if(msgBytes > 4096) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "msgBytes=%u exceeds 4096-byte limit for atomic write", msgBytes);
            /* this counts as an sFlow drop-event */
            wk->sampler->dropEvents++;
        }
        else if(apr_file_write_full(sm->pipe_write, msg, msgBytes, &msgBytes) != APR_SUCCESS) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "error in apr_file_write_full()\n");
            /* this counts as an sFlow drop-event */
            wk->sampler->dropEvents++;
        }
        resetSampleCollector(wk->receiver);
    }

    
    if((now_uS - wk->lastTickTime) > SFWB_WORKER_TICK_US) {
        wk->lastTickTime = now_uS;
        ap_log_rerror(APLOG_MARK, APLOG_DEBUG, 0, r, "worker tick - sending counters\n");
        /* point to the start of the datagram */
        uint32_t *msg = wk->receiver->sampleCollector.datap;
        /* msglen, msgType, msgId */
        put32(wk->receiver, 0); /* we'll come back and fill this in later */
        put32(wk->receiver, SFLCOUNTERS_SAMPLE);
        put32(wk->receiver, SFLCOUNTERS_HTTP);
        putOpaque(wk->receiver, (char *)ctrs, sizeof(*ctrs));
        /* now reset my private counter block so that we only send the delta each time */
        memset(ctrs, 0, sizeof(*ctrs));
        /* get the msg bytes */
        uint32_t msgBytes = (wk->receiver->sampleCollector.datap - msg) << 2;
        /* write this in as the first 32-bit word */
        *msg = msgBytes;

        /* if greater than 4096 the pipe write will not be atomic. Should never happen, */
        /* but can't risk it, since we are relying on this as the synchronization mechanism */
        if(msgBytes > 4096) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "msgBytes=%u exceeds 4096-byte limit for atomic write", msgBytes);
            /* this counts as an sFlow drop-event */
            wk->sampler->dropEvents++;
        }
        else if(apr_file_write_full(sm->pipe_write, msg, msgBytes, &msgBytes) != APR_SUCCESS) {
            ap_log_rerror(APLOG_MARK, APLOG_ERR, 0, r, "error in apr_file_write_full()\n");
            /* this counts as an sFlow drop-event */
            wk->sampler->dropEvents++;
        }
        resetSampleCollector(wk->receiver);

        /* check in case the sampling-rate setting has changed. */
        sflow_set_random_skip(wk);
    }

    return OK;
}

static void sflow_register_hooks(apr_pool_t *p)
{
    ap_hook_pre_config(sflow_pre_config,NULL,NULL,APR_HOOK_REALLY_FIRST);
    ap_hook_post_config(sflow_post_config,NULL,NULL,APR_HOOK_MIDDLE);
    ap_hook_child_init(sflow_init_worker,NULL,NULL,APR_HOOK_MIDDLE);
    ap_hook_handler(sflow_handler_test, NULL, NULL, APR_HOOK_MIDDLE);
    ap_hook_handler(sflow_handler, NULL, NULL, APR_HOOK_LAST);
    ap_hook_log_transaction(sflow_multi_log_transaction,NULL,NULL,APR_HOOK_MIDDLE);
}

/*
static const char *cmd_sflow_enabled(cmd_parms *cmd, void *dummy, int arg)
{
    AP_DEBUG_ASSERT(cmd != NULL);
    SFWB *sm = ap_get_module_config(cmd->server->module_config, &sflow_module);
    AP_DEBUG_ASSERT(sm != NULL);
    sm->enabled = arg;
    return NULL;
}

static const command_rec sflow_cmds[] = {
    AP_INIT_FLAG("enable", cmd_sflow_enabled, NULL, RSRC_CONF, "sFlow sampling on/off"),
    {NULL}
};
*/

/* Dispatch list for API hooks */
module AP_MODULE_DECLARE_DATA sflow_module = {
    STANDARD20_MODULE_STUFF, 
    NULL,                  /* create per-dir config structures        */
    NULL,                  /* merge  per-dir config structures        */
    create_sflow_config,   /* create per-server config structures     */
    NULL,                  /* merge  virtual-server config structures */
    NULL,/*sflow_cmds*/    /* table of config file commands           */
    sflow_register_hooks,  /* register hooks                          */
};

