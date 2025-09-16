# sample_01.py

import dask
import sys
import math
import pandas as pd
import subprocess
from subprocess import check_output
import re
import os
from datetime import datetime, timedelta
import json
import datetime
import time
from collections import defaultdict
from elasticsearch import Elasticsearch
from dateutil import tz
import pytz
from pytz import timezone

## Read config
from configobj import ConfigObj

scriptPath = (os.path.dirname((os.path.realpath(__file__))))
print(scriptPath)
# Parse Config File
# conffile=scriptPath+"./conf/newinfo.conf"
conffile=scriptPath+"/../conf/newinfo_storage.conf"
config = ConfigObj(conffile)




# (from the outline view)

#@dask.delayed
def getInfo(serverID,stime,etime,**kwargs):
    return serverList

def getDirectReports(userID):
    from elasticsearch import Elasticsearch
    scriptPath = (os.path.dirname((os.path.realpath(__file__))))

    ca_cert = "snpsICA2.pem"
    es = Elasticsearch(
        ['ems'],
        http_auth=('grafanausr', 'Gr1f1n!'),
        port=9200,
        use_ssl=True,
        ca_certs=ca_cert,
        timeout=500
    )

    query = '{"size":100,"_source":["user"],"query":{"query_string":{"query":"manager:'+userID+' AND status:Active "}}}'
    res = es.search(index="lookup",scroll="2m", body=query)
    results={}
    scrollID = res['_scroll_id']
    scroll_size = len(res['hits']['hits'])
    reports=[]
    while (scroll_size > 0):
        for hit in res['hits']['hits']:
            reports.append(hit['_source']['user'])
        res = es.scroll(scroll_id = scrollID, scroll="2m")
        scrollID = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])
    return reports


def getTrendDates(stime,etime,**kwargs):
    st_date=datetime.datetime.fromtimestamp(stime/1000)
    ed_date=datetime.datetime.fromtimestamp(etime/1000)
    dt=st_date
    delta=ed_date-st_date
    if 'frequency' in kwargs:
        frequency=kwargs['frequency'].lower()
    else:
        frequency="1d"
    if 'd' in frequency:
        freq=frequency.replace('d','')
        if freq.isdigit():
            tdelta=datetime.datetime.timedelta(days=int(freq))
        else:
            return "Frequency cannot be non acceptable format"
    td="days"
    ti=freq
    dates=[]

    while dt<ed_date:
        if dt>ed_date:
            break
        if dt==st_date:
            start=dt
            dt=dt.replace(hour=0,minute=0,second=0)+tdelta
            end=dt-datetime.timedelta(seconds=1)
        elif dt == ed_date:
            start=dt.replace(hour=0, minute=0, second=0)
            dt=dt.replace(hour=0,minute=0,second=0)+tdelta
            end=dt-datetime.timedelta(seconds=1)
        else:
            start=dt
            dt=dt+tdelta  # datetime.timedelta(td=ti)  #hour=23,minute=59,second=59)
            end=dt-datetime.timedelta(seconds=1)

        if end>ed_date:
            end=ed_date
        start_timestamp=int(datetime.datetime.timestamp(start)*1000)
        end_timestamp=int(datetime.datetime.timestamp(end)*1000)
        if 'dt_seq' in kwargs:
            dates.append([start_timestamp,end_timestamp])
        else:
            edate_d=datetime.datetime.fromtimestamp(end_timestamp/1000).strftime("%Y-%m-%d")
            dates.append(edate_d)

    return dates

def get_all_values(aggResp, aggList):
    vDict={}
    for agg in aggResp:
        if agg in aggList:
            for buc in aggResp[agg]['buckets']:
                vDict[buc['key']]=get_all_values(buc,aggList)
        elif agg in ['key']:
            dictKey=aggResp[agg]
        elif agg in ['doc_count','doc_count_error_upper_bound','sum_other_doc_count']:
            continue
        else:
            coreCount=ramCount=0
            if agg== "core_ram":
                for bagg in aggResp[agg]['buckets']:
                    tempCore = bagg['core_ram_buckets']['hits']['hits'][0]['_source']['core_count']
                    if tempCore is None:
                        tempCore = 0
                    coreCount = coreCount + tempCore
                    tempRam = bagg['core_ram_buckets']['hits']['hits'][0]['_source']['ram']
                    if tempRam is None:
                        tempRam = 0
                    ramCount = ramCount + tempRam
                vDict.update({"core_count":coreCount,"ram":ramCount})
            else:
                vDict.update({agg:aggResp[agg]['value']})
    return vDict

def get_all_values_storage(aggResp, aggList):
    vDict={}
    for agg in aggResp:
        if agg in aggList:
            for buc in aggResp[agg]['buckets']:
                vDict[buc['key']]=get_all_values_storage(buc,aggList)
        elif agg in ['key']:
            dictKey=aggResp[agg]
        elif agg in ['doc_count', 'doc_count_error_upper_bound', 'sum_other_doc_count']:
            continue
        else:
            usedSize=totalSize=0
            if agg=="total_used":
                for bagg in aggResp[agg]['buckets']:
                    if bagg['total_used_buckets']['hits']['hits'][0]['_source']['totalsize'] is None:
                        bagg['total_used_buckets']['hits']['hits'][0]['_source']['totalsize'] = 0
                    totalSize=totalSize+(bagg['total_used_buckets']['hits']['hits'][0]['_source']['totalsize'])
                    if bagg['total_used_buckets']['hits']['hits'][0]['_source']['usedsize'] is None:
                        bagg['total_used_buckets']['hits']['hits'][0]['_source']['usedsize'] = 0
                    usedSize=usedSize+(bagg['total_used_buckets']['hits']['hits'][0]['_source']['usedsize'])
                vDict.update({"total_size":totalSize,"used_size":usedSize,"disk_efficiency":(usedSize/totalSize)*100 if totalSize != 0 else 0})
            else:
                vDict.update({agg:aggResp[agg]['value']})
    return vDict


@dask.delayed
def computeAggFetch(computeQuery, times, **kwargs):

    start_time_e=times[0]
    end_time_e=times[1]
    startTsString="@timestamp:>={}".format(start_time_e)
    endTsString="@timestamp:<={}".format(end_time_e)
    aggQuery={"size":0, "query":computeQuery}

    statsAggs={
        "count":{"cardinality":{"field":"hostname","precision_threshold":300000}},
        "run_rate":{"sum":{"field":"total_runrate"}},
        "core_hours":{"sum":{"field":"core_hours"}},
        "ram_hours":{"sum":{"field":"ram_hours"}},
        "efficiency":{"avg":{"field":"cpu_30d","script":{"source":"_value * 100"}}},
        "core_ram":{
            "terms":{"field":"hostname","size":300000},
            "aggs":{
                "core_ram_buckets":{
                    "top_hits":{
                        "sort":[{"@timestamp":{"order":"desc"}}],
                        "_source":{"includes":["hostname","core_count","ram"]},
                        "size":1
                    }}}}
    }

    edate=datetime.datetime.fromtimestamp(end_time_e/1000)
    edate=edate.replace(tzinfo=timezone('PST8PDT'))
    edate_d=edate.strftime("%Y-%m-%d")
    if 'aggs' in kwargs and kwargs['aggs']:
        prevAgg=None
        aggDictStr=""
        for agg in kwargs['aggs']:
            aggStr='"aggs":{"'+agg+'":{"terms":{"field":"'+agg+'","size":1000},<AGGS>}}'
            if prevAgg is not None:
                aggDictStr=aggDictStr.replace('<AGGS>',aggStr)
            else:
                aggDictStr=aggStr
                prevAgg=agg
        aggDictStr=aggDictStr.replace('<AGGS>','"aggs":'+json.dumps(statsAggs)+'')

        computeAggQuery='{"size":0, "query":'+json.dumps(computeQuery)+','+aggDictStr+'}'
        computeAggQuery= re.sub('@timestamp:>=(\\d+)', startTsString, computeAggQuery)
        computeAggQuery= re.sub('@timestamp:<=(\\d+)', endTsString, computeAggQuery)
        #print(computeAggQuery)
        aggResp=runelasticQuery("compute", computeAggQuery, "compute")
        computeReturn=get_all_values(aggResp, kwargs['aggs'])
    else:
        computeAggQuery=aggQuery
        computeAggQuery.update({"aggs":statsAggs})
        computeAggQuery=json.dumps(computeAggQuery)
        computeAggQuery= re.sub('@timestamp:>=(\\d+)', startTsString, computeAggQuery)
        computeAggQuery= re.sub('@timestamp:<=(\\d+)', endTsString, computeAggQuery)

        aggResp=runelasticQuery("compute", computeAggQuery, "compute")
        computeReturn=get_all_values(aggResp,[])

    return {edate_d:computeReturn}


@dask.delayed
def storageAggFetch(storageQuery, times, **kwargs):

    start_time_e=times[0]
    end_time_e=times[1]
    startTsString='@timestamp:>={}'.format(start_time_e)
    endTsString='@timestamp:<={}'.format(end_time_e)
    aggQuery={'size':0, 'query':storageQuery}
    statsAggs={
        "disk_count":{"cardinality":{"field":"mount_point","precision_threshold":300000}},
        "run_rate":{"sum":{"field":"runrate"}},
        "disk_hours":{"sum":{"field":"disk_hours"}},
        "efficiency":{"avg":{"field":"used_pct"}},
        "total_used":{
            "terms":{"field":"mount_point","size":300000},
            "aggs":{
                "total_used_buckets":{
                    "top_hits":{
                        "sort":[{"@timestamp":{"order":"desc"}}],
                        "_source":{"includes":["hostname","totalsize","usedsize"]},
                        "size":1
                    }}}}
    }

    edate=datetime.datetime.fromtimestamp(end_time_e/1000)
    edate=edate.replace(tzinfo=timezone('PST8PDT'))
    edate_d=edate.strftime("%Y-%m-%d")
    if 'aggs' in kwargs and kwargs['aggs']:
        prevAgg=None
        aggDictStr=""
        for agg in kwargs['aggs']:
            aggStr='"aggs":{"'+agg+'":{"terms":{"field":"'+agg+'","size":1000},<AGGS>}}'
            if prevAgg is not None:
                aggDictStr=aggDictStr.replace('<AGGS>',aggStr)
            else:
                aggDictStr=aggStr
                prevAgg=agg
        aggDictStr=aggDictStr.replace('<AGGS>','"aggs": '+json.dumps(statsAggs)+'')

        storageAggQuery='{"size":0, "query":'+json.dumps(storageQuery)+','+aggDictStr+'}'
        storageAggQuery= re.sub('@timestamp:>=(\\d+)', startTsString, storageAggQuery)
        storageAggQuery= re.sub('@timestamp:<=(\\d+)', endTsString, storageAggQuery)
        #print(storageAggQuery)
        aggResp=runelasticQuery("storage", storageAggQuery, "storage")
        storageReturn=get_all_values_storage(aggResp, kwargs['aggs'])
    else:
        storageAggQuery=aggQuery
        storageAggQuery.update({"aggs":statsAggs})
        storageAggQuery=json.dumps(storageAggQuery)
        storageAggQuery= re.sub('@timestamp:>=(\\d+)', startTsString, storageAggQuery)
        storageAggQuery= re.sub('@timestamp:<=(\\d+)', endTsString, storageAggQuery)

        aggResp=runelasticQuery("storage", storageAggQuery, "storage")
        storageReturn=get_all_values_storage(aggResp,[])

    return {edate_d:storageReturn}



def buildQuery(debugQuery, **kwargs):
    elkQuery='''{"size":10000,<SOURCE>"query":{"query_string":{"query":"'''+debugQuery+''' AND <STATUS> AND <QUERY_STRING> AND @timestamp:>='''+str(kwargs['start_time'])+''' AND @timestamp:<='''+str(kwargs['end_time'])+'''"
    }}}
'''
    return elkQuery


def transformQuery(serv, stats, fieldsMap, fields, elkQuery, **kwargs):
    queryString = ""
    sourceFields=['@timestamp']
    for field in fieldsMap:
        sourceFields.append(fieldsMap[field][serv])
    sourceFields.extend(stats)
    sourceString='"_source":{}'.format(json.dumps(sourceFields))
    elkQuery=elkQuery.replace('<SOURCE>',sourceString)
    for param in kwargs:
        if param in fields:
            if queryString == "":
                if param == "owner_mgr_chain" and 'exec_reports' in kwargs:
                    queryString=' ({}:({}) OR {}:({})) '.format(param, ' OR '.join(kwargs[param]), fieldsMap['pri_owner'][serv], ' OR '.join(kwargs[param]))
                else:
                    queryString=' {}:(\\"{}\\") '.format(param, '\\" OR \\"'.join(kwargs[param]))
            else:
                if param == "owner_mgr_chain" and 'exec_reports' in kwargs:
                    queryString='{} AND ({}:({}) OR {}:({})) '.format(queryString, param, ' OR '.join(kwargs[param]), fieldsMap['pri_owner'][serv], ' OR '.join(kwargs[param]))
                else:
                    queryString='{} AND {}:(\\"{}\\") '.format(queryString, param, '\\" OR \\"'.join(kwargs[param]))
    # print(queryString)
    elkQuery=elkQuery.replace('<QUERY_STRING>',queryString)

    return elkQuery


def gather_inputs(**kwargs):
    scriptPath = (os.path.dirname((os.path.realpath(__file__))))
    ##Parse Config File
    confFile=scriptPath+"/../conf/newinfo.conf"
    config = ConfigObj(confFile)
    
    global computeService
    computeService=['odcbatch','odcres','odcdynamic','traditional']
    global fieldsMap
    fieldsMap={
            "pri_owner":           {"storage":"owner","compute":"pri_owner"},
            "sec_owner":           {"storage":"owner","compute":"sec_owner"},
            "bg_code":             {"storage":"bg_code","compute":"bg_code"},
            "owner_vp":            {"storage":"owner_vp","compute":"owner_vp"},
            
            "owner_mgr":           {"storage":"owner_manager","compute":"owner_mgr"},
            "owner_mgr_chain":     {"storage":"owner_mgr_chain","compute":"owner_mgr_chain"},
            "mount_point":         {"storage":"mount_point","compute":"hostname"},
            "hostname":            {"storage":"mount_point","compute":"hostname"},
            
            "clust_vol_mount":     {"storage":"clust_vol_mount","compute":"clust_vol_mount"},
            "bu_code":             {"storage":"bu_code","compute":"bu_code"},
            "site":                {"storage":"site","compute":"site"},
            "farm":                {"storage":"farm","compute":"farm"},
            "service":             {"storage":"service","compute":"service"},
                    "cpu_class":           {"storage":"class","compute":"cpu_class"},
                    "cluster":             {"storage":"cluster","compute":"cage_location"},
                    "cpu":                 {"storage":"cpu_model","compute":"cpu"},
                    
                    "server_category":     {"storage":"category","compute":"server_category"},
                    "os_version":          {"storage":"os_name","compute":"os_version"},
                    "qsc":                 {"storage":"qsc","compute":"qsc"},
                    "status_name":         {"storage":"state","compute":"status_name"},
                    
                    "cpu_speed":           {"storage":"cpu_speed","compute":"cpu_speed"},
                    "bu_project":          {"storage":"bu_project","compute":"bu_project"},
                    "bu_product":          {"storage":"bu_product","compute":"bu_product"},
                    "backup":              {"storage":"backups","compute":"backups"}
                }
    queryParams={}
    requiredFields = ['start_time','end_time','required_info']
    if not all(field in kwargs for field in requiredFields):
        print("Not all required fields are presend,required fields are [{}]".format(','.join(requiredFields)))
        return
    
    start_time_e=kwargs['start_time']
    end_time_e=kwargs['end_time']
    sdate=datetime.datetime.fromtimestamp(start_time_e/1000)
    edate=datetime.datetime.fromtimestamp(end_time_e/1000)
    required_info=kwargs['required_info']

    if 'exec_name' in kwargs:
            execResults={}
            execList=kwargs['exec_name']
            for exec_name in execList:
                newargs=kwargs
                if 'exec_name' in newargs:
                    del newargs['exec_name']
                reportees=(getDirectReports(exec_name))
                if len(reportees) == 0:
                    if 'user_name' in newargs:
                        newargs.append(exec_name)
                    else:
                        newargs['pri_owner']=[exec_name]
                    execResults[exec_name]=gather_inputs(**newargs)
                else:
                    for reportee in reportees:
                        newargs['owner_mgr_chain']=[reportee]
                        execResults[reportee]=gather_inputs(**newargs)
            return execResults



    statusMap={"Production":{"storage":"online"},"Retired":{"storage":"offline"}}
        if 'status' not in kwargs:
            status=['Production','Retired']
            kwargs['status']=status
        else:
            status=kwargs['status']
        
        if 'service' not in kwargs:
            service=['odcbatch','odcres','odcdynamic','traditional','storage']
            kwargs['service']=service
        else:
            service=kwargs['service']
        
        aggsGiven = kwargs.get('aggs',[])



    #compute
        # elkQuery = buildQuery(" -pri_owner:na ", **kwargs)
        elkQuery = buildQuery(" -pri_owner:na AND NOT odc_hv:1 AND NOT hostname:*-hv* ", **kwargs)
        compute_stats = ["core_count","ram","total_runrate","total_mins","cpu_30d","core_hours","ram_hours"]
        compute_fields = ["pri_owner","bg_code","owner_vp","owner_manager",
                        "owner_mgr_chain","hostname","owner_bu","site","farm","service","cpu_class"]
        elkQuery = transformQuery('compute', compute_stats, fieldsMap, compute_fields, elkQuery, **kwargs)
        
        #storage
        StorelkQuery = buildQuery(" -owner:na ", **kwargs)
        storage_stats = ["totalsize","usedsize","disk_hours","runrate","total_mins","used_pct"]
        storage_fields = ["owner","bg_code","owner_vp","owner_manager","owner_mgr_chain",
                        "mount_point","owner_bu","site","farm","service","class","storage_type","clust_vol_mount"]
        StorelkQuery = transformQuery('storage', storage_stats, fieldsMap, storage_fields, StorelkQuery, **kwargs)
        # print(StorelkQuery)
        #scroll output



    response_type="details"
        if required_info == "instance_count_info":
            print("fetching details")
            response_type=required_info
        resp=[]
        print(status)
        for stat in status:
            if stat == 'Production':
                resp1=[]
                resp2=[]
                if any(item in computeService for item in kwargs['service']):
                    elkQuery=elkQuery.replace('<STATUS>',"status_name:Production")
                    resp1=runElasticQuery("compute",elkQuery,response_type)
                if "storage" in kwargs['service']:
                    StorelkQuery=StorelkQuery.replace('<STATUS>',"NOT state:offline")
                    resp2=runElasticQuery("storage",StorelkQuery,response_type)
                resp=resp+resp1+resp2




    elif stat == 'Retired':
                resp1=[]
                resp2=[]
                if any(item in computeService for item in kwargs['service']):
                    elkQuery1=elkQuery.replace('status_name:Production','status_name:Retired')
                    resp1=runElasticQuery("compute",elkQuery1,response_type)
                if "storage" in kwargs['service']:
                    StorelkQuery1=StorelkQuery.replace('NOT state:offline','state:offline')
                    resp2=runElasticQuery("storage",StorelkQuery1,response_type)
                resp=resp+resp1+resp2
        df = pd.DataFrame(resp)
        
        # print(df)
        df.to_csv('Details.csv', sep=",", index=False, float_format='%g', escapechar=',')
        return resp


    #dask calculations
        else:
            global serverList
            serverList=[]
            global serverInventory
            serverInventory={}
            global storageInventory
            storageInventory={}
            from dask.distributed import Client
            import time
            cName="{}_{}".format(required_info,time.time())
            client = Client('10.15.24.72:8786',name=cName,timeout=20)
            dask.config.set(scheduler='threads')



    for stat in status:
                if stat == 'Production':
                    if any(item in computeService for item in kwargs['service']):
                        elkQuery=elkQuery.replace('<STATUS>',"status_name:Production")
                    if 'storage' in kwargs['service']:
                        StorelkQuery=StorelkQuery.replace('<STATUS>',"NOT state:offline")
                # print(elkQuery)
                computeQuery=json.loads(elkQuery)
                # print(StorelkQuery)
                storageQuery=json.loads(StorelkQuery)


    if required_info in ["disk_count","count","instance_count",
                                    "core_count","ram","core_hours","ram_hours","capacity",
                                    "stats","efficiency","run_rate","disk_hours","total_size","used_size"]:
                    if 'trend' in kwargs:
                        if 'frequency' in kwargs:
                            frequency=kwargs['frequency']
                        else:
                            frequency="1d"
                            
                        trendTimes=getTrendDates(start_time_e,end_time_e,frequency=frequency,dt_seq=1)
                        data=[]
                        Computeresult=[[]]
                        Storageresult=[[]]
                        if any(item in computeService for item in kwargs['service']):
                            aggsMapped = [fieldsMap[item]['compute'] for item in aggsGiven]
                            kwargs['aggs'] = aggsMapped


    if 'aggs' in kwargs:
                data = [computeAggFetch(computeQuery['query'], i, aggs=kwargs['aggs']) for i in trendTimes]
            else:
                data = [computeAggFetch(computeQuery['query'], i) for i in trendTimes]
            Computeresult = dask.compute(data)
        if "storage" in kwargs['service']:
            aggsMapped = [fieldsMap[item]['storage'] for item in aggsGiven]
            kwargs['aggs'] = aggsMapped
            if 'aggs' in kwargs:
                data = [storageAggFetch(storageQuery['query'], i, aggs=kwargs['aggs']) for i in trendTimes]
            else:
                data = [storageAggFetch(storageQuery['query'], i) for i in trendTimes]
            Storageresult = dask.compute(data)


    if len(Storageresult[0]) > 0 or len(Computeresult[0]) > 0:
            ComputeResult={}
            if len(Computeresult[0]) > 0:
                from collections import ChainMap
                ComputeResult=dict(ChainMap(*Computeresult[0]))
            StorageResult={}
            if len(Storageresult[0]) > 0:
                from collections import ChainMap
                StorageResult=dict(ChainMap(*Storageresult[0]))
            aggs={"storage":StorageResult,"compute":ComputeResult}
            return aggs
        else:
            result={}
            for date in getTrendDates(start_time_e,end_time_e,frequency=frequency):
                result[date]=0
            return result

    else:
            data=[]
            computeReturn={}
            storageReturn={}
            if any(item in computeService for item in kwargs['service']):
                aggsMapped = [fieldsMap[item]['compute'] for item in aggsGiven]
                kwargs['aggs'] = aggsMapped
                Ttimes=[[start_time_e,end_time_e]]
                aggQuery={"size":0,"query":computeQuery['query']}


    statsAggs={
    "count":        {"cardinality":{"field":"hostname","precision_threshold":300000}},
    "run_rate":     {"sum":{"field":"total_runrate"}},
    "core_hours":   {"sum":{"field":"core_hours"}},
    "ram_hours":    {"sum":{"field":"ram_hours"}},
    "efficiency":   {"avg":{"field":"cpu_30d","script":{"source":"_value * 100"}}},
    "core_ram":     {
                    "terms":{"field":"hostname","size":300000},
                    "aggs":{
                        "core_ram_buckets":{
                            "top_hits":{
                                "sort":[{"@timestamp":{"order":"desc"}}],
                                "_source":{"includes":["hostname","core_count","ram"]},
                                "size":1
                            }
                        }
                    }
                }
            }




    # print(kwargs["aggs"])
            if 'aggs' in kwargs and kwargs['aggs']:
                prevAgg=None
                aggDictStr=""
                for agg in kwargs['aggs']:
                    aggStr='"aggs":{"'+agg+'":{"terms":{"field":"'+agg+'","size":1000},<AGGS>}}'
                    if prevAgg is not None:
                        aggDictStr=aggDictStr.replace('<AGGS>',aggStr)
                    else:
                        aggDictStr=aggStr
                        prevAgg=agg
                aggDictStr=aggDictStr.replace('<AGGS>','"aggs":'+json.dumps(statsAggs)+'')
                computeAggQuery='{"size":0,"query":'+json.dumps(computeQuery['query'])+','+aggDictStr+'}'
                aggResp=runElasticQuery("compute",computeAggQuery,response_type)
                computeReturn=get_all_values(aggResp,kwargs['aggs'])


    else:
                computeAggQuery=aggQuery
                computeAggQuery.update({"aggs":statsAggs})
                computeAggQuery=json.dumps(computeAggQuery)
                # print(computeAggQuery)
                aggResp=runElasticQuery("compute",computeAggQuery,response_type)
                computeReturn=get_all_values(aggResp,[])


    if "storage" in kwargs['service'] and required_info in ["disk_count","total_size","used_size","disk_hours","capacity","stats"]:
                aggsMapped = [fieldsMap[item]['storage'] for item in aggsGiven]
                kwargs['aggs'] = aggsMapped
                Ttimes=[[start_time_e,end_time_e]]
                aggQuery={"size":0,"query":storageQuery['query']}



    statsAggs={
    "disk_count":{
    "cardinality":{
    "field":"clust_vol_mount",
    "precision_threshold":300000}
    },
    "run_rate":{
    "sum":{"field":"runrate"}
    },
    "disk_hours":{
    "sum":{"field":"disk_hours"}
    },
    "efficiency":{
    "avg":{"field":"used_pct"}
    },
    "total_used":{
    "terms":{"field":"clust_vol_mount","size":300000},
    "aggs":{
    "total_used_buckets":{
    "top_hits":{
    "sort":[{"@timestamp":{"order":"desc"}}],
    "_source":{"includes":["clust_vol_mount","totalsize","usedsize"]},
    "size":1}}}}}    


    if 'aggs' in kwargs and kwargs['aggs']:
                    prevAgg=None
                    aggDictStr=""
                    for agg in kwargs['aggs']:
                        aggStr='"aggs":{"'+agg+'":{"terms":{"field":"'+agg+'","size":1000},<AGGS>}}'
                        if prevAgg is not None:
                            aggDictStr=aggDictStr.replace('<AGGS>',aggStr)
                        else:
                            aggDictStr=aggStr
                            prevAgg=agg
                    aggDictStr=aggDictStr.replace('<AGGS>','"aggs":'+json.dumps(statsAggs)+'')
                    storageAggQuery='{"size":0,"query":'+json.dumps(storageQuery['query'])+','+aggDictStr+'}'
                    # print(storageAggQuery)
                    aggResp=runElasticQuery("storage",storageAggQuery,response_type)
                    # print(aggResp)
                    storageReturn=get_all_values_storage(aggResp,kwargs['aggs'])


    else:
                    storageAggQuery=aggQuery
                    storageAggQuery.update({"aggs":statsAggs})
                    storageAggQuery=json.dumps(storageAggQuery)
                    aggResp=runElasticQuery("storage",storageAggQuery,response_type)
                    storageReturn=get_all_values_storage(aggResp,[])
            if len(storageReturn) > 0 or len(computeReturn) >0:
                import itertools
                StorageColList=[]
                StorageColList=[]
                df=pd.DataFrame()
                aggs={}
                aggs={"storage":storageReturn,"compute":computeReturn}
                return aggs
            else:
                if 'aggs' in kwargs:
                    return {}
                return 0


# def runelasticQuery(serv, query, response_type,**kwargs): ...







