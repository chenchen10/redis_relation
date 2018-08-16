#!/usr/bin/env python
# -*- coding: utf-8 -*-

import redis
import sys
import time
import MySQLdb
import re



global line
s_line=''

def redis_slave_info(instance, redis_auth, slave_list):
    global s_line
    redis_slave_list = slave_list
    redisAuth = redis_auth
    host, port = instance.split(":") 
    redisConn = redis.Redis(host=host, port=port, password=redisAuth)
    rep_info = redisConn.info('Replication')
    keys = rep_info.keys()
    #if rep_info.has_key('master_host'):
    #    print 'aaaaaaaaa',rep_info['master_host']
    slave_key = []
    for key in keys:
       w = re.findall('slave\d',key)
       if len(w) != 0:
          slave_key.append(key)
          #print key,info[key]
    #print slave_key
    if len(slave_key) != 0:
        #print 'bbbbbbb',slave_key
        for s in slave_key:
            #print s
            slave_ip = rep_info[s]['ip']
            slave_port = rep_info[s]['port']
            slave_instance = '%s:%s' % (slave_ip, slave_port)
            s_line += '\t'
            if sys.argv[1] != 'check':
                print '%s|-%s' % (s_line, slave_instance)
            redis_slave_list.append(slave_instance)
            redis_slave_info(slave_instance, redisAuth, redis_slave_list)
    else:
        s_line = ''
        print ''

def master_relate(redismaster):
    sql="select master,redis_auth,namespace from biz a,slice b where a.id=b.id and b.master='%s'" % (redismaster)
    row = mysql_fetch(sql)
    try:
        names = row[0][2]
    except IndexError:
        print '[Fail] %s not in proxy cluster masterip' % redismaster
        return 0
    print '='*50 + ' NameSpace: %s ' % names + '='*50
    for i in row:
        redis_slave_list = []
        masterip = i[0]
        redis_auth = i[1]
        print masterip 
        redis_slave_info(masterip, redis_auth, redis_slave_list)

def namespace_relate(namespace):
    sql="select master,redis_auth from biz a,slice b where a.id=b.id and a.namespace='%s' order by slice_id" % (namespace)
    #print redismaster
    #redis_slave_info(redismaster)
    row = mysql_fetch(sql)
    if len(row) == 0:
        print '[Fail] %s not in proxy cluster namespace' % namespace
        return 0
    print '='*50 + ' NameSpace: %s ' % namespace + '='*50
    for i in row:
        redis_slave_list = []
        masterip = i[0]
        redis_auth = i[1]
        print masterip 
        redis_slave_info(masterip, redis_auth, redis_slave_list)

def check_redis_slave(namespace):
    sql="select master,redis_auth,slave,slice_id from biz a,slice b where a.id=b.id and a.namespace='%s' order by id" % (namespace)
    #print redismaster
    #redis_slave_info(redismaster)
    row = mysql_fetch(sql)
    if len(row) == 0:
        print '[Fail] %s not in proxy cluster namespace' % namespace
        return 0
    print '='*50 + ' NameSpace: %s ' % namespace + '='*50
    for i in row:
        redis_slave_list = []
        masterip = i[0]
        redis_auth = i[1]
        slaveip = i[2]
        slice_id = i[3]
        slaveip_list = slaveip.split(',')
        #print slaveip_list
        redis_slave_info(masterip, redis_auth, redis_slave_list)
        for sip in slaveip_list:
            if sip not in redis_slave_list:
                print '[Fail] IP %s not in Slice %s' % (sip, slice_id)
            else:
                print '[OK] Slice %s Check slave %s sucess' % (slice_id,sip)
        #print redis_slave_list 

def mysql_fetch(SQL):
        conn=MySQLdb.connect(host="192.168.1.100",port=3306,user="chen_r",passwd="123456",db="x_service",charset="utf8")
        cur = conn.cursor()
        try:
            cur.execute(SQL)
            results = cur.fetchall()
            return results
        except Exception,e:
           print "Error: unable to fecth data",es

def Usage():
    print '='*70
    print 'Usage:    python redis_relation.py master/namespace/check MasterIp/RedisNamespace'
    print 'Example1: python redis_relation.py\tmaster\t\t192.168.200.1:5000'
    print 'Example2: python redis_relation.py\tnamespace\tchen_cn'
    print 'Example3: python redis_relation.py\tcheck\t\tchen_cn'
    print '='*70
    sys.exit(0)

#master_relate()
#namespace_relate()

try:
   action = sys.argv[1]
   value = sys.argv[2]
except IndexError:
   Usage()

if sys.argv[1] == 'master':
    if sys.argv[2] == '':
        Usage()
    else:
        master_relate(sys.argv[2])
elif sys.argv[1] == 'namespace':
    if sys.argv[2] == '':
        Usage()
    else:
        namespace_relate(sys.argv[2])
elif sys.argv[1] == 'check':
    if sys.argv[2] == '':
        Usage()
    else:
        check_redis_slave(sys.argv[2])
else:
    Usage()
