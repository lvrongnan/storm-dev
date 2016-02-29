/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.tools.NthLastModifiedTimeTracker;
import storm.starter.tools.SlidingWindowCounternew;
import storm.starter.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import java.lang.Long;
import java.lang.Integer;
import java.util.Date;
import java.util.Arrays;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.text.Format;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.*;


public class RedisBolt extends BaseRichBolt {

  private Map<Object, AtomicLongArray> objToCounts = new ConcurrentHashMap<Object, AtomicLongArray>();
  private static final Logger LOG = Logger.getLogger(SumBolt.class);
  private HashMap timeseq;
  private OutputCollector collector;
  private String logtime = null;
  private int num_stor;
  private int freq;
  private FileWriter fw;
  private Format tmptime;
  private String humanrealtime;
  private Object obj;
  private JedisPoolConfig config;
  private JedisPool pool;
  private Pipeline pipe;;
  private Jedis client;
  private Map map;
  private int queryTimeoutSecs = 60;

  public RedisBolt(){
    }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    tmptime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    config = new JedisPoolConfig();
    config.setMaxTotal(30000);
    config.setMaxIdle(10);
    config.setTestOnBorrow(false);
    config.setTestOnReturn(false);
    config.setMaxWaitMillis(30*1000);
    pool = new JedisPool(config,"192.168.111.220",6379);
   // try{
   //     fw = new FileWriter("/tmp/sumtext");
   // }catch(Exception e){
   //             e.printStackTrace();
   // }
    }

  @Override
  public void execute(Tuple tuple) {
    collector.ack(tuple);
    Object obj = tuple.getValue(0);
    String humanrealtime = tuple.getString(1);
    String deloldkeytime = tuple.getString(2);
    long num  = Long.parseLong(tuple.getString(3));
    boolean borrowOrOprSuccess = true;
	try{
		client = pool.getResource();
		pipe = client.pipelined();
		pipe.hincrBy(obj.toString(),humanrealtime,num);
		pipe.hdel(obj.toString(),deloldkeytime);
	}catch(JedisConnectionException e){
		//e.printStackTrace();
		//LOG.info("redis err: "+e.toString());
		borrowOrOprSuccess = false;
		if(client != null){
			pool.returnBrokenResource(client);
			}
		
	}finally{
		if (borrowOrOprSuccess){
			pipe.sync();
			pool.returnResource(client);
		}
	}
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
  @Override
  public void cleanup() {
    pool.destroy();
    super.cleanup();
  }  
}
