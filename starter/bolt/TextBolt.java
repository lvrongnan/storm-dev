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
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class TextBolt extends BaseRichBolt {

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
  private Jedis client;
  private Map map;
  private int queryTimeoutSecs = 60;

  public TextBolt(){
    }
    //map.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
    //map.put("dataSource.url","jdbc:mysql://192.168.111.220/ad_ha");
    //map.put("dataSource.user","root");
    //map.put("dataSource.password","funshionsys");
    //connectionProvider = new HikariCPConnectionProvider(map);
    //connectionProvider.prepare();
    //jdbcClient = new JdbcClient(connectionProvider,queryTimeoutSecs);

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    tmptime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //config = new JedisPoolConfig();
    //config.setMaxIdle(10);
    //config.setMaxTotal(30);
    //config.setMaxWaitMillis(3*1000);
    //pool = new JedisPool(config,"192.168.111.220",6379);
    try{
        fw = new FileWriter("/tmp/sumtext");
    }catch(Exception e){
                e.printStackTrace();
    }
    }

  @Override
  public void execute(Tuple tuple) {
    collector.ack(tuple);
    String obj = tuple.getValue(0).toString();
    String humanrealtime = tuple.getString(1);
    String deloldkeytime = tuple.getString(2);
    String num  = tuple.getString(3);
        //	try{
	//		jedisCommands = container.getInstance();//in for loop,gen too many instance
	//		jedisCommands.hset(obj.toString(),humanrealtime,getCount(obj,slot));
	//	}catch(Exception e){
        //               LOG.info(e.printStackTrace());
			//LOG.info("redis error: "+e);
        //	}finally{
	//		if (jedisCommands != null) {
        //            		container.returnInstance(jedisCommands);
        //       		 }		
	//	}
	//
	 try{
                                if(obj.equals("200")){
                                	fw.write(humanrealtime+" "+"200"+" "+num+"\n");
		                        fw.write("------------------------"+"\n");
                		        fw.flush();
				}
                }catch(Exception e){
                        LOG.info("redis err: "+e.toString());
                }	
	
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
