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
package storm.starter.tools;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import java.text.Format;
import java.text.SimpleDateFormat;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.*;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlotBasedCounterRedis<T> implements Serializable {

  private static final long serialVersionUID = 4858185737378394432L;

  //private ConcurrentHashMap<T, AtomicLongArray> objToCounts = new ConcurrentHashMap<T, AtomicLongArray>();
  private Map<T, AtomicLongArray> objToCounts = new ConcurrentHashMap<T, AtomicLongArray>();
  private final int numSlots;
  private Format tmptime;
  private JedisPoolConfig config;
  private JedisPool pool;
  private Pipeline pipe;;
  private Jedis client;
  private static final Logger LOG = Logger.getLogger(SlotBasedCounternew.class);


  public SlotBasedCounterRedis(int numSlots) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
    }
    this.numSlots = numSlots;
    tmptime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    config = new JedisPoolConfig();
    config.setMaxTotal(30000);
    config.setMaxIdle(10);
    config.setTestOnBorrow(false);
    config.setTestOnReturn(false);
    config.setMaxWaitMillis(30*1000);
    pool = new JedisPool(config,"192.168.111.220",6379);    
  }

  public void incrementCount(T obj, int slot) {
    AtomicLongArray _counts  = objToCounts.get(obj);
    if (_counts == null) {
      AtomicLongArray _newCounts = new AtomicLongArray(numSlots+10);
     _newCounts.set(slot,(long)1);
     objToCounts.put(obj, _newCounts);
      
    }
    else{
    	_counts.incrementAndGet(slot);
    }
    
  }

  public long getCount(T obj, int slot) {
    AtomicLongArray counts = objToCounts.get(obj);
    if (counts == null) {
      return 0;
    }
    else {
      return counts.get(slot);
    }
  }

    public void getCounts(String logtime,int slot,OutputCollector collector) {
    //LOG.info("##now we want to get data from slot: "+slot);
    String humanrealtime = tmptime.format(Long.parseLong(logtime)*1000).toString(); //for redisBolt
    String deloldkeytime = tmptime.format((Long.parseLong(logtime)-10800)*1000).toString(); //for redisBolt
    boolean borrowOrOprSuccess = true;
    try{
 	client = pool.getResource();
        pipe = client.pipelined();

        for (T obj : objToCounts.keySet()) {
	    pipe.hincrBy(obj.toString(),humanrealtime,computeTotalCount(obj,slot));
	    pipe.hdel(obj.toString(),deloldkeytime);
    	}
	}catch(JedisConnectionException e){
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

  private long computeTotalCount(T obj,int slot) {
    AtomicLongArray curr = objToCounts.get(obj);
    return curr.get(slot);
  }

  /**
   * Reset the slot count of any tracked objects to zero for the given slot.
   *
   * @param slot
   */
  public void wipeSlot(int slot) {
    for (T obj : objToCounts.keySet()) {
      resetSlotCountToZero(obj, slot);
    }
  }

  private void resetSlotCountToZero(T obj, int slot) {
    AtomicLongArray counts = objToCounts.get(obj);
    counts.set(slot,(long)0);
  }


  /**
   * Remove any object from the counter whose total count is zero (to free up memory).
   */

}
