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



public class SumandText extends BaseRichBolt {

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
  private Map map;
  private int queryTimeoutSecs = 60;

  public SumandText(int num_stor,int freq){
    if (num_stor < 3) {
      throw new IllegalArgumentException(
          "num_stor in must be at least three (you requested " + num_stor + ")");
    }
    this.num_stor = num_stor;
    this.freq = freq;	
    timeseq = new HashMap();
    Date date=new Date();
    long tmp,diff;
    tmp=date.getTime()/1000;
    diff=tmp%freq;
    for(int i=0;i<num_stor+10;i++){
	timeseq.put((tmp-diff+10*freq-i*freq),i);
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    tmptime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try{
        fw = new FileWriter("/tmp/sumtext");
    }catch(Exception e){
                e.printStackTrace();
    }
    }

  @Override
  public void execute(Tuple tuple) {
    this.collector.ack(tuple);
    if (TupleHelpers.isTickTuple(tuple)){
        Object[] key_arr = timeseq.keySet().toArray();
	Arrays.sort(key_arr);
        long time_key = Long.parseLong(key_arr[num_stor-2].toString());
        int slot = slotSelect(time_key);
 	long maxkey = Long.parseLong(key_arr[num_stor+10-1].toString());
        long new_key = maxkey + (long)freq;
	long minkey = Long.parseLong(key_arr[0].toString());
	int new_num = Integer.parseInt(timeseq.get(minkey).toString());
	timeseq.put(new_key,new_num);
	timeseq.remove(minkey);
	for (Object obj : objToCounts.keySet()){
		AtomicLongArray counts = objToCounts.get(obj);
		counts.set(new_num,(long)0);	
	}
	//LOG.info("sumblot maxkey is: "+maxkey+" new key is: "+new_key+" new num is: "+new_num+" key_array: "+(key_arr[num_stor-2]).toString());
	String humanrealtime = tmptime.format(Long.parseLong((key_arr[num_stor-2]).toString())*1000).toString();
	String deloldkeytime = tmptime.format((Long.parseLong((key_arr[num_stor-2]).toString())-10800)*1000).toString();
	//LOG.info("log to redis realtime is: "+humanrealtime);
	try{
			for(int i =0;i<num_stor;i++){
				long testtime_key = Long.parseLong(key_arr[i].toString());
				String testnicetime = tmptime.format(testtime_key*1000).toString();
	        		int testslot = slotSelect(testtime_key);
				Object testobj = "200";
				fw.write(testnicetime+" "+"200"+" "+getCount(testobj,testslot)+"\n");
			}
			fw.write("------------------------"+"\n");
			fw.flush();
		}catch(Exception e){
			LOG.info("redis err: "+e.toString());
		}
	  	
	}
    else{
	long logtime = Long.parseLong(tuple.getString(2));
	long num = Long.parseLong(tuple.getString(1));
        Object obj = tuple.getValue(0);	
        int slot = slotSelect(logtime);
	incrementCount(obj,slot,num);
    } 

  }

 public int slotSelect(long logtime){
   if(timeseq.containsKey(logtime)){
	return Integer.parseInt(timeseq.get(logtime).toString()); 
   }
   else{
	return -1;
   }

 }

 public String getCount(Object obj, int slot) {
    AtomicLongArray counts = objToCounts.get(obj);
    if (counts == null || slot < 0) {
      return "0";
    }
    else {
      return Long.toString(counts.get(slot));
    }
  }

  public void incrementCount(Object obj, int slot,long num) {
    if(slot < 0){
	return;
    }
    AtomicLongArray _counts  = objToCounts.get(obj);
    if (_counts == null) {
      AtomicLongArray _newCounts = new AtomicLongArray(num_stor+10);
     //LOG.info("##indexout slot is: "+slot+" counts length is: "+_newCounts.length());
     _newCounts.set(slot,(long)num);
     objToCounts.put(obj, _newCounts);

    }
    else{
    //LOG.info("slot is: "+slot);	
        _counts.getAndAdd(slot,num);
    }

  }


 
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, freq);
    return conf;
  }
}
