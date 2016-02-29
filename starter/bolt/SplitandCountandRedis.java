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
import storm.starter.tools.SlidingWindowCounterRedis;
import storm.starter.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.lang.Long;
import java.util.Collections;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import java.text.SimpleDateFormat;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 */
public class SplitandCountandRedis extends BaseRichBolt {

  private static final Logger LOG = Logger.getLogger(SplitandCount.class);
  private OutputCollector collector;
  private static final int DEFAULT_NUM_STOR = 20;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 5*60;
  private  SlidingWindowCounterRedis<Object> counter;
  private final int numStor;
  private final int emitFrequencyInSeconds;

  public SplitandCountandRedis() {
	this(DEFAULT_NUM_STOR,DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }
  public SplitandCountandRedis(int numStor,int emitFrequencyInSeconds) {
    this.numStor = numStor;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    //counter = new SlidingWindowCounterRedis<Object>(this.numStor,this.emitFrequencyInSeconds);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    counter = new SlidingWindowCounterRedis<Object>(this.numStor,this.emitFrequencyInSeconds);
  }


 public static <T> List<List<T>> combination(List<T> values, int size) {
    if (0 == size) {
        return Collections.singletonList(Collections.<T> emptyList());
    }
    if (values.isEmpty()) {
        return Collections.emptyList();
    }
    List<List<T>> combination = new LinkedList<List<T>>();
    T actual = values.iterator().next();
    List<T> subSet = new LinkedList<T>(values);
    subSet.remove(actual);
    List<List<T>> subSetCombination = combination(subSet, size - 1);
    for (List<T> set : subSetCombination) {
        List<T> newSet = new LinkedList<T>(set);
        newSet.add(0, actual);
        combination.add(newSet);
    }
    combination.addAll(combination(subSet, size));
    return combination;
}






  private void emitCurrentWindowCounts() {
        counter.getCountsThenAdvanceWindow(collector);
  }

  private void countObjAndAck(Object obj,long logtime) {
    //convert source logtime,match it to hashmap key
    logtime = logtime-logtime%emitFrequencyInSeconds + emitFrequencyInSeconds;
    counter.incrementCount(logtime,obj);
    }

  @Override
  public void execute(Tuple tuple) {
	collector.ack(tuple);
     if (TupleHelpers.isTickTuple(tuple)) {
	emitCurrentWindowCounts();
	}
     else{		
	String jsonstring = tuple.getValue(0).toString();
	JSONObject jo = new JSONObject(jsonstring);
	String  timestr = jo.getString("timestamp").trim(); 
	String[] timearray = timestr.split("\\.");
	SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
        try{
	long timelong = sdf.parse(timearray[0]).getTime()/1000;
	//LOG.info("##timestamp is:"+timestamp); 
	List wordlist = new ArrayList();
	String wordtmp = "";
        List wordtmp1 = new ArrayList();
	List wordtmp2 = new LinkedList();
	StringBuilder strbuild = new StringBuilder();
	if(jo.getString("statusCode").trim().length()!=0)
		wordlist.add(jo.getString("statusCode").trim());
	if(jo.getString("province").trim().length()!=0)
		wordlist.add(jo.getString("province").trim());
	if(jo.getString("city").trim().length()!=0)
		wordlist.add(jo.getString("city").trim());
	if(jo.getString("isp").trim().length()!=0)
		wordlist.add(jo.getString("isp").trim());
	if(jo.getString("uri").trim().length()!=0)
		wordlist.add(jo.getString("uri").trim());	
	//wordlist.size()
	for(int i = 1; i <= wordlist.size();i++){
		wordtmp1 = combination(wordlist,i);
		for(int j = 0;j<wordtmp1.size();j++){
			wordtmp2 = (LinkedList)wordtmp1.get(j);
			for(int z = 0;z<wordtmp2.size();z++){
				strbuild.append(wordtmp2.get(z));
				if(z < wordtmp2.size()-1)
					strbuild.append(" ");	
			}
			wordtmp = strbuild.toString();
			//collector.emit(new Values(timestamp, wordtmp));
			//collector.ack(tuple);
			countObjAndAck(wordtmp,timelong);
			strbuild.delete(0,strbuild.length());	
		}			
	}
	}catch(Exception e){
			e.printStackTrace();
	}
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  	//declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds")); for SumBolt
  	declarer.declare(new Fields("obj", "actualtime","olddeltime","count")); //for redisBolt
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return conf;
  }


}
