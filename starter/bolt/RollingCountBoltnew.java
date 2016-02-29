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
public class RollingCountBoltnew extends BaseRichBolt {

  private static final Logger LOG = Logger.getLogger(RollingCountBolt.class);
  private static final int DEFAULT_NUM_STOR = 20;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 5*60;
  private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
      "Actual window length is %d seconds when it should be %d seconds"
          + " (you can safely ignore this warning during the startup phase)";

  private final SlidingWindowCounternew<Object> counter;
  private final int numStor;
  private final int emitFrequencyInSeconds;
  private OutputCollector collector;


  public RollingCountBoltnew() {
    this(DEFAULT_NUM_STOR,DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public RollingCountBoltnew(int numStor,int emitFrequencyInSeconds) {
    this.numStor = numStor;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    counter = new SlidingWindowCounternew<Object>(this.numStor,this.emitFrequencyInSeconds);
  }


  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
  
    if (TupleHelpers.isTickTuple(tuple)) {
      //LOG.info("Received tick tuple, triggering emit of current window counts");
      emitCurrentWindowCounts();
    }
    else {
      countObjAndAck(tuple);
      //LOG.info("pass");
    }
    collector.ack(tuple);
  }

  private void emitCurrentWindowCounts() {
   	counter.getCountsThenAdvanceWindow(collector);
  }


  private void countObjAndAck(Tuple tuple) {
    Object obj = tuple.getValue(1);
    long logtime = Long.parseLong(tuple.getValue(0).toString());
    //convert source logtime,match it to hashmap key
    logtime = logtime-logtime%emitFrequencyInSeconds + emitFrequencyInSeconds;
    counter.incrementCount(logtime,obj);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return conf;
  }
}
