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
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.bolt.MysqlOperationperiod;
import storm.starter.bolt.RollingCountBoltnew;
import storm.starter.bolt.SplitKafka;
import storm.starter.bolt.SplitandCount;
import storm.starter.bolt.SplitandCountandRedis;
import storm.starter.bolt.CountBolt;
import storm.starter.bolt.SumBolt;
import storm.starter.bolt.TextBolt;
import storm.starter.bolt.RedisBolt;
import storm.starter.bolt.SumandMysql;
import storm.starter.bolt.SumandText;
import storm.starter.bolt.SumandJdbc;
import storm.starter.bolt.SumandRedis;
import java.util.HashMap;
import java.util.Map;

import java.util.Arrays;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class readfromkafkatopology {
  public static class SplitSentence extends ShellBolt implements IRichBolt {

    public SplitSentence() {
      super("python", "splitsentence_kafka.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("logtime","word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }
  public static class SplitSentencebycomb extends ShellBolt implements IRichBolt {

    public SplitSentencebycomb() {
      super("python", "splitsentence_kafka_comb2.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("logtime","word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }


  public static void main(String[] args) throws Exception {

    String zks = "192.168.111.210:2181,192.168.111.211:2181,192.168.111.212:2181";
    String topic = "adnewtest1";
    String zkRoot = "/brokers";//default is /brokers
    String id = "txt";
    BrokerHosts brokerHosts = new ZkHosts(zks);
    SpoutConfig spoutConf = new SpoutConfig(brokerHosts,topic,zkRoot,id);
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConf.forceFromStart = false;
    //spoutConf.zkServers = Arrays.asList(new String[] {"127.0.0.1"});
    //spoutConf.zkPort = 2181;

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new KafkaSpout(spoutConf), 1);
    //builder.setBolt("splitbycomb", new SplitKafka(), 1).shuffleGrouping("spout");
    //builder.setBolt("splitandcount", new SplitandCount(100,30), 2).shuffleGrouping("spout");
    builder.setBolt("splitandcountandredis", new SplitandCountandRedis(100,30), 2).shuffleGrouping("spout");    
    //builder.setBolt("SumandRedis", new SumandRedis(100,30), 1).shuffleGrouping("splitandcount");
    //builder.setBolt("sum", new SumBolt(100,30),1).shuffleGrouping("splitandcount");
    //builder.setBolt("redis", new RedisBolt(),2).shuffleGrouping("splitandcount");
    //  builder.setBolt("text", new TextBolt(),2).shuffleGrouping("splitandcount");
    //builder.setBolt("sumandtext", new SumandText(100,30),1).shuffleGrouping("splitandcount");

    Config conf = new Config();
    conf.setDebug(false);
    conf.setNumAckers(3);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(10);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(5);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
