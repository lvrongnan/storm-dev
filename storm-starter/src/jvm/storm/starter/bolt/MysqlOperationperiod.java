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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import java.util.Map;

import java.io.File; 
import java.io.BufferedWriter; 
import java.io.FileWriter;

import java.util.List;
import com.google.common.collect.Lists;

import java.lang.Long;
import java.lang.Integer;
import java.text.SimpleDateFormat;
import java.text.Format;

@SuppressWarnings("serial")
public class MysqlOperationperiod extends BaseRichBolt {
  private OutputCollector collector;
  Connection conn = null;
  String from = "test";
  private String word;
  private int num;
  private long realtime;
  private String humanrealtime;
  private FileWriter fw;
  private  Object obj;
  private  long count;
  private Format tmptime;
  private String url = "jdbc:mysql://192.168.111.220/ad_ha";
  private String name = "com.mysql.jdbc.Driver";
  private String user = "root";
  private String password = "funshionsys";

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    try{
        fw = new FileWriter("/tmp/test");
	tmptime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }catch(Exception e){
                e.printStackTrace();
        }
  }
  
  
  @Override
  public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		//int num = (int)tuple.getLongByField("count").longValue();
		//long realtime = tuple.getInteger(2);
		int num = Integer.parseInt(tuple.getString(1));
 		long realtime = Long.parseLong(tuple.getString(2));
		String humanrealtime = tmptime.format(realtime*1000).toString();
                try{
                fw.write(realtime+" "+word+" "+num+" "+humanrealtime+"\n");
                fw.flush();
                }catch (Exception e){
                e.printStackTrace();
                }


		try{
                        Class.forName(name);
                        conn = DriverManager.getConnection(url, user, password);
                }catch(Exception e){
                        e.printStackTrace();
                }
		InsertDB(word,num,humanrealtime);
		this.collector.ack(tuple);	
  }

  private void InsertDB(String word,int num,String realtime){
		
		this.word = word;
		this.num = num;
		this.humanrealtime = realtime;
		String sql = "insert into testtime(content,num,realtime) values "+"("+"'"+this.word+"'"+","+"'"+this.num+"'"+","+"'"+this.humanrealtime+"'"+")";
		//try{
		//fw.write(sql);
		//fw.flush();
		//}catch (Exception e){
                //e.printStackTrace();
        	//}	
		
		try{
                Statement statement = conn.createStatement();
                statement.execute(sql);
		statement.close();
		conn.close();
      		}catch (SQLException e) {
        		e.printStackTrace();
      		}
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer arg0) {
    
  }

}
