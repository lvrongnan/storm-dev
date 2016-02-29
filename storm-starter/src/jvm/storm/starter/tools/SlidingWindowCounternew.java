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
import java.util.Map;

import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.Format;
import java.util.Iterator;
import java.util.Arrays;
import java.lang.Integer;
import java.lang.Long;

import org.apache.log4j.Logger;
import backtype.storm.task.OutputCollector;
/**
 * This class counts objects in a sliding window fashion.
 * <p/>
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread (e.g. {@link PeriodicSlidingWindowCounter}) read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
 * <p/>
 * A note for analyzing data based on a sliding window count: During the initial <code>windowLengthInSlots</code>
 * iterations, this sliding window counter will always return object counts that are equal or greater than in the
 * previous iteration. This is the effect of the counter "loading up" at the very start of its existence. Conceptually,
 * this is the desired behavior.
 * <p/>
 * To give an example, using a counter with 5 slots which for the sake of this example represent 1 minute of time each:
 * <p/>
 * <pre>
 * {@code
 * Sliding window counts of an object X over time
 *
 * Minute (timeline):
 * 1    2   3   4   5   6   7   8
 *
 * Observed counts per minute:
 * 1    1   1   1   0   0   0   0
 *
 * Counts returned by counter:
 * 1    2   3   4   4   3   2   1
 * }
 * </pre>
 * <p/>
 * As you can see in this example, for the first <code>windowLengthInSlots</code> (here: the first five minutes) the
 * counter will always return counts equal or greater than in the previous iteration (1, 2, 3, 4, 4). This initial load
 * effect needs to be accounted for whenever you want to perform analyses such as trending topics; otherwise your
 * analysis algorithm might falsely identify the object to be trending as the counter seems to observe continuously
 * increasing counts. Also, note that during the initial load phase <em>every object</em> will exhibit increasing
 * counts.
 * <p/>
 * On a high-level, the counter exhibits the following behavior: If you asked the example counter after two minutes,
 * "how often did you count the object during the past five minutes?", then it should reply "I have counted it 2 times
 * in the past five minutes", implying that it can only account for the last two of those five minutes because the
 * counter was not running before that time.
 *
 * @param <T> The type of those objects we want to count.
 */
public final class SlidingWindowCounternew<T> implements Serializable {

  private static final long serialVersionUID = -2645063988768785810L;

  private SlotBasedCounternew<T> objCounter;
  private int objSlot;
  private int num_stor;
  private int freq;
  private int windowLengthInSlots;
  private HashMap timeseq;
  private Iterator it;
  private static final Logger LOG = Logger.getLogger(SlidingWindowCounternew.class);

 
 public SlidingWindowCounternew(int num_stor,int freq) {
    if (num_stor < 3) {
      throw new IllegalArgumentException(
          "num_stor in must be at least three (you requested " + num_stor + ")");
    }
    this.num_stor = num_stor;
    this.freq = freq;

    //build timeseq hashmap
    this.timeseq = new HashMap();
    Date date=new Date();
    long tmp,diff;
    tmp=date.getTime()/1000;
    diff=tmp%freq;
    for(int i=0;i<num_stor+10;i++){
	    //LOG.info("&&&put"+i+"to timeseq");
            this.timeseq.put((tmp-diff+10*freq-i*freq),i);
    }

    this.objCounter = new SlotBasedCounternew<T>(this.num_stor);

    this.objSlot = 0;
  }

  public void incrementCount(long logtime,T obj) {
    objSlot = slotSelect(logtime);
    objCounter.incrementCount(obj, objSlot);
  }

  /**
   * Return the current (total) counts of all tracked objects, then advance the window.
   * <p/>
   * Whenever this method is called, we consider the counts of the current sliding window to be available to and
   * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
   * objects within the next "chunk" of the sliding window.
   *
   * @return The current (total) counts of all tracked objects.
   */
    public void  getCountsThenAdvanceWindow(OutputCollector collector) {
    //sort key
    Object[] key_arr = timeseq.keySet().toArray();
    Arrays.sort(key_arr);
    //LOG.info("the min arr:"+(key_arr[0]).toString());
    //LOG.info("the max arr:"+(key_arr[19]).toString());
    //now test is 4,this will change
    //4->1
    int now_slot = Integer.parseInt(timeseq.get(key_arr[num_stor-1]).toString());
    objCounter.getCounts((key_arr[num_stor-1]).toString(),now_slot,collector);
    int last_slot = Integer.parseInt(timeseq.get(key_arr[0]).toString());
    objCounter.wipeSlot(last_slot);
    long minkey = Long.parseLong(key_arr[0].toString());
    long maxkey = Long.parseLong(key_arr[num_stor+10-1].toString());
    //LOG.info("####maxkey is: "+maxkey+" min key is: "+minkey+" now_slot is: "+now_slot+"now tick logtime is: "+key_arr[this.num_stor-1].toString());
    advanceHead(minkey,maxkey);
  }

  private void advanceHead(long minkey,long maxkey) {
    int slot_num = Integer.parseInt(this.timeseq.get(minkey).toString());
    long new_key = maxkey + (long)this.freq;
    //LOG.info("###advanceHead new keyis: "+new_key);
    this.timeseq.put(new_key,slot_num);
    this.timeseq.remove(minkey);
  }

  private int slotSelect(long logtime) {
        //LOG.info("&&&&&&&&&&&&&&&&comin logtime is: "+logtime);
        //it = this.timeseq.entrySet().iterator();
        //while(it.hasNext()){
        //        LOG.info("&&&&&&&"+it.next());
        //}
	if(timeseq.containsKey(logtime)){
		//LOG.info("&&&&&&&slot is"+timeseq.get(logtime).toString()+"logtime is: "+logtime);
    		return Integer.parseInt(timeseq.get(logtime).toString());
	}
	if(timeseq.isEmpty()){
		//LOG.info("##############timeseq is null");
		return 0;
	}
	return 0;
  }


}
