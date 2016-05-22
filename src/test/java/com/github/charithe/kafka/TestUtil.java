/*
 * Copyright 2016 Charith Ellawala
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.charithe.kafka;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestUtil {
    static List<String> extractValues(ListenableFuture<List<ConsumerRecord<String, String>>> recordFuture) throws Exception {
        return Futures.transform(recordFuture, new Function<List<ConsumerRecord<String,String>>, List<String>>() {
            @Override
            public List<String> apply(List<ConsumerRecord<String, String>> inList) {
                List<String> outList = new ArrayList<>();
                for(ConsumerRecord<String, String> rec : inList){
                    outList.add(rec.value());
                }
                return outList;
            }
        }).get(1, TimeUnit.SECONDS);
    }
}


