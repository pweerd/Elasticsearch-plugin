/*
 * Licensed to De Bitmanager under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. De Bitmanager licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package nl.bitmanager.elasticsearch.extensions.cachedump;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import nl.bitmanager.elasticsearch.extensions.cachedump.CacheDumpTransportItem.SortType;

public class IndexCacheInfo {
    private final Map<String, Map<String, CacheInfo>> indexCacheInfo;
    private final Set<String> indexSet;
    private String errorMsg;
    
    public IndexCacheInfo() {
        indexCacheInfo = new HashMap<String, Map<String, CacheInfo>>();
        indexSet = new HashSet<String>();
    }
    
    public IndexCacheInfo(StreamInput in) throws IOException {
        int N = in.readInt();
        indexSet = new HashSet<String>(N);
        for (int i=0; i<N; i++) indexSet.add(in.readString());
        
        N = in.readInt();
        Map <String, Map<String, CacheInfo>> indexCacheMap = new TreeMap <String, Map<String, CacheInfo>> ();
        for (int i=0; i<N; i++) {
            String index = in.readString();
            int M = in.readInt();
            TreeMap<String, CacheInfo> tmp = new TreeMap<String, CacheInfo>();
            for (int j=0; j<M; j++) {
                CacheInfo item = new CacheInfo(in);
                tmp.put(item.query, item);
            }
            indexCacheMap.put (index, tmp);
        }
        indexCacheInfo = indexCacheMap;
    }
    
    public void writeTo (StreamOutput out) throws IOException {
        int N = indexSet.size();
        out.writeInt(N);
        for (String x: indexSet) writeStr (out, x);
        
        N = indexCacheInfo.size();
        out.writeInt(N);
        for (Entry<String, Map<String, CacheInfo>> kvp1: indexCacheInfo.entrySet()) {
            writeStr(out, kvp1.getKey()); 
            Map<String, CacheInfo> statsPerQuery = kvp1.getValue();
            int M = statsPerQuery==null ? 0 : statsPerQuery.size();
            out.writeInt(M);
            if (M > 0) {
                for (Entry<String, CacheInfo> kvp2: statsPerQuery.entrySet()) {
                    kvp2.getValue().writeTo(out);
                }
            }
        }
    }
    
    
    public XContentBuilder toXContent(XContentBuilder builder, String name, SortType sort, boolean dumpRaw) throws IOException {
        if (errorMsg != null) {
            return builder.field(name, errorMsg);
        }
        builder.startArray(name);
        for (Entry<String, Map<String, CacheInfo>> kvp: indexCacheInfo.entrySet()) {
            builder.startObject();
            builder.field("index", kvp.getKey());
            builder.startArray("caches");
            if (sort==SortType.Query) {
                for (Entry<String, CacheInfo> kvp2: kvp.getValue().entrySet()) {
                    kvp2.getValue().toXContent(builder);
                }
            } else {
                List<CacheInfo> list = new ArrayList<CacheInfo>(kvp.getValue().size());
                for (Entry<String, CacheInfo> kvp2: kvp.getValue().entrySet()) {
                    list.add(kvp2.getValue());
                }
                Collections.sort(list, CacheInfo.sortBytes);
                for (CacheInfo ci: list) ci.toXContent(builder);
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();
        
        if (dumpRaw) {
            builder.startArray(name + "_raw_keys");
            for (String x: indexSet) {
                builder.value(x);
            }
            builder.endArray();
        }
        return builder;
    }

    
    
    public void addIndex (String index) {
        indexSet.add(index);
    }
    
    public Map<String, CacheInfo> getStatsPerKeyForIndex (String index) {
        Map<String, CacheInfo> statsPerKey = indexCacheInfo.get(index);
        if (statsPerKey == null) {
            statsPerKey = new HashMap<String, CacheInfo>();
            indexCacheInfo.put(index, statsPerKey);
        }
        return statsPerKey;
    }

    
    
    
    public static void writeStr(StreamOutput out, String x) throws IOException {
        out.writeString(x==null ? "" : x);
    }

}
