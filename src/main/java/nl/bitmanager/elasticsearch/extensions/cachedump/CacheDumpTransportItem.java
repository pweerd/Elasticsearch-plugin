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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import nl.bitmanager.elasticsearch.support.RegexReplace;
import nl.bitmanager.elasticsearch.support.Utils;
import nl.bitmanager.elasticsearch.transport.ActionDefinition;
import nl.bitmanager.elasticsearch.transport.NodeRequest;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestRequest;


public class CacheDumpTransportItem extends TransportItemBase {
    public enum CacheType {Request, Query, Bitset};
    public enum SortType {Query, Size};
    private Map<String, Map<String, CacheInfo>> indexCacheMap;
    private Set<String> indexSet;
    private String indexExpr;
    private RegexReplace replacer;
    private SortType sort;
    public boolean dumpRaw;
    String errorMsg;
    public CacheType cacheType;

    public CacheDumpTransportItem(ActionDefinition definition) {
        super(definition);
        cacheType = CacheType.Query;
    }

    public CacheDumpTransportItem(ActionDefinition definition, RestRequest req) {
        super(definition);
        if (definition.debug) {
            definition.logger.info("CacheDumpTransportItem::CacheDumpTransportItem from req.");
            definition.logger.info("Params=" + req.params().toString());
        }
        indexExpr = req.param("index_expr");
        
        String sortType = req.param("sort", "size").toLowerCase();
        if (sortType==null || sortType.equals("size")) 
            sort = SortType.Size;
        else if (sortType.equals("query")) 
            sort = SortType.Query;
        else throw new RuntimeException ("Unsupported value for sort: [" + sortType + "]. Valid: query, size.");

        dumpRaw = req.paramAsBoolean("dump_raw", false);
        String type=req.param("type", "query").toLowerCase();
        if (type.equals("request")) {
            cacheType = CacheType.Request;
            if (indexExpr == null) indexExpr = "([^ ]+ [^ ]+ )/$1";
        } else if (type.equals("query")) {
            cacheType = CacheType.Query;
            if (indexExpr == null) indexExpr = "//indices//(.*)//\\d*//index/$1";
        } else if (type.equals("bitset")) {
            cacheType = CacheType.Bitset;
            if (indexExpr == null) indexExpr = "(.*)/$1";
        }  else throw new RuntimeException ("Unsupported value for type: [" + type + "]. Valid: query, request, bitset.");

        if ("null".equals(indexExpr) || (indexExpr != null && indexExpr.length()==0)) indexExpr = null;
    }
    
    public CacheDumpTransportItem(NodeRequest req, String initError) {
        this(req.definition);
        CacheDumpTransportItem other = (CacheDumpTransportItem)req.getTransportItem();
        indexExpr = other.indexExpr;
        sort = other.sort;
        dumpRaw = other.dumpRaw;
        errorMsg  = initError;
        cacheType = other.cacheType;
    }
    
    public CacheDumpTransportItem(ActionDefinition definition, StreamInput in) throws IOException {
        super(definition);
        cacheType = CacheType.values()[in.readByte()];
        sort = SortType.values()[in.readByte()];
        errorMsg = readStr (in);
        indexExpr = readStr (in);
        dumpRaw = in.readBoolean();

        indexSet = readSet (in);
        int N = in.readInt();
        Map <String, Map<String, CacheInfo>> indexCacheMap = new TreeMap <String, Map<String, CacheInfo>>();
        for (int i=0; i<N; i++) {
            String index = readStr (in);
            int M = in.readInt();
            TreeMap<String, CacheInfo> tmp = new TreeMap<String, CacheInfo>();
            for (int j=0; j<M; j++) {
                CacheInfo item = new CacheInfo(in);
                tmp.put(item.query, item);
            }
            indexCacheMap.put (index, tmp);
        }
        this.indexCacheMap = indexCacheMap;
            System.out.printf("[%s]: readFrom->%s\n", "CDTI", this);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte((byte)cacheType.ordinal());
        out.writeByte((byte)sort.ordinal());
        writeStr(out, errorMsg);
        writeStr(out, indexExpr);
        out.writeBoolean(dumpRaw);

        writeSet (out, indexSet);
        int N = indexCacheMap==null ? 0 : indexCacheMap.size();
        out.writeInt(N);
        if (N > 0) {
            for (Entry<String, Map<String, CacheInfo>> kvp1: indexCacheMap.entrySet()) {
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
    }


    public RegexReplace getIndexReplacer() {
        if (indexExpr==null)  return null;
        if (replacer==null) 
            replacer = new RegexReplace(indexExpr);
        return replacer;
    }

    public void setCacheInfo (Set<String> indexSet, Map<String, Map<String, CacheInfo>> indexCacheMap, String errorMsg) {
        this.indexSet = indexSet;
        this.indexCacheMap = indexCacheMap;
        this.errorMsg = errorMsg;
    }
    
    
    @Override
    public String toString () {
        return String.format("%s: expr=%s, sortq=%s, type=%s, creationid=%s", Utils.getTrimmedClass(this), this.indexExpr, this.sort, cacheType, System.identityHashCode(this));
    }
    
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (errorMsg != null && errorMsg.length()>0)
            builder.field("error", errorMsg);
        builder.startObject("request");
        builder.field("type", cacheType.toString());
        builder.field("index_expr", indexExpr);
        builder.field("sort", sort);
        builder.field("dump_raw", dumpRaw);
        builder.endObject();
        builder.startArray("index_caches");
        if (indexCacheMap!=null) {
            for (Entry<String, Map<String, CacheInfo>> kvp: indexCacheMap.entrySet()) {
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
        }
        builder.endArray();


        builder.startArray("raw_keys");
        if (dumpRaw && indexSet!=null) {
            for (String x: indexSet) {
                builder.value(x);
            }
        }
        builder.endArray();
        return builder;
    }
    
    
    @Override
    protected void consolidateResponse(TransportItemBase _other) {
        CacheDumpTransportItem other = (CacheDumpTransportItem) _other;
        if (errorMsg == null || errorMsg.length()==0)
            this.errorMsg = other.errorMsg;
        cacheType = other.cacheType;
        
        if (indexSet==null) {
            indexSet = other.indexSet;
        } else {
            if (other.indexSet != null) {
                indexSet.addAll(other.indexSet);
            }
        }
        
        if (indexCacheMap==null || indexCacheMap.size()==0) {
            this.indexCacheMap = other.indexCacheMap;
        } else {
            if (this.indexCacheMap==null)
                indexCacheMap = new TreeMap<String, Map<String, CacheInfo>>();
            for (Entry<String, Map<String, CacheInfo>> kvp1: other.indexCacheMap.entrySet()) {
                String index = kvp1.getKey();
                
                Map<String, CacheInfo> existing = indexCacheMap.get(index);
                if (existing==null) {
                    indexCacheMap.put(index,  kvp1.getValue());
                    continue;
                }
                
                for (Entry<String, CacheInfo> kvp2: kvp1.getValue().entrySet()) {
                    CacheInfo ci = kvp2.getValue();
                    CacheInfo existingCi = existing.get(ci.query);
                    if (existingCi == null)
                        existing.put(ci.query, ci);
                    else
                        existingCi.combine(ci);
                }
            }
        }
    }
    
}
