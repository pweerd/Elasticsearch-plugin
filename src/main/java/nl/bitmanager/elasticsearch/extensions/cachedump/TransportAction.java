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
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReader.CacheKey;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.ShardCoreKeyMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache.Value;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import nl.bitmanager.elasticsearch.support.RegexReplace;
import nl.bitmanager.elasticsearch.transport.NodeRequest;
import nl.bitmanager.elasticsearch.transport.NodeTransportActionBase;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

public class TransportAction extends NodeTransportActionBase {
    
   private final IndicesService indicesService;


   @Inject
   public TransportAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
           TransportService transportService, ActionFilters actionFilters,
           IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService) {
       super(ActionDefinition.INSTANCE, settings, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver);
       this.indicesService = indicesService;
   }
   

   @Override
   protected TransportItemBase handleNodeRequest(NodeRequest request) throws Exception {
       CacheDumpTransportItem item = new CacheDumpTransportItem (request, null);
       _CacheGetter worker = new _CacheGetter(indicesService, item);
       SecurityManager sm = System.getSecurityManager();
       if (sm != null) {
           System.out.println("sm != null2");
           sm.checkPermission(new SpecialPermission());
           System.out.println("after check2");
       }
       Throwable th = (Throwable)AccessController.doPrivileged (worker);
       if (th != null) item.errorMsg = th.toString() + "\nIs the correct plugin-security.policy in place?";;
       
       return item;
   }

   

    static class _PrivilegeHelper {
       public static Object getField(Object obj, String fld) throws Exception {
           if (obj==null) return null;

           Class<? extends Object> c = obj.getClass();
           while (c != null) {
               for (Field f : c.getDeclaredFields()) {
                   if (!fld.equals(f.getName())) continue;
                   
                   f.setAccessible(true);
                   return f.get(obj);
               }
               c = c.getSuperclass();
           }
           return null;
       }
   }
   
   static class _CacheGetter extends _PrivilegeHelper implements PrivilegedAction<Object> {
       Object lruCache;
       ShardCoreKeyMap shardKeyMap;
       Map<Object,Object> luceneInternalCache;
       
       final IndicesService indicesService;
       final CacheDumpTransportItem req;
       Throwable error;

       public _CacheGetter(IndicesService indicesService, CacheDumpTransportItem req) {
           this.req = req;
           this.indicesService = indicesService;
       }

        @Override
        public Object run() {
            System.out.println("Running cachegetter + " + req.cacheType);
            try {
                switch (req.cacheType) {
                    case Query: processQueryCache(); break;
                    case Request: processRequestCache(); break;
                    case Bitset: processBitsetCache(); break;
                }
                return null;
            } catch (Throwable th) {
                th.printStackTrace();
                return th;
            }
        }

        @SuppressWarnings("rawtypes")
        private void processRequestCache() throws Exception {
            System.out.println("Fetching REQUEST cache");
            IndicesRequestCache requestCache = (IndicesRequestCache) getField(indicesService, "indicesRequestCache");
            Cache lruCache = (Cache) getField(requestCache, "cache");

            Map<String, Map<String, CacheInfo>> indexCacheMap = new HashMap<String, Map<String, CacheInfo>>();
            Set<String> indexSet = new HashSet<String>();

            final RegexReplace indexReplacer = req.getIndexReplacer();
            final boolean dumpRaw = req.dumpRaw;
            for (Object key: lruCache.keys()) {
                Object v = lruCache.get(key);
                if (v==null) continue;
                
                long bytes = (v instanceof BytesReference) ? ((BytesReference)v).ramBytesUsed() : 0;
                String q = parseKey((BytesReference) getField(key, "value"));
                if (dumpRaw) indexSet.add(q);
                
                String index;
                if (indexReplacer==null) 
                    index = "_ALL";
                else {
                    index = indexReplacer.extract(q);
                    if (index == null)
                        index = "?";
                    else {
                        int idx = q.indexOf(index) + index.length();
                        index = index.trim();
                        q = q.substring(idx);
                    }
                }
                    
                CacheInfo cacheInfo = new CacheInfo (q, bytes);
                
                Map<String, CacheInfo> statsPerQuery = indexCacheMap.get(index);
                if (statsPerQuery == null) {
                    statsPerQuery = new HashMap<String, CacheInfo>();
                    indexCacheMap.put(index, statsPerQuery);
                }
                
                CacheInfo existing = statsPerQuery.get(q);
                if (existing != null) {
                    existing.combine(cacheInfo);
                    continue;
                }
                statsPerQuery.put(cacheInfo.query, cacheInfo);
            }
            req.setCacheInfo(indexSet, indexCacheMap, null);
        }
        
        private String parseKey (BytesReference key) throws IOException {
            StreamInput strm = key.streamInput();

            StringBuilder sb = new StringBuilder(); 
            sb.append(strm.readString());
            boolean emitSpace = false;
            while(true) {
                int b = strm.read();
                if (b<0) break;
                if (b <= 0x20 || b >= 128) {
                    emitSpace = true;
                    continue;
                };
                if (emitSpace) {
                    sb.append(' ');
                    emitSpace = false;
                }
                sb.append ((char)b);
            }
            return sb.toString();            
        }
        
        

        @SuppressWarnings("unchecked")
        private void processQueryCache() throws Exception {
            System.out.println("Fetching QUERY cache");
            IndicesQueryCache indicesQueryCache = indicesService.getIndicesQueryCache();
            LRUQueryCache lruCache = (LRUQueryCache) getField(indicesQueryCache, "cache");
            shardKeyMap = (ShardCoreKeyMap) getField(indicesQueryCache, "shardKeyMap");
            luceneInternalCache = lruCache == null ? null : (Map<Object, Object>) getField(lruCache, "cache");
            if (lruCache == null)
                throw new RuntimeException("IndicesQueryCache::cache==null");
            if (luceneInternalCache == null)
                throw new RuntimeException("IndicesQueryCache::cache::cache==null");
            Map<String, Map<String, CacheInfo>> indexCacheMap = new HashMap<String, Map<String, CacheInfo>>();
            Set<String> indexSet = new HashSet<String>();

            final RegexReplace indexReplacer = req.getIndexReplacer();
            final boolean dumpRaw = req.dumpRaw;

            for (Entry<Object, Object> kvp : luceneInternalCache.entrySet()) {
                String possibleDir = getDirectoryName(kvp.getKey());
                if (dumpRaw) indexSet.add(possibleDir);
                String index = indexReplacer == null ? "_ALL" : indexReplacer.extract(possibleDir);

                Map<String, CacheInfo> statsPerQuery = indexCacheMap.get(index);
                if (statsPerQuery == null) {
                    statsPerQuery = new HashMap<String, CacheInfo>();
                    indexCacheMap.put(index, statsPerQuery);
                }

                Object leafCache = kvp.getValue();
                Map<Query, DocIdSet> leafCacheMap = (Map<Query, DocIdSet>) getField(leafCache, "cache");
                if (leafCacheMap == null) {
                    // PWerrorMsg = "Field 'cache' was not found in object " +
                    // Utils.getType(leafCache);
                    continue;
                }
                for (Entry<Query, DocIdSet> kvp2 : leafCacheMap.entrySet()) {
                    CacheInfo info = new CacheInfo(kvp2.getKey(), kvp2.getValue());
                    CacheInfo existing = statsPerQuery.get(info.query);
                    if (existing != null) {
                        existing.combine(info);
                        continue;
                    }
                    statsPerQuery.put(info.query, info);
                }
            }
            req.setCacheInfo(indexSet, indexCacheMap, null);

        }
        @SuppressWarnings("unchecked")
        private void processBitsetCache() throws Exception {
            System.out.println("Fetching BITSET cache");
            Map<String, Map<String, CacheInfo>> indexCacheMap = new HashMap<String, Map<String, CacheInfo>>();
            Set<String> indexSet = new HashSet<String>();

            Iterator<IndexService> iter = indicesService.iterator();
            while (iter.hasNext()) {
                IndexService indexService = iter.next();
                BitsetFilterCache bitsetCache = indexService.cache().bitsetFilterCache();
                String name = indexService.index().getName();
                indexSet.add(name);
                
                Cache<IndexReader.CacheKey, Cache<Query, Value>> loadedFilters = (Cache<CacheKey, Cache<Query, Value>>) getField(bitsetCache, "loadedFilters");
                for (Cache<Query, Value> cc: loadedFilters.values()) {
                    for (Query key: cc.keys()) {
                        Value value = cc.get(key);
                        if (value == null) continue;
                        
                        BitSet bitset = (BitSet) getField (value, "bitset");
                        CacheInfo info = new CacheInfo(key, bitset);

                        Map<String, CacheInfo> statsPerQuery = indexCacheMap.get(name);
                        if (statsPerQuery == null) {
                            statsPerQuery = new HashMap<String, CacheInfo>();
                            indexCacheMap.put(name, statsPerQuery);
                        }
                        
                        CacheInfo existing = statsPerQuery.get(info.query);
                        if (existing != null) {
                            existing.combine(info);
                            continue;
                        }
                        statsPerQuery.put(info.query, info);
                    }
                }
            }
            req.setCacheInfo(indexSet, indexCacheMap, null);
        }

        public static String getDirectoryName(Object obj) throws Exception {
            if (obj == null)
                return "NULL1";
            String clsName = obj.getClass().getName();
            if (!"org.apache.lucene.index.SegmentCoreReaders".equals(clsName))
                return "NOSEGMENTCORE[" + clsName + "]";

            return getDirectoryName((Directory) getField(obj, "cfsReader")).replace('\\', '/');
        }

        public static String getDirectoryName(Directory dir) {
            if (dir == null)
                return "NULL2";
            if (dir instanceof FSDirectory) {
                FSDirectory fsDir = (FSDirectory) dir;
                return fsDir.getDirectory().toString();
            }
            return "NOFSDIR[" + dir.getClass().getName() + "] tos=" + dir.toString();
        }
    }

}