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
import java.util.Comparator;

import nl.bitmanager.elasticsearch.support.Utils;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

/** 
 * Holds raw cache information out of the LruCache
 */
public class CacheInfo {
    public final String query;
    public long ramBytesUsed;
    public CacheInfo (Query query, DocIdSet docidSet) {
        this.query = query.toString();
        this.ramBytesUsed = docidSet.ramBytesUsed();
    }
    public CacheInfo (String query, long bytesUsed) {
        this.query = query;
        this.ramBytesUsed = bytesUsed;
    }
    
    public CacheInfo(StreamInput in) throws IOException {
        this.query = TransportItemBase.readStr (in);
        this.ramBytesUsed = in.readVLong();
    }

    public void writeTo(StreamOutput out) throws IOException {
        TransportItemBase.writeStr(out, query);
        out.writeVLong(ramBytesUsed);
    }

    public void combine (CacheInfo other) {
        this.ramBytesUsed += other.ramBytesUsed;
    }

    public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("query", query);
        builder.field("ram", ramBytesUsed);
        builder.field("ram_pretty", Utils.prettySize(ramBytesUsed));
        builder.endObject();
        return builder;
    }

    public static final Comparator<CacheInfo> sortBytes = new Comparator<CacheInfo>() {
        @Override
        public int compare(CacheInfo arg0, CacheInfo arg1) {
            return Long.compare(arg1.ramBytesUsed, arg0.ramBytesUsed);
        }
    };
}

