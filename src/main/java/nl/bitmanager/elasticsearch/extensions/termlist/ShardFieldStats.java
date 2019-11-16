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

package nl.bitmanager.elasticsearch.extensions.termlist;

import java.io.IOException;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import nl.bitmanager.elasticsearch.support.BytesHelper;
import nl.bitmanager.elasticsearch.support.Utils;

/**
 * Hold the statistics for 1 field, 1 shard.
 * Combining by adding doccount and taking the max of the termcount
 */
public class ShardFieldStats {
    private static final byte[] EMPTY = new byte[0] ;
    public byte[] min;
    public byte[] max;
    public long docCount;
    public long termCount;

    public ShardFieldStats() {
        min = EMPTY;
        max = EMPTY;
        docCount = 0;
        termCount = 0;
    }

    public ShardFieldStats(StreamInput in) throws IOException {
        min = in.readByteArray();
        max = in.readByteArray();
        termCount = in.readVLong();
        docCount = in.readVLong();
    }

    public void combine (ShardFieldStats other) {
        docCount += other.docCount;
        termCount = Math.max(termCount, other.termCount);
        if (min.length==0 || BytesHelper.bytesComparer.compare(min, other.min) > 0) min = other.min;
        if (BytesHelper.bytesComparer.compare(max, other.max) < 0) max = other.max;
    }

    public ShardFieldStats(Terms terms) throws IOException {
        docCount = terms.getDocCount();
        termCount = getTermCount(terms);
        min = Utils.cloneBytes (terms.getMin(), EMPTY);
        max = Utils.cloneBytes (terms.getMax(), EMPTY);
    }

    public ShardFieldStats(PointValues points) throws IOException {
        docCount = points.getDocCount();
        termCount = points.size();
        min = Utils.cloneBytes (points.getMinPackedValue(), EMPTY);
        max = Utils.cloneBytes (points.getMaxPackedValue(), EMPTY);
    }



    private static long getTermCount (Terms terms) throws IOException {
        long ret = terms.size();
        if (ret < 0){
            ret =0;
            TermsEnum e = terms.iterator();
            while (null != e.next())  ++ret;
        }
        return ret;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(min);
        out.writeByteArray(max);
        out.writeVLong(termCount);
        out.writeVLong(docCount);
    }

}
