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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

/**
 *  Stores information about a field, including some basic statistics
 */
public class FieldInfoItem {
    public final String key;
    public final String name;
    public final String index;
    public final String indexOptions;
    public final String docValuesOptions;
    public final String es_type;
    public final ShardFieldStats stats;

    public void toXContent(XContentBuilder builder) throws IOException {
        builder.startObject(name);
        builder.field("indexOptions", indexOptions);
        builder.field("docValuesOptions", docValuesOptions);
        builder.field("es_type", es_type);

        builder.field("doc_count", stats.docCount);
        builder.field("term_count", stats.termCount);

        TypeHandler typeHandler = TypeHandler.create(es_type);
        builder.field("term_lo", typeHandler.toString(stats.min));
        builder.field("term_hi", typeHandler.toString(stats.max));

        builder.endObject();
    }

    private static String indexOptionsAsString(FieldInfo info, MappedFieldType mft) {
        int dim = info.getPointDataDimensionCount(); //PW7 nakijken: er is ook eentje met index
        int bytes = info.getPointNumBytes();
        IndexOptions indexOptions = info.getIndexOptions();

        if (dim > 0 && bytes > 0) {
            String points = String.format("POINTS(%d::%d)", dim, bytes);
            if ((indexOptions == null || indexOptions == IndexOptions.NONE))
                return points;
            return String.format("%s, %s", indexOptions, points);
        }
        return indexOptions.toString();
    }
    
    private String docValuesAsString(FieldInfo info, MappedFieldType mft) {
        DocValuesType dvt = mft.docValuesType();
        //System.out.printf ("docValuesAsString F=%s, T=%s, dvgen=%d, dvt=%s, hasdv=%s\n", info.name, mft.typeName(), info.getDocValuesGen(), dvt, mft.hasDocValues());

        if (mft.hasDocValues()) {
            if (dvt != DocValuesType.NONE) return dvt.toString();
            
            DocValueFormat dvFormat = mft.docValueFormat(null,  null); 
            if (dvFormat != null) return dvFormat.getWriteableName();
        }

        try {
            mft.fielddataBuilder(index);
            return "FIELDDATA";
        } catch(Throwable th) {}
        return "NONE";
    }

    public FieldInfoItem(String indexName, FieldInfo info, MappedFieldType mft, ShardFieldStats stats) throws IOException {
        key = indexName + "|" + info.name;
        index = indexName;
        name = info.name;
        docValuesOptions = docValuesAsString(info, mft);
        indexOptions = indexOptionsAsString(info, mft);
        this.es_type = mft.typeName();
        this.stats = stats!=null ? stats : new ShardFieldStats();
    }

    public FieldInfoItem(StreamInput in) throws IOException {
        index = in.readString();
        name = in.readString();
        key = index + '|' + name;
        docValuesOptions = in.readString();
        indexOptions = in.readString();
        es_type = in.readString();
        stats = new ShardFieldStats(in);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(name);
        out.writeString(docValuesOptions);
        out.writeString(indexOptions);
        out.writeString(es_type);
        stats.writeTo(out);
    }

    public void consolidate(FieldInfoItem fld) {
        stats.combine(fld.stats);
    }
}
