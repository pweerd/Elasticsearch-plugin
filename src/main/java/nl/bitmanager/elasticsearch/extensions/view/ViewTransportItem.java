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

package nl.bitmanager.elasticsearch.extensions.view;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestRequest;

import nl.bitmanager.elasticsearch.transport.ActionDefinition;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;

public class ViewTransportItem extends TransportItemBase {
    public byte[] json;
    public final String type, id, fieldFilter, fieldExpr, outputFilter;
    public final int outputLevel;
    public final int docOffset;
    public final boolean postings, dense;

    public ViewTransportItem(ActionDefinition definition) {
        super(definition);
        type = null;
        id = null;
        fieldFilter = null;
        fieldExpr = null;
        outputFilter = null;
        outputLevel = 0;
        docOffset = 0;
        postings = false;
        dense = false;
    }

    public ViewTransportItem(ActionDefinition definition, RestRequest req) {
        super(definition);
        type = req.param("type");
        id = req.param("id");
        fieldFilter = req.param("field");
        fieldExpr = req.param("field_expr");
        outputFilter = req.param("output");
        outputLevel = req.paramAsInt("output_lvl",  0);
        docOffset = req.paramAsInt("offset",  0);
        postings = req.paramAsBoolean("postings", false);
        dense = req.paramAsBoolean("dense", false);
    }

    public ViewTransportItem(ViewTransportItem other) {
        super(other.definition);
        type = other.type;
        id = other.id;
        fieldFilter = other.fieldFilter;
        fieldExpr = other.fieldExpr;
        outputFilter = other.outputFilter;
        outputLevel = other.outputLevel;
        docOffset = other.docOffset;
        postings = other.postings;
        dense = other.dense;
    }


    public ViewTransportItem (ActionDefinition definition, StreamInput in) throws IOException {
        super(definition, in);
        type = TransportItemBase.readStr(in);
        id = TransportItemBase.readStr(in);
        fieldFilter = TransportItemBase.readStr(in);
        fieldExpr = TransportItemBase.readStr(in);
        outputFilter = TransportItemBase.readStr(in);
        outputLevel = in.readInt();
        docOffset = in.readInt();
        json = readByteArray(in);
        postings = in.readBoolean();
        dense = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        TransportItemBase.writeStr(out, type);
        TransportItemBase.writeStr(out, id);
        TransportItemBase.writeStr(out, fieldFilter);
        TransportItemBase.writeStr(out, fieldExpr);
        TransportItemBase.writeStr(out, outputFilter);
        out.writeInt(outputLevel);
        out.writeInt(docOffset);
        writeByteArray(out, json);
        out.writeBoolean(postings);
        out.writeBoolean(dense);
    }
    @Override
    protected void consolidateResponse(TransportItemBase _other) {
        ViewTransportItem other = (ViewTransportItem)_other;
        if (other.json==null) return;
        json = other.json;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("request");
        builder.field("field", fieldFilter);
        builder.field("field_expr", fieldExpr);
        builder.field("output", outputFilter);
        builder.field("output_lvl", outputLevel);
        builder.field("postings", postings);
        builder.field("dense", dense);
        builder.field("offset", docOffset);
        builder.endObject();
        if (json==null || json.length==0) return builder;
        builder.rawField ("doc", new ByteArrayInputStream (json), XContentType.JSON);
        return builder;
    }

    public void processShard (IndicesService indicesService, IndexShard indexShard) throws Exception {
        Searcher searcher = indexShard.acquireSearcher("view");
        try {
            BooleanQuery.Builder b = new BooleanQuery.Builder();
            //b.add(new TermQuery (new Term (IdFieldMapper.NAME, Uid.createUidAsBytes (type, id))), Occur.SHOULD);
            b.add(new TermQuery (new Term ("_id", Uid.encodeId(id))), Occur.SHOULD);
            b.setMinimumNumberShouldMatch(1);
            BooleanQuery bq = b.build();
            System.out.printf("TERM=%s, id=%s, type=%s\n", b, id, type);
            List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
            System.out.println("shard rdr: " + searcher.getIndexReader().getClass().getName());
            for (LeafReaderContext leaf : leaves) {
                LeafReader leafRdr = leaf.reader();
                IndexSearcher leafSearcher = new IndexSearcher (leafRdr);
                //Try to locate the doc
                TopDocs topdocs = leafSearcher.search (bq,  1);
                if (topdocs.totalHits.value<=0) continue;

                int docid = topdocs.scoreDocs[0].doc + docOffset;
                System.out.printf("-- doc=%d, use doc=%d\n", topdocs.scoreDocs[0].doc, docid);
                Document d = leaf.reader().document(docid);
//                for (IndexableField f: d) {
//                    System.out.println("F=" + f);
//                }
                DocInverter di = new DocInverter (this, docid, d, leaf, null, type, indicesService, indexShard);
                json = di.jsonBytes;
                break; //Found! So we are done...
            }
        } finally {
            searcher.close();
        }
    }
}
