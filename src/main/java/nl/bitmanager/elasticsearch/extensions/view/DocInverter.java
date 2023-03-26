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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;

import nl.bitmanager.elasticsearch.support.Utils;
import nl.bitmanager.elasticsearch.typehandlers.BytesHandler;
import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class DocInverter {
    Selector selectedFields, selectedOutput;
    private final int docId;
    private final int outputLevel;
    private final boolean postings, dense;
    private final LeafReader leafRdr;
    private final LeafReaderContext leafCtx;
    private final IndicesService indicesService;
    private final IndexShard indexShard;

    public byte[] jsonBytes;

    private String getTypeFromDoc(Document d, String defType) {
        String uid = d.get("_uid");
        if (uid==null) return defType;

        int idx = uid.indexOf('#');
        if (idx <= 0) return defType;
        return uid.substring(0, idx);
    }

    public DocInverter(ViewTransportItem reqItem, int docid, Document doc, LeafReaderContext leaf, String fields, String type, IndicesService indicesService, IndexShard indexShard) throws Exception {
        this.leafRdr = leaf.reader();
        this.leafCtx = leaf;
        this.indicesService = indicesService;
        this.indexShard = indexShard;
        DocumentFieldMappers fieldMappers = null;
        type = getTypeFromDoc(doc, type);
        if (type != null ) {
            DocumentMapper documentMapper = indexShard.mapperService().documentMapper(type);
            fieldMappers = documentMapper.mappers();
        }

        this.docId = docid;
        this.postings = reqItem.postings;
        this.dense = reqItem.dense;
        selectedFields = new Selector (reqItem.fieldFilter, reqItem.fieldExpr);
        selectedOutput = new Selector (reqItem.outputFilter, null);
        outputLevel = reqItem.outputLevel;

        XContentBuilder json = JsonXContent.contentBuilder();
        json.startObject();
        json.field("shardId", indexShard.shardId());
        json.field("docid", docId);
        loadStoredFields(json, doc, fieldMappers);
        loadIndexedFields(json, doc, leafRdr, fieldMappers);
        json.endObject();

        json.flush();
        jsonBytes = ((ByteArrayOutputStream)json.getOutputStream()).toByteArray();
    }

    private void loadStoredFields(XContentBuilder json, Document d, DocumentFieldMappers fieldMappers) throws IOException {
        if (!selectedOutput.isSelected("stored")) return;
        json.startArray("storedFields");
        for (IndexableField f : d.getFields()) {
            if (!selectedFields.isSelected (f.name().toLowerCase())) continue;

            json.startObject();
            json.field("name", f.name());
            String s = f.stringValue();
            json.field("value", s);
            Number n = f.numericValue();
            if (n != null)
                json.field("value_num", n);

            if (outputLevel > 0 || (s==null && n==null)) {
                json.field("value_rdr", f.readerValue());
                BytesRef binVal = f.binaryValue();
                if (binVal == null)
                    json.field("value_bin", (Object)null);
                else {
                    json.field("value_bin", binVal);
                    TypeHandler h = TypeHandler.create(f.name());
                    json.field("value_binstr", h.toString(TypeHandler.toByteArray(binVal)));
                }
            }
            if (outputLevel > 0) {
                json.field("type", f.fieldType().toString());
                json.field("class", f.getClass().getName());
            }
            json.endObject();
        }
        json.endArray();
    }

    public static FieldInfo[]  getFields (LeafReader leafRdr) {
        FieldInfos fieldInfos = leafRdr.getFieldInfos();
        int N = fieldInfos.size();
        FieldInfo[] ret = new FieldInfo[N];

        int j=0;
        for (int i=0; i<N; i++) {
            ret[j] = fieldInfos.fieldInfo(i);
            if (ret[j]==null) continue;
            j++;
        }
        if (j != N) {
            ret = Arrays.copyOf(ret, j);
            N = j;
        }
        Arrays.sort(ret, fieldComparator);
        return ret;
    }

    private static final Comparator<FieldInfo> fieldComparator = new  Comparator<FieldInfo>() {
        @Override
        public int compare(FieldInfo arg0, FieldInfo arg1) {
            String name0 = arg0.name;
            String name1 = arg1.name;
            if (name0.charAt(0)=='_') {
                if (name1.charAt(0)!='_') return 1;
            } else {
                if (name1.charAt(0)=='_') return -1;
            }
            return name0.compareTo(name1);
        }
    };



    private void loadIndexedFields(XContentBuilder json, Document d, LeafReader leafRdr, DocumentFieldMappers fieldMappers) throws Exception {
        if (!selectedOutput.isSelected("indexed") && !selectedOutput.isSelected ("docvalues")) return;

        json.startArray("indexedFields");

        FieldInfo[] fieldInfos = getFields(leafRdr);
        final IndexService indexService = indicesService.indexServiceSafe(indexShard.shardId().getIndex());
        final QueryShardContext queryShardContext = indexService.newQueryShardContext(indexShard.shardId().getId(), leafRdr, () -> 0L, null);

        boolean inField=false;
        for (FieldInfo field: fieldInfos) {
            if (field==null || !selectedFields.isSelected (field.name.toLowerCase())) continue;

            if (inField) {
                json.endObject();
            }
            inField = true;
            MappedFieldType fieldType = indexShard.mapperService().fullName(field.name);
            TypeHandler typeHandler = TypeHandler.create(fieldType, field.name);

            json.startObject();
            json.field("name", field.name);
            json.field("class", Utils.getClass(field));
            json.field("mappedType", fieldType==null ? null : fieldType.typeName());
            json.field("mappedClass", Utils.getClass(fieldType));
            json.field("type", Utils.toString(fieldType));
            json.field("dv-type", field.getDocValuesType().toString());

            System.out.printf("Handling field2 %s\n", field);
            //Dump index terms if requested
            if (selectedOutput.isSelected ("indexed")) {
                json.startArray("terms");
                Terms terms = leafRdr.terms(field.name);
                if (terms != null)
                    extractTerms (json, terms, field, fieldType, typeHandler);
                else {
                    System.out.printf("points for %s: dim=%d, num=%d\n", field.name, field.getPointDataDimensionCount(), field.getPointNumBytes());

                    if (field.getPointDataDimensionCount() > 0 && field.getPointNumBytes() >= 0) //PW7 is ook index variant
                        extractPointTerms(json, field, fieldType, typeHandler);
                }
                json.endArray();
            }

            if (selectedOutput.isSelected ("docvalues")) {
                json.startObject("docvalues");
                extractDocValues (json, queryShardContext, field, fieldType, typeHandler);
                json.endObject();
            }
        }
        if (inField) {
            json.endObject();
        }
        json.endArray();

    }

    private void extractDocValues(XContentBuilder json, QueryShardContext queryShardContext, FieldInfo field, MappedFieldType fieldType, TypeHandler typeHandler) throws IOException {
        IndexFieldData<?> fd = null;
        try {
            if (fieldType!=null) fd = queryShardContext.getForField(fieldType);
        } catch(Throwable th) {
            String x = th.toString();
            if (th instanceof IllegalArgumentException) {
                if (x.indexOf("not supported") > 0)
                    x = "Not supported";
            }
            json.field ("error", x);
        }
        if (fd == null) {
            DocValuesType dvt = field.getDocValuesType();
            switch (dvt) {
                default:  return;
                case NUMERIC:
                case SORTED_NUMERIC:
                    if (typeHandler.numericType != null)
                       fd = new SortedNumericDVIndexFieldData(queryShardContext.index(), field.name, typeHandler.numericType);
                    break;
            }
        }

        AtomicFieldData dv = fd==null ? null : fd.load(leafCtx);
        if (this.outputLevel > 0) {
            json.field("ft_class", Utils.getClass(fieldType));
            json.field("fd_class", Utils.getClass(fd));
            json.field("dv_class", Utils.getClass(dv));
            json.field("th_class", Utils.getClass(typeHandler));
        }
        if (dv != null) {
            Object[] values =  typeHandler.docValuesToObjects(dv, docId);
            int N = values==null ? 0 : values.length;
            json.field("count", N);
            if (N > 0) {
                if (outputLevel > 0) {
                    Object[] binValues = BytesHandler.instance.docValuesToObjects(dv,  docId);
                    Object[] newValues = new Object[2*N];
                    for (int i=0; i<N; i++) {
                        newValues[2*i+0] = values[i];
                        newValues[2*i+1] = "BIN: " + binValues[i];
                    }
                    values = newValues;
                }
                json.array("values", values);
            }
        }
    }

    private void extractPointTerms(XContentBuilder json, FieldInfo fieldInfo, MappedFieldType fieldType, TypeHandler typeHandler) throws IOException {
        PointValues values = leafRdr.getPointValues(fieldInfo.name);
        System.out.printf("points for %s: %s\n", fieldInfo.name, values);
        if (values == null) return;
        try {
           values.intersect(new PointsVisitor(json, typeHandler, this.docId, this.outputLevel));
        } catch (IllegalArgumentException x) {  //Expected: bug in Lucene
        }
    }

    private void extractTerms(XContentBuilder json, Terms terms, FieldInfo fieldInfo, MappedFieldType fieldType, TypeHandler typeHandler) throws IOException {
        TermsEnum te = terms.iterator();
        int indexOptions = fieldInfo.getIndexOptions().ordinal();
        if (!postings || indexOptions < IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
            while (true) {
                BytesRef term = te.next();
                if (term == null) break;

                PostingsEnum dpe = leafRdr.postings(new Term(fieldInfo.name, term));
                if (dpe == null)
                    continue;

                // Try find the doc...
                if (docId != dpe.advance(docId))
                    continue;

                byte[] bytes =  Utils.getBytes(term, null);
                typeHandler.export(json, bytes);
                if (outputLevel>0) json.value ("BIN: "  + BytesHandler.instance.toString (bytes));
            }
            return;
        }
        
        //At this point we know we have postings
        int what = PostingsEnum.FREQS | PostingsEnum.POSITIONS;
        boolean offsets = false;
        if (indexOptions > IndexOptions.DOCS_AND_FREQS_AND_POSITIONS.ordinal()) {
            what |= PostingsEnum.OFFSETS;
            offsets = true;
        }
        List<_Term> termList = new ArrayList<_Term>();

        while (true) {
            BytesRef term = te.next();
            if (term == null)
                break;

            PostingsEnum dpe = leafRdr.postings(new Term(fieldInfo.name, term), what);
            if (dpe == null)
                continue;

            // Try find the doc...
            if (docId != dpe.advance(docId))
                continue;

            termList.add(new _Term (Utils.getBytes(term, null), dpe));
        }
        
        //Sort the terms on position and term
        _Term.sort(termList);
        
        //Export them, reusing the buffer for dens processing
        StringBuilder sb = dense ? new StringBuilder() : null;
        for (_Term t: termList) {
            t.export(json, typeHandler, outputLevel>0, offsets, sb);
        }
    }

    
    /**
     * Class to hold a term with its postings. 
     * Used to be able to sort on 
     * - first occurrence position
     * - term itself
     */
    static class _Term {
        byte[] termBytes;
        List<_Posting> postings;
        int firstPos;
        
        public _Term (byte[] bytes, PostingsEnum dpe) throws IOException {
            termBytes = bytes;
            int count = dpe.freq();
            postings = new ArrayList<_Posting>(count);
            for (int i=0; i<count; i++) {
                postings.add(new _Posting(dpe));
            }
        }
        
        public void export(XContentBuilder json, TypeHandler typeHandler, boolean bin, boolean offsets, StringBuilder denseBuilder) throws IOException {
            json.startObject();
            json.startObject(typeHandler.toString(termBytes));
            if (bin) json.field("bin", BytesHandler.instance.toString (termBytes));
            int N = postings.size();
            json.field("count", N);
            
            if (N>0) {
                json.startArray("postings");
                for (int i=0; i<N; i++) {
                    postings.get(i).export(json, offsets, denseBuilder);
                }
                json.endArray();
            }
            json.endObject();
            json.endObject();
        }
        
        public static void sort(List<_Term> list) {
            for (int i=0; i<list.size(); i++) {
                _Term t = list.get(i);
                List<_Posting> plist = t.postings;
                switch (plist.size()) {
                    case 0: break;
                    case 1: t.firstPos = plist.get(0).pos; break;
                    default:
                        plist.sort(_Posting.cbPosOffset);
                        t.firstPos = plist.get(0).pos; break;
                }
            }
            list.sort(cbPosTerm);
        }
        
        static final Comparator<_Term> cbPosTerm = new Comparator<_Term>() {
            @Override
            public int compare(_Term o1, _Term o2) {
                if (o1.firstPos < o2.firstPos) return -1;
                if (o1.firstPos > o2.firstPos) return 1;
                byte[] b1 = o1.termBytes;
                byte[] b2 = o2.termBytes;
                int N = Math.min(b1.length, b2.length);
                for (int i=0; i<N; i++) {
                    int rc = (b1[i] & 255) - (b1[i] & 255);
                    if (rc != 0) return rc;
                }
                return b1.length - b2.length;
            }
        };
    }
    
    /**
     * Class to hold a posting for a term. 
     * Postings will be sorted on 
     * - pos
     * - endoffset
     */
    static class _Posting {
        int pos;
        int startOffset;
        int endOffset;
        public _Posting(PostingsEnum dpe) throws IOException {
            pos = dpe.nextPosition();
            startOffset = dpe.startOffset();
            endOffset = dpe.endOffset();
        }
        
        public void export (XContentBuilder json, boolean offsets, StringBuilder denseBuilder) throws IOException {
            if (denseBuilder != null) {
                denseBuilder.setLength(0);
                denseBuilder.append("pos=").append(pos);
                if (offsets) {
                    denseBuilder.append(", offset=").append(startOffset).append("..").append(endOffset);
                }
                json.value(denseBuilder.toString());
            } else {
                json.startObject();
                json.field("pos", pos);
                if (offsets) {
                    json.field("start", startOffset);
                    json.field("end", endOffset);
                }
                json.endObject();
            }
        }
        
        public static final Comparator<_Posting> cbPosOffset = new Comparator<_Posting>() {
            @Override
            public int compare(_Posting o1, _Posting o2) {
                if (o1.pos < o2.pos) return -1;
                if (o1.pos > o2.pos) return 1;
                if (o1.endOffset < o2.endOffset) return 1;
                if (o1.endOffset > o2.endOffset) return -1;
                return 0;
            }
        };
    }

    /**
     * This class enumerates all values in the BK_Tree and stores them in the
     * termlist
     */
    static class PointsVisitor implements IntersectVisitor {
        XContentBuilder json;
        TypeHandler typeHandler;
        int argDocid;
        int outputLevel;

        public PointsVisitor(XContentBuilder json, TypeHandler typeHandler, int docid, int outputLevel) {
            this.json = json;
            this.typeHandler = typeHandler;
            this.argDocid = docid;
            this.outputLevel = outputLevel;
        }

        @Override
        public void visit(int docID) throws IOException {
        }

        @Override
        public void visit(int docID, byte[] bytes) throws IOException {
            //System.out.printf("visit %s (arg=%s)\n", docID, argDocid);
            if (docID==argDocid) {
                json.startObject();
                typeHandler.export(json, "value", bytes);
                if (outputLevel > 0) BytesHandler.instance.export(json, "raw", bytes);
                json.endObject();
            }
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_CROSSES_QUERY; // Needed to get the correct
                                                // visit() called!
        }

    }


}
