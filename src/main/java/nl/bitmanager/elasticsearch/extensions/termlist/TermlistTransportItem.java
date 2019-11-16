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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.rest.RestRequest;

import nl.bitmanager.elasticsearch.support.BytesRange;
import nl.bitmanager.elasticsearch.support.IntRange;
import nl.bitmanager.elasticsearch.support.RegexReplacers;
import nl.bitmanager.elasticsearch.transport.ActionDefinition;
import nl.bitmanager.elasticsearch.transport.TransportItemBase;
import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class TermlistTransportItem extends TransportItemBase {
    private static final String P_FIELD = "field";
    private static final String P_FILTER = "filter";
    private static final String P_NOT_FILTER = "not_filter";
    private static final String P_SORT = "sort";
    private static final String P_COUNT = "count";
    private static final String P_LENGTH = "length";
    private static final String P_RESULT_LIMIT = "result_limit";
    private static final String P_REPL_EXPR = "repl_expr";
    private static final String P_COLLISIONS_ONLY = "collisions_only";
    private static final String P_RANGE = "range";

    private HashSet<String> fieldsMap;
    private RegexReplacers replacers;

    private String fields, filterExpr, notFilterExpr, outputType, term;
    private String replExpr;
    private String range;
    private TermList termlist;
    private String count_range, length_range;;
    private int resultLimit;
    private int mode;
    private SortType sortType;
    private boolean requestRawText;
    private boolean collisionsOnly;

    public TermlistTransportItem(ActionDefinition definition) {
        super(definition);
        sortType = new SortType();
        termlist = new TermList(sortType);
    }

    public TermlistTransportItem(ActionDefinition definition, RestRequest request) {
        super(definition);
        fields = nullIfEmpty(request.param(P_FIELD));
        term = request.param("term");
        range = nullIfEmpty(request.param(P_RANGE));
        filterExpr = nullIfEmpty(request.param(P_FILTER));
        notFilterExpr = nullIfEmpty(request.param(P_NOT_FILTER));
        replExpr = nullIfEmpty(request.param(P_REPL_EXPR));
        outputType = request.param("output");
        requestRawText = "text".equalsIgnoreCase(outputType);
        mode = request.paramAsInt("mode", 0);

        count_range = nullIfEmpty(request.param(P_COUNT));
        length_range = nullIfEmpty(request.param(P_LENGTH));
        resultLimit = request.paramAsInt(P_RESULT_LIMIT, 20000);
        collisionsOnly = request.paramAsBoolean(P_COLLISIONS_ONLY, true);

        this.sortType = new SortType (request.param(P_SORT));
        termlist = new TermList (sortType);

        //Check the format
        new IntRange (count_range);
        new IntRange (length_range);

        initCachedObjects();
    }

    public TermlistTransportItem(TermlistTransportItem other) {
        super(other.definition);
        fields = other.fields;
        term = other.term;
        filterExpr = other.filterExpr;
        notFilterExpr = other.notFilterExpr;
        replExpr = other.replExpr;
        outputType = other.outputType;
        requestRawText = other.requestRawText;
        mode = other.mode;

        count_range = other.count_range;
        length_range = other.length_range;
        resultLimit = other.resultLimit;
        collisionsOnly = other.collisionsOnly;
        sortType = other.sortType;
        termlist = new TermList (sortType);
        range = other.range;
        initCachedObjects();
    }

    public TermlistTransportItem(ActionDefinition definition, StreamInput in) throws IOException {
        super(definition, in);
        fields = readStr(in);
        range = readStr(in);
        filterExpr = readStr(in);
        notFilterExpr = readStr(in);
        outputType = readStr(in);
        sortType = new SortType(in.readVInt());
        term = readStr(in);
        mode = in.readVInt();
        resultLimit = in.readVInt();
        count_range = readStr(in);
        length_range = readStr(in);
        replExpr = readStr(in);
        collisionsOnly = in.readBoolean();
        termlist = new TermList (sortType);
        termlist.loadFromStream(in);
        initCachedObjects();
    }

    public void writeTo(StreamOutput out) throws IOException {
        System.out.println(String.format("SHARDREQ: strm=%s, field=%s", out, fields));
        super.writeTo (out);
        writeStr(out, fields);
        writeStr(out, range);
        writeStr(out, filterExpr);
        writeStr(out, notFilterExpr);
        writeStr(out, outputType);
        out.writeVInt(sortType.order);
        writeStr(out, term);
        out.writeVInt(mode);
        out.writeVInt(resultLimit);
        writeStr(out, count_range);
        writeStr(out, length_range);
        writeStr(out, replExpr);
        out.writeBoolean(collisionsOnly);
        termlist.saveToStream(out);
    }


    public boolean isTextRequest() {
        return requestRawText;
    }

    public boolean isFieldRequested(String fld) {
        if (fieldsMap == null || fld == null)
            return true;
        return fieldsMap.contains(fld.toLowerCase());
    }

    public TermList getTermlist() {
        return termlist;
    }

    private void initCachedObjects() {
        fieldsMap = null;
        replacers = null;
        if (fields != null) {
            HashSet<String> map = new HashSet<String>();
            String[] arr = fields.toLowerCase().split(",");
            for (int i = 0; i < arr.length; i++) {
                map.add(arr[i]);
            }
            fieldsMap = map;
        }

        // System.out.println("Init replacers, expr=" + replExpr);
        if (replExpr != null) {
            replacers = new RegexReplacers(replExpr);
            if (!replacers.hasReplacements())
                replacers = null;
            // System.out.println("Init replacers, result=" + replacers);
        }
    }

    public IntRange getCountRange() {
        return count_range==null ? null : new IntRange(count_range);
    }

    public int getMode() {
        return mode;
    }

    public int getResultLimit() {
        return resultLimit;
    }

    public String getOutputType() {
        return outputType;
    }

    public String getReplExpr() {
        return replExpr;
    }

    public RegexReplacers getReplacers() {
        return replacers;
    }

    public boolean collisionsRequested() {
        return replacers != null;
    }

    public String getFilterExpr() {
        return filterExpr;
    }

    public String getNotFilterExpr() {
        return notFilterExpr;
    }

    public SortType getSortType() {
        return sortType;
    }

    public Boolean getCollisionsOnly() {
        return collisionsOnly;
    }

    public Pattern createPattern() {
        if (filterExpr == null)
            return null;
        return Pattern.compile(filterExpr);
    }

    public Pattern createNotPattern() {
        if (notFilterExpr == null)
            return null;
        return Pattern.compile(notFilterExpr);
    }


    @Override
    protected void consolidateResponse(TransportItemBase other) {
        termlist.combine(((TermlistTransportItem) other).termlist);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        System.out.println("Got overall response: " + this.termlist);

        builder.startObject("request");
        exportToJson(builder);
        builder.endObject();

        TermList.TermListLimits limits = termlist.getLimits(this);
        builder.startObject("response");
        limits.exportToJson(builder);

        builder.startObject("fields");
        TypeHandler th = TypeHandler.create(termlist.getType());
        termlist.exportFieldsToJson(builder);
        builder.endObject();

        if (replacers != null) {
            Collisions collisions = termlist.buildCollisions(this);
            builder.field("collisionCount", collisions.size());
            builder.startArray("collisions");
            collisions.exportToJson(builder, this, th);
            builder.endArray();
        } else {
            builder.startArray(term == null ? "terms" : "fields");
            termlist.exportToJson(builder, this, limits, th);
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    public void exportToJson(XContentBuilder builder) throws IOException {
        builder.field(P_FIELD, fields);
        builder.field(P_RANGE, range);
        builder.field(P_FILTER, filterExpr);
        builder.field(P_NOT_FILTER, notFilterExpr);
        builder.field(P_COUNT, count_range==null ? "" : count_range.toString());
        builder.field(P_LENGTH, length_range==null ? "" : length_range.toString());
        builder.field(P_RESULT_LIMIT, resultLimit);
        builder.field(P_REPL_EXPR, replExpr);
        builder.field(P_COLLISIONS_ONLY, collisionsOnly);
        builder.field(P_SORT, sortType.toString());
    }

    public void processShard (IndexShard indexShard) throws Exception {
        Searcher searcher = indexShard.acquireSearcher("termlist");
        searcher.getIndexReader().getContext().leaves();
        try {
            for (LeafReaderContext c : searcher.getIndexReader().getContext().leaves()) {
                LeafReader rdr = c.reader();
                System.out.println("getTerm=" + term);
                if (term == null)
                    extractTerms(rdr, indexShard);
                else
                    extractFields(rdr, indexShard);
            }
        } finally {
            searcher.close();
        }
    }

    private void extractTerms(LeafReader rdr, IndexShard indexShard) throws Exception {

        FieldInfos fieldInfos = rdr.getFieldInfos();
        //Fields luceneFields = MultiFields.getFields(rdr);
        if (fieldInfos == null || fieldInfos.size() == 0)
            return;

        Pattern pattern = createPattern();
        Pattern notPattern = createNotPattern();
        boolean fieldsSpecified = fields != null;

        for (Iterator<FieldInfo> it = fieldInfos.iterator(); it.hasNext();) {
            FieldInfo fieldInfo = it.next();
            if (isFieldRequested(fieldInfo.name)) {
                // SourceFieldType xx=null;
                MappedFieldType mft = indexShard.mapperService().fullName(fieldInfo.name);
                if (mft==null) {;
                   System.out.printf ("ERROR: field %s has no es-fieldmapping\n", fieldInfo.name);
                   continue;
                }
                System.out.printf("MappedFieldType for %s : %s\n", fieldInfo.name, mft==null ? "null": mft.getClass().getName());
                TypeHandler typeHandler = TypeHandler.create(mft, fieldInfo.name);
                termlist.setType(typeHandler.typeName);
                Terms terms = rdr.terms(fieldInfo.name);
                ShardFieldStats stats = terms != null ? new ShardFieldStats(terms) : extractPointStats (rdr, fieldInfo.name);
                //FieldData fdt;
//                org.elasticsearch.index.mapper.TextFieldMapper.TextFieldType

                termlist.addField(new FieldInfoItem(indexShard.shardId().getIndexName(), fieldInfo, mft, stats));
                if (!fieldsSpecified)
                    continue;

                BytesRange range = this.range == null ? null : new BytesRange (this.range, typeHandler);
                if (terms == null) {
                    if (fieldInfo.getPointDataDimensionCount() > 0 && fieldInfo.getPointNumBytes() >= 0) //PW7 kijk ook voor index variant
                        extractPointTerms(termlist, range, rdr, fieldInfo.name);
                    continue;
                }

                TermsEnum termsEnum = terms.iterator();
                BytesRef text;

                IntRange lengthRange = new IntRange(length_range);
                boolean needText = lengthRange != null || notPattern != null || pattern != null;
                while ((text = termsEnum.next()) != null) {
                    byte[] bytes = text.bytes;
                    boolean owned = false;
                    if (text.offset != 0 || text.length != bytes.length) {
                        owned = true;
                        bytes = Arrays.copyOfRange(bytes, text.offset, text.length);
                    }

                    if (range != null && !range.isInRange(bytes))
                        continue;

                    if (needText) {
                        String term = text.utf8ToString();
                        if (lengthRange != null && !lengthRange.isInRange (term.length())) continue;

                        if (notPattern != null && notPattern.matcher(term).find())
                            continue;
                        if (pattern != null && !pattern.matcher(term).find())
                            continue;
                    }
                    System.out.printf("ADD %s: %d\n", term, termsEnum.docFreq());
                    termlist.add(bytes, owned, termsEnum.docFreq());
                }
                System.out.printf("ADDED %d terms\n", termlist.size());
            }
        }
    }

    private void extractFields(LeafReader rdr, IndexShard indexShard) throws IOException {
        FieldInfos fields = rdr.getFieldInfos();
        if (fields == null || fields.size()==0)
            return;

        String strTerm = this.term;
        TypeHandler typeHandler = TypeHandler.create("text");
        for (FieldInfo field : fields) {
            String name = field.name;
            if (name.charAt(0) == '_')
                continue;

            Term term = new Term(name, strTerm);
            int docFreq = rdr.docFreq(term);
            if (docFreq > 0)
                termlist.add(typeHandler.toBytes(name), true, docFreq);
        }
    }

    private void extractPointTerms(TermList termlist, BytesRange range, IndexReader rdr, String field) throws IOException {
        IndexReaderContext ctx = rdr.getContext();
        List<LeafReaderContext> leaves = ctx.leaves();
        for (LeafReaderContext leave : leaves) {
            LeafReader leaveRdr = leave.reader();
            PointValues values = leaveRdr.getPointValues(field);
            if (values == null)
                continue;
            try {
               values.intersect(new PointsVisitor(termlist, range));
            } catch (IllegalArgumentException x) {  //Expected: bug in Lucene
            }
        }
    }
    private ShardFieldStats extractPointStats(IndexReader rdr, String field) throws IOException {
        IndexReaderContext ctx = rdr.getContext();
        List<LeafReaderContext> leaves = ctx.leaves();
        ShardFieldStats stats = null;
        for (LeafReaderContext leave : leaves) {
            LeafReader leafRdr = leave.reader();
            PointValues values = leafRdr.getPointValues(field);
            if (values == null)
                continue;
            try {
                ShardFieldStats tmp = new ShardFieldStats(values);
                if (stats==null) stats = tmp;
                else stats.combine(tmp);
            } catch (IllegalArgumentException x) {  //Expected: bug in Lucene
            }
        }
        return stats;
    }

    /**
     * This class enumerates all values in the BK_Tree and stores them in the
     * termlist
     */
    static class PointsVisitor implements IntersectVisitor {
        BytesRange range;
        TermList termlist;

        public PointsVisitor(TermList termlist, BytesRange range) {
            this.termlist = termlist;
            this.range = range;
        }

        @Override
        public void visit(int docID) throws IOException {
        }

        @Override
        public void visit(int docID, byte[] bytes) throws IOException {
            if (range == null || range.isInRange(bytes))
                termlist.add(bytes, false, 1);
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_CROSSES_QUERY; // Needed to get the correct
                                                // visit() called!
        }

    }

}
