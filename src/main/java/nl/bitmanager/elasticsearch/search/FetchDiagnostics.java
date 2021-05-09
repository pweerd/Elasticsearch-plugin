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

package nl.bitmanager.elasticsearch.search;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

/**
 * This class optional fetches extra diagnostic information like segment-level info and docvalues.
 * It is controlled via the SearchParms (supplied via ext: {})
 */
public class FetchDiagnostics implements FetchSubPhase {
    private final static boolean DEBUG=false;

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        try {
            if (DEBUG) System.out.println("HIT execute1");

            SearchParms x = (SearchParms)context.getSearchExt("_bm");
            if (x==null || !x.diagnostics) return;
            if (context.storedFieldsContext() != null && context.storedFieldsContext().fetchFields() == false) {
                return ;
            }
            SearchHit hit = hitContext.hit();
            Map<String, DocumentField> fields = hitContext.hit().fieldsOrNull();
            if (fields == null) {
                fields = new HashMap<>();
                hit.fields(fields);
            }
            LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
            LeafReaderContext ctx = hitContext.readerContext();
            int docid = hit.docId();
            int rel_docid = docid - ctx.docBase;
            map.put("shard",  context.getQueryShardContext().getShardId());
            map.put("segment",  ctx.ord);
            map.put("docid",  docid);
            map.put("docid_rel",  rel_docid);
            map.put("index_uuid",  context.getQueryShardContext().index().getUUID());
            Query query = context.query();
            map.put("query", query==null ? null : query.toString());
            fields.put("_bm", new DocumentField("_bm", Collections.singletonList(map)));

            DocumentMapper docMapper = context.mapperService().documentMapper(hitContext.hit().getType());
            DocumentFieldMappers mappers = docMapper.mappers();

            QueryShardContext shardCtx = context.getQueryShardContext();
            LinkedHashMap<String, Object> dst = new LinkedHashMap<String, Object>();
            for (Mapper mapper: mappers) {
                if (!(mapper instanceof FieldMapper)) continue;
                FieldMapper f = (FieldMapper)mapper;

                if (DEBUG) System.out.println("HIT execute2 F=" + f.name() + ", dv=" + f.fieldType().hasDocValues());
                if (f.fieldType().hasDocValues())
                    extractDocValues  (dst, f, shardCtx, hitContext);
            }
            if (dst.size()>0) map.put("docvalues", dst);
        } catch (Exception e) {
            throw new RuntimeException (e.getMessage(), e);
        }


    }

    private void extractDocValues(Map<String, Object> dst, FieldMapper field, QueryShardContext shardCtx, HitContext hitContext) throws IOException {
        IndexFieldData<?> fd;
        MappedFieldType fieldType = field.fieldType();
        try {
            fd = shardCtx.getForField(fieldType);
        } catch(Throwable th) {
            String x = th.toString();
            if (th instanceof IllegalArgumentException) {
                if (x.indexOf("not supported") > 0) return;
            }
            dst.put(field.name(), "error: " + x);
            return;
        }

        AtomicFieldData dv = fd.load(hitContext.readerContext());
        TypeHandler typeHandler = TypeHandler.create(fieldType, field.name());
        if (dv != null) {
            int reldoc = hitContext.docId();
            if (reldoc < 0) {
                dst.put(field.name(), String.format("negative reldoc=%d docid=%d, base=%d", reldoc, hitContext.docId() , hitContext.readerContext().docBase));
                return;
            }
            if (DEBUG) System.out.println("reldoc=" + reldoc);
            Object[] values = typeHandler.docValuesToObjects (dv, reldoc);
            if (values != null && values.length > 0)
                dst.put(field.name(), values);
        }
    }


}
