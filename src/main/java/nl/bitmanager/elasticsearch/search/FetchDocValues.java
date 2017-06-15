package nl.bitmanager.elasticsearch.search;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import nl.bitmanager.elasticsearch.extensions.view.DocInverter;
import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class FetchDocValues implements FetchSubPhase {
    private final static boolean DEBUG=false;

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        try {
            if (DEBUG) System.out.println("HIT execute1");
            
            SearchParmDocValues x = (SearchParmDocValues)context.getSearchExt("_bm");
            if (x==null || !x.docvalues) return;
            if (context.storedFieldsContext() != null && context.storedFieldsContext().fetchFields() == false) {
                return ;
            }
            SearchHit hit = hitContext.hit();
            Map<String, SearchHitField> fields = hitContext.hit().fieldsOrNull();
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
            fields.put("_bm", new SearchHitField("_bm", Collections.singletonList(map)));
            
            DocumentMapper docMapper = context.mapperService().documentMapper(hitContext.hit().getType());
            DocumentFieldMappers mappers = docMapper.mappers();
            
            FieldInfo[] segmentFields = DocInverter.getFields(ctx.reader());
            IndexFieldDataService fds = context.fieldData();
            LinkedHashMap<String, Object> dst = new LinkedHashMap<String, Object>();
            for (FieldMapper f: mappers) {
                if (DEBUG) System.out.println("HIT execute2 F=" + f.name() + ", dv=" + f.fieldType().hasDocValues());
                if (!f.fieldType().hasDocValues()) continue;
                extractDocValues  (dst, f, fds, hitContext);
                
                
            }
            if (dst.size()>0) map.put("docvalues", dst);
            //.parentFieldMapper();
        } catch (Exception e) {
            throw new RuntimeException (e.getMessage(), e);
        }
        
        
    }
    
    private void extractDocValues(Map<String, Object> dst, FieldMapper field, IndexFieldDataService fieldDataService, HitContext hitContext) throws IOException {
        IndexFieldData<?> fd;
        MappedFieldType fieldType = field.fieldType();
        try {
            fd = fieldDataService.getForField(fieldType);
        } catch(Throwable th) {
            String x = th.toString();
            if (th instanceof IllegalArgumentException) {
                if (x.indexOf("not supported") > 0) return;
            }
            dst.put(field.name(), "error: " + x);
            return;
        }
        //SortedSetDVBytesAtomicFieldData
        //ParentChildIndexFieldData
        AtomicFieldData dv = fd.load(hitContext.readerContext());
        TypeHandler typeHandler = TypeHandler.create(fieldType);
        if (dv != null) {
            int reldoc = hitContext.docId();
            if (reldoc < 0) {
                dst.put(field.name(), String.format("neg reldoc=%d docid=%d, base=%d", reldoc, hitContext.docId() , hitContext.readerContext().docBase));
                return;
            }
            if (DEBUG) System.out.println("reldoc=" + reldoc);
            Object[] values = typeHandler.docValuesToObjects (dv, reldoc);
            if (values != null && values.length > 0)
                dst.put(field.name(), values);
        }
    }

    
}
