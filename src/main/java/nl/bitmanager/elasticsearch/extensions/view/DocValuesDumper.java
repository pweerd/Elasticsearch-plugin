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

//package nl.bitmanager.elasticsearch.extensions.view;
//
//import java.io.IOException;
//
//import org.apache.lucene.index.BinaryDocValues;
//import org.apache.lucene.index.FieldInfo;
//import org.apache.lucene.index.DocValuesType;
//import org.apache.lucene.index.LeafReader;
//import org.apache.lucene.index.NumericDocValues;
//import org.apache.lucene.index.SortedDocValues;
//import org.apache.lucene.index.SortedNumericDocValues;
//import org.apache.lucene.index.SortedSetDocValues;
//import org.apache.lucene.util.BytesRef;
//import org.elasticsearch.common.xcontent.XContentBuilder;
//import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
//import org.elasticsearch.index.fielddata.IndexFieldData;
//import org.elasticsearch.index.fielddata.IndexFieldDataService;
//import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
//import org.elasticsearch.index.mapper.FieldMapper;
//import org.elasticsearch.index.mapper.MappedFieldType;
////kijken naar FieldValueFactorFunction
//public class DocValuesDumper {
//    public BinaryDocValues binDocValues;
//    public NumericDocValues numDocValues = null;
//    public SortedDocValues sortedDocValues = null;
//    public SortedNumericDocValues sortedNumDocValues = null;
//    public SortedSetDocValues sortedSetDocValues = null;
//    
//    protected final NumericType numType;
//    protected final MappedFieldType fieldType;
//    protected final FieldInfo fieldInfo;
//    protected final DocValuesType docValuesType;
//    protected final String dumper;
//    protected final IndexFieldDataService fieldDataService;
//    protected IndexFieldData fieldData;
//    
//    protected BytesRef rawDocValue;
//    public long numDocValue;
//    
//    public DocValuesDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, LeafReader rdr, int docid, NumericType numType) throws IOException {
//        dumper = getClass().getSimpleName();
//        this.fieldType = fieldType;
//        this.numType = numType;
//        this.fieldInfo = fieldInfo;
//        this.fieldDataService = fieldDataService;
//        this.fieldData = null;//PW(fieldDataService == null || fieldType == null) ? null : fieldDataService.getForField(fieldType);
//        
//        DocValuesType dvt = null;
//        if (fieldType != null)
//            dvt = fieldType.docValuesType();
//        if (dvt==null && fieldInfo != null) dvt = fieldInfo.getDocValuesType();
//        docValuesType = dvt;
//        String field = fieldInfo.name;
//        if (docValuesType==null) return;
//        switch (docValuesType) {
//            default: break;
//            case BINARY: 
//                binDocValues = rdr.getBinaryDocValues(field);
//                rawDocValue = binDocValues.get(docid);
//                break;
//            case NUMERIC: 
//                numDocValues = rdr.getNumericDocValues(field);
//                numDocValue = numDocValues.get(docid);
//                break;
//            case SORTED: sortedDocValues = rdr.getSortedDocValues(field); break;
//            case SORTED_NUMERIC: sortedNumDocValues = rdr.getSortedNumericDocValues(field); break;
//            case SORTED_SET: sortedSetDocValues = rdr.getSortedSetDocValues(field); break;
//        }
//    }
//    
//    public void dumpToJson (XContentBuilder json) throws IOException {
//        _dumpToJson (json);
//        if (rawDocValue != null) {
//            json.field("raw", rawDocValue.toString());
//            return;
//        }
//    }
//    protected void _dumpToJson (XContentBuilder json) throws IOException {
//        json.field ("dumper", dumper);
//        json.field ("type", docValuesType);
//    }
// 
//    
//    
//    static class LongBinDocValuesDumper extends DocValuesDumper {
//        protected SortedNumericDocValues longValues;
//        protected int numValues;
//        public LongBinDocValuesDumper(IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, LeafReader rdr, int docid, NumericType numType) throws IOException {
//            super(fieldDataService, fieldType, fieldInfo, rdr, docid, numType);
//            if (docValuesType==null) return;
//
//            //org.elasticsearch.index.fielddata.plain.SortedNumericDVIndexFieldData.SortedNumericLongFieldData
//            AtomicNumericFieldData atom = (AtomicNumericFieldData)fieldData.load(rdr.getContext());
//            longValues = atom.getLongValues();
//            longValues.setDocument(docid);
//            numValues = longValues.count();
//        }
//        public void dumpToJson (XContentBuilder json) throws IOException {
//            super.dumpToJson(json);
//            if (docValuesType==null) return;
//            json.field("values_count", numValues);
//            json.startArray("values");
//            
//            for (int i=0; i<numValues; i++) {
//                dumpValueToJson (json, longValues.valueAt(i));
//            }
//
//            json.endArray();
//        }
//        protected void dumpValueToJson (XContentBuilder json, long value) throws IOException {
//            json.value(value);
//        }
//    }
//
//    static class DateBinDocValuesDumper extends LongBinDocValuesDumper {
//        public DateBinDocValuesDumper(IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, LeafReader rdr, int docid) throws IOException {
//            super(fieldDataService, fieldType, fieldInfo, rdr, docid, NumericType.LONG);
//        }
//        protected void dumpValueToJson (XContentBuilder json, long value) throws IOException {
//            json.value(Long.toHexString(value));
//            json.value(value);
//        }
//    }
//
//}
