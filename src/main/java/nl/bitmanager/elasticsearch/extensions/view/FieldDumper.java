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
//import org.apache.lucene.index.FieldInfo;
//import org.apache.lucene.index.LeafReader;
//import org.apache.lucene.util.BytesRef;
//import org.apache.lucene.util.LegacyNumericUtils;
//import org.apache.lucene.util.NumericUtils;
//import org.elasticsearch.common.xcontent.XContentBuilder;
//import org.elasticsearch.index.fielddata.IndexFieldDataService;
//import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
//import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
//import org.elasticsearch.index.mapper.MappedFieldType;
//import org.joda.time.DateTime;
//
//@SuppressWarnings("rawtypes")
//public class FieldDumper {
//    public DocValuesDumper docValuesDumper;
//    protected IndexFieldDataService fieldDataService;
//    protected FieldInfo fieldInfo;
//    protected MappedFieldType fieldType;
//    protected String fieldDataType;
//    protected String dumper;
//    public int dumpLevel;
//    protected FieldDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//        this.fieldDataService = fieldDataService;
//        this.fieldInfo = fieldInfo;
//        this.dumper = getClass().getSimpleName();
//        this.fieldType = fieldType;
//        this.fieldDataType = mapperToFieldDataType(fieldType);
//        dumpLevel = outputLevel;
//    }
//    
//    public void dumpToJson (XContentBuilder json) throws IOException {
//        if (dumpLevel > 0) json.field ("dumper", dumper);
//        json.field("type", fieldType);
//        json.field("fieldDataType", fieldDataType);
//        json.field("numericType", fieldType.numericType());
//        if (fieldType.numericType()!=null)
//            json.field("precisionSteps", fieldType.numericPrecisionStep());
//    }
//    public void dumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//        baseDumpToJson(json, term);
//    }
//    protected void baseDumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//        if (dumpLevel > 0) json.field("raw", term.toString());
//    }
//    
//    public DocValuesDumper createDocValuesDumper (LeafReader rdr, int docid) throws IOException {
//        return new DocValuesDumper (fieldDataService, fieldType, fieldInfo, rdr, docid, null);
//    }
//
//    private static String mapperToFieldDataType(MappedFieldType fieldType) {
//        return fieldType==null ? "unknown" : fieldType.typeName();
//    }
//    
//    public static FieldDumper create (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//        String dataType = mapperToFieldDataType(fieldType);
//        if ("string".equals(dataType) || 
//                "_all".equals(dataType) || 
//                "_type".equals(dataType) || 
//                "_field_names".equals(dataType) || 
//                "_uid".equals(dataType)) 
//            return new StringFieldDumper(fieldDataService, fieldType, fieldInfo, outputLevel);
//
//        if ("long".equals(dataType)) {
//            if (fieldType instanceof DateFieldType) return new DateFieldDumper (fieldDataService, fieldType, fieldInfo, outputLevel);
//            return new LongFieldDumper(fieldDataService, fieldType, fieldInfo, outputLevel);
//        }
//        if ("int".equals(dataType)) return new IntFieldDumper(fieldDataService, fieldType, fieldInfo, outputLevel);
//        
//        
//        return new RawFieldDumper (fieldDataService, fieldType, fieldInfo, outputLevel);
//    }
//    
//    
//    public static class RawFieldDumper extends FieldDumper {
//        public  RawFieldDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//            super (fieldDataService, fieldType, fieldInfo, outputLevel);
//            
//        }
//        
//        public void dumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//            super.dumpToJson(json, term);
//        }
//        
//    }
//    
//    public static class StringFieldDumper extends FieldDumper {
//        public  StringFieldDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//            super (fieldDataService, fieldType, fieldInfo, outputLevel);
//            
//        }
//        
//        public void dumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//            super.dumpToJson(json, term);
//            json.field("value", term.utf8ToString());
//        }
//        
//    }
//    
//    public static class LongFieldDumper extends FieldDumper {
//        public  LongFieldDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//            super (fieldDataService, fieldType, fieldInfo, outputLevel);
//        }
//        
//        public DocValuesDumper createDocValuesDumper (LeafReader rdr, int docid) throws IOException {
//            return new DocValuesDumper.LongBinDocValuesDumper (fieldDataService, fieldType, fieldInfo, rdr, docid, NumericType.LONG);
//        }
//
//        
//        public void dumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//            baseDumpToJson(json, term);
//            
//            try { 
//                long val = LegacyNumericUtils.prefixCodedToLong(term);
//                if (dumpLevel > 0) json.field("hex", Long.toHexString(val));
//                json.field("value", val);
//            } catch (Throwable t) {
//                json.field("err", t.getMessage());
//                t.printStackTrace();
//            }
//        }
//    }
//
//    public static class DateFieldDumper extends FieldDumper {
//        public  DateFieldDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//            super (fieldDataService, fieldType, fieldInfo, outputLevel);
//        }
//        
//        public DocValuesDumper createDocValuesDumper (LeafReader rdr, int docid) throws IOException {
//            return new DocValuesDumper.DateBinDocValuesDumper (fieldDataService, fieldType, fieldInfo, rdr, docid);
//        }
//
//        public void dumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//            super.dumpToJson(json, term);
//            try { 
//                long val = LegacyNumericUtils.prefixCodedToLong(term);
//                DateTime dt = new DateTime (val);
//                if (dumpLevel > 0) json.field("long", val);
//                if (dumpLevel > 0) json.field("hex", Long.toHexString(val));
//                json.field("value", dt);
//            } catch (Throwable t) {
//                json.field("err", t.getMessage());
//                t.printStackTrace();
//            }
//        }
//    }
//
//    public static class IntFieldDumper extends LongFieldDumper {
//        public  IntFieldDumper (IndexFieldDataService fieldDataService, MappedFieldType fieldType, FieldInfo fieldInfo, int outputLevel) {
//            super (fieldDataService, fieldType, fieldInfo, outputLevel);
//        }
//        
//        public void dumpToJson (XContentBuilder json, BytesRef term) throws IOException {
//            baseDumpToJson(json, term);
//            try { 
//                int val = LegacyNumericUtils.prefixCodedToInt(term);
//                if (dumpLevel > 0) json.field("hex", Integer.toHexString(val));
//                json.field("value", val);
//            } catch (Throwable t) {
//                json.field("err", t.getMessage());
//                t.printStackTrace();
//            }
//        }
//    }
//}
