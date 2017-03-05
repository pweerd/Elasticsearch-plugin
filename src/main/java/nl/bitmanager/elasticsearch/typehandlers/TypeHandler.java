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

package nl.bitmanager.elasticsearch.typehandlers;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;

public abstract class TypeHandler {
    public final String typeName;
    public final boolean knowType;

    public abstract XContentBuilder export(XContentBuilder builder, byte[] bytes) throws IOException;

    public XContentBuilder export(XContentBuilder builder, String field, byte[] bytes) throws IOException {
        return export (builder.field(field), bytes);
    }
    
    public abstract XContentBuilder exportDocValues(XContentBuilder builder, AtomicFieldData fieldData, int docid) throws IOException;
    public XContentBuilder export(XContentBuilder builder, String field, AtomicFieldData fieldData, int docid) throws IOException {
        return exportDocValues (builder.field(field), fieldData, docid);
    }


    public abstract byte[] toBytes(String s);

    public abstract String toString(byte[] b);

    protected TypeHandler(String type) {
        this.typeName = type;
        this.knowType = true;
    }

    protected TypeHandler(String type, boolean known) {
        this.typeName = type;
        this.knowType = known;
    }

    public static TypeHandler create(MappedFieldType mft) {
        return create(mft == null ? null : mft.typeName());
    }

    public static TypeHandler create(String type) {
        if ("text".equals(type) || 
                "keyword".equals(type) || 
                "text_with_docvalues".equals(type) ||
                "analyzed_keyword".equals(type) ||
                "string".equals(type) || 
                "_all".equals(type) || 
                "_parent".equals(type) || 
                "_routing".equals(type) || 
                "_type".equals(type) || 
                "_uid".equals(type) || 
                "_field_names".equals(type))
            return new StringHandler(type);
        if ("bool".equals(type) || "boolean".equals(type))
            return new BoolHandler(type);
        if ("long".equals(type))
            return new Int64Handler(type);
        if ("integer".equals(type) || "short".equals(type) || "byte".equals(type))
            return new Int32Handler(type);
        
        if ("float".equals(type))
            return new Float32Handler(type);
        if ("double".equals(type))
            return new Float64Handler(type);

        if ("geo_point".equals(type))
            return new GeoPointHandler(type);

        if ("date".equals(type))
            return new DateHandler(type);
        System.out.printf("Unknown type [%s]. Using ByteHandler.\n", type);
        return new BytesHandler(type, false);
    }


}
