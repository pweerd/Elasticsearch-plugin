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

public abstract class TypeHandlerBase extends TypeHandler {

    abstract Object[] _bytesToObjects(byte[] bytes);
    abstract Object[] _docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException ;


    @Override
    public XContentBuilder export(XContentBuilder builder, byte[] bytes) throws IOException {
        Object[] values = _bytesToObjects(bytes);
        return (values.length==1) ? builder.value(values[0]) : builder.value(values);
    }
    
    
    @Override
    public Object[] docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException {
        return _docValuesToObjects (fieldData, docid);
    }

    
    protected TypeHandlerBase(String type) {
        super(type);
    }

    protected TypeHandlerBase(String type, boolean known) {
        super(type, known);
    }

    @Override
    public String toString(byte[] bytes) {
        if (bytes==null) return null;
        Object[] values = _bytesToObjects(bytes);
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<values.length; i++) {
            if (i>0) sb.append(", ");
            sb.append(values[i]);
        }
        return sb.toString();
    }
    

}
