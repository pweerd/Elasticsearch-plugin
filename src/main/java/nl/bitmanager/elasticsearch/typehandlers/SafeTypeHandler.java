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
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;

public abstract class SafeTypeHandler extends TypeHandlerBase {

    @Override
    public XContentBuilder export(XContentBuilder builder, byte[] bytes) throws IOException {
        try {
            return super.export (builder, bytes);
        } catch (Throwable th) {
            th.printStackTrace();
            return BytesHandler.instance.export (builder, bytes);
        }
    }
    
    @Override
    public Object[] docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException {
        try {
            return _docValuesToObjects (fieldData, docid);
        } catch (Throwable th) {
            th.printStackTrace();
            return BytesHandler.instance.docValuesToObjects (fieldData, docid);
        }
    }

    @Override
    public String toString(byte[] b) {
        try {
            return super.toString(b);
        } catch (Throwable th) {
            return BytesHandler.instance.toString(b);
        }
    }

    protected SafeTypeHandler (String type, NumericType numType, boolean known) {
        super(type, numType, known);
    }

}
