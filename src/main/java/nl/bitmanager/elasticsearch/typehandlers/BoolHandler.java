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

import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;

public class BoolHandler extends TypeHandlerBase {

    protected BoolHandler(String type) {
        super(type, NumericType.BOOLEAN, true);
    }

    @Override
    protected Object[] _bytesToObjects(byte[] bytes) {
        Object[] ret = new Object[bytes.length];
        for (int i=0; i<bytes.length; i++)
            ret[i] = bytes[i]==(byte)'T';
        return ret; 
    }

    @Override
    protected Object[] _docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException {
        AtomicNumericFieldData numData = (AtomicNumericFieldData) fieldData;
        SortedNumericDocValues dvs = numData.getLongValues();
        if (!dvs.advanceExact(docid)) return NO_DOCVALUES;
        int N = dvs.docValueCount();
        Object[] ret = new Object[N];
        if (N > 0) {
            for (int i = 0; i < N; i++) 
                ret[i] = (0 != dvs.nextValue());
        }
        return ret;
    }

    @Override
    public byte[] toBytes(String s) {
        byte[] bytes = new byte[1];
        bytes[0] = Boolean.parseBoolean(s) ? (byte)'T' : (byte)'F';
        return bytes;
    }
}
