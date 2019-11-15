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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;

public class Int64Handler extends SafeTypeHandler {

    protected Int64Handler(String type) {
        super(type, NumericType.LONG, true);
    }
    protected Int64Handler(String type, NumericType numType, boolean known) {
        super(type, numType, known);
    }

    @Override
    protected Object[] _bytesToObjects(byte[] bytes) {
        Object[] ret = new Object[bytes.length / 8];
        for (int i=0; i<ret.length; i++)
            ret[i] = NumericUtils.sortableBytesToLong(bytes, i*8);
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
                ret[i] = dvs.nextValue();
        }
        return ret;
    }

    @Override
    public byte[] toBytes(String s) {
        byte[] bytes = new byte[8];
        NumericUtils.longToSortableBytes(Long.parseLong(s), bytes, 0);
        return bytes;
    }

}
