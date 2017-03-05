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

import org.apache.lucene.util.NumericUtils;

public class Float32Handler extends Float64Handler {

    protected Float32Handler(String type) {
        super(type);
    }

    @Override
    protected Object[] _bytesToObjects(byte[] bytes) {
        Object[] ret = new Object[bytes.length / 4];
        for (int i=0; i<ret.length; i++)
            ret[i] = NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(bytes, i*4));
        return ret; 
    }

    @Override
    public byte[] toBytes(String s) {
        byte[] bytes = new byte[4];
        NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(Float.parseFloat(s)), bytes, 0);
        return bytes;
    }
}
