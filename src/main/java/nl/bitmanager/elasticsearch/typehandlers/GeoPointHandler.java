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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicGeoPointFieldData;

public class GeoPointHandler extends SafeTypeHandler {

    protected GeoPointHandler(String type) {
        super(type);
    }

    @Override
    protected Object[] _bytesToObjects(byte[] bytes) {
        Object[] ret = new Object[bytes.length / 8];
        for (int i=0; i<ret.length; i++) {
            double lat = GeoEncodingUtils.decodeLatitude(bytes, i*8);
            double lon = GeoEncodingUtils.decodeLongitude(bytes, i*8+4);
            ret[i] = String.format("%f; %f", lat, lon);
        }
        return ret; 
    }

    @Override
    protected Object[] _docValuesToObjects(AtomicFieldData fieldData, int docid) throws IOException {
        AbstractAtomicGeoPointFieldData dv = (AbstractAtomicGeoPointFieldData)fieldData;
        MultiGeoPointValues dvs = dv.getGeoPointValues();
        if (!dvs.advanceExact(docid)) return NO_DOCVALUES;
        int N = dvs.docValueCount();
        Object[] ret = new Object[N];
        if (N > 0) {
            for (int i = 0; i < N; i++) 
                ret[i] = dvs.nextValue().toString();
        }
        return ret;
    }

    @Override
    public byte[] toBytes(String s) {
        throw new RuntimeException ("GeopointHandler.toBytes not supported");
    }
    
}
