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

package nl.bitmanager.elasticsearch.support;

import static org.junit.Assert.*;

import org.junit.Test;

import nl.bitmanager.elasticsearch.typehandlers.TypeHandler;

public class BytesRangeTest {

    @Test
    public void test() {
        TypeHandler th = TypeHandler.create("text");
        byte[] a = th.toBytes("a");
        byte[] b = th.toBytes("b");
        byte[] c = th.toBytes("c");
        byte[] d = th.toBytes("d");
        byte[] e = th.toBytes("e");
        byte[] f = th.toBytes("f");
        byte[] bb = th.toBytes("bb");
        byte[] abc = th.toBytes("abc");
        byte[] de = th.toBytes("de");
        
        assertEquals (0, BytesHelper.bytesComparer.compare(a,  a));
        assertEquals (-1, BytesHelper.bytesComparer.compare(a,  b));
        assertEquals (1, BytesHelper.bytesComparer.compare(b,  a));
        assertEquals (-1, BytesHelper.bytesComparer.compare(b,  bb));
        assertEquals (1, BytesHelper.bytesComparer.compare(bb,  b));
        assertEquals (1, BytesHelper.bytesComparer.compare(c,  bb));
        assertEquals (-1, BytesHelper.bytesComparer.compare(bb,  c));

        assertEquals (0, BytesHelper.bytesComparerRev.compare(a,  a));
        assertEquals (1, BytesHelper.bytesComparerRev.compare(a,  b));
        assertEquals (-1, BytesHelper.bytesComparerRev.compare(b,  a));
        assertEquals (1, BytesHelper.bytesComparerRev.compare(b,  bb));
        assertEquals (-1, BytesHelper.bytesComparerRev.compare(bb,  b));
        assertEquals (-1, BytesHelper.bytesComparerRev.compare(c,  bb));
        assertEquals (1, BytesHelper.bytesComparerRev.compare(bb,  c));
        
        BytesRange range = new BytesRange("b..c", th);
        assertEquals (false, range.isInRange(a));
        assertEquals (true, range.isInRange(b));
        assertEquals (false, range.isInRange(c));
        assertEquals (false, range.isInRange(d));
        range = new BytesRange("b", th);
        assertEquals (false, range.isInRange(a));
        assertEquals (true, range.isInRange(b));
        assertEquals (false, range.isInRange(c));
        assertEquals (false, range.isInRange(d));

        range = new BytesRange("a..b", th);
        assertEquals (false, range.isInRange(b));
        assertEquals (true, range.isInRange(abc));
        assertEquals (false, range.isInRange(bb));
        assertEquals (false, range.isInRange(d));
        
        range = new BytesRange("d..e", th);
        assertEquals (false, range.isInRange(b));
        assertEquals (true, range.isInRange(de));
        assertEquals (false, range.isInRange(bb));
        assertEquals (false, range.isInRange(f));
        
        
    }

}
