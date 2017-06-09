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
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

public class DateHandler extends Int64Handler {

    protected DateHandler(String type) {
        super(type);
    }

    protected static Object[] convertToDate (Object[] arr) {
        for (int i=0; i<arr.length; i++)
            arr[i] = new Instant((long)arr[i]);
        return arr;
    }
    @Override
    protected Object[] _bytesToObjects(byte[] bytes) {
        return convertToDate (super._bytesToObjects(bytes));
    }
    @Override
    public Object[] docValuesToObjects(AtomicFieldData fieldData, int docid) {
        return convertToDate (super.docValuesToObjects(fieldData, docid));
    }

    @Override
    public byte[] toBytes(String s) {
        DateTime x = dateParser.parseDateTime(s);
        byte[] bytes = new byte[8];
        NumericUtils.longToSortableBytes(x.getMillis(), bytes, 0);
        return bytes;
    }
    static final DateTimeFormatter dateParser;
    static {
        DateTimeParser[] parsers = { 
                DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" ).getParser(),
                DateTimeFormat.forPattern( "yyyy-MM-dd" ).getParser() };
        dateParser = new DateTimeFormatterBuilder().append( null, parsers ).toFormatter();
    }

}
