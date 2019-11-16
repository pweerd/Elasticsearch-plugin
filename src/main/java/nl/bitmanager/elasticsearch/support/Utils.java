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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.jar.Manifest;

import org.apache.lucene.util.BytesRef;

public class Utils {
    public static String getStackTrace() {
        return getStackTrace(2, true);
    }

    public static String getStackTrace(int from, boolean prependDashes) {
        StringBuilder bld = new StringBuilder();
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        for (int i = from; i < trace.length; i++) {
            if (prependDashes)
                bld.append("-- ");
            bld.append(trace[i]);
            bld.append("\r\n");
        }
        return bld.toString();
    }

    public static void printStackTrace() {
        System.out.println(getStackTrace(2, true));
    }

    public static void printStackTrace(String reason) {
        System.out.println("Dump stack: " + reason);
        System.out.println(getStackTrace(2, true));
    }

    public static String prettySize(long size) {
        if (size < 1000)
            return String.format("%d bytes", size);
        String unit = "KB";
        double d = size / 1000.0;
        if (d > 1000.0) {
            unit = "MB";
            d /= 1000.0;
            if (d > 1000.0) {
                unit = "GB";
                d /= 1000.0;
            }
        }
        if (d < 10)
            return String.format("%.2f %s", d, unit);
        if (d < 100)
            return String.format("%.1f %s", d, unit);
        return String.format("%.0f %s", d, unit);
    }

    public static String getType(Object obj) {
        if (obj == null)
            return "NULL";
        return obj.getClass().getName();
    }

    public static String getSimpleType(Object obj) {
        if (obj == null)
            return "NULL";
        return obj.getClass().getSimpleName();
    }

    public static void dump (String what, Object obj) {
        System.out.printf ("-- %s=%s, type=%s\n", what, obj, getType(obj));
    }

    public static void dumpObj (String what, Object obj) {
        dump (what,obj);
        if (obj==null) return;

        Class<? extends Object> c = obj.getClass();

        for (Field f: c.getDeclaredFields()) {
            System.out.println ("-- -- Field: " + f);
        }
        for (Method m: c.getDeclaredMethods()) {
            System.out.println ("-- -- Method: " + m);
        }

        Class<? extends Object> p=c;
        while (true) {
            p = p.getSuperclass();
            if (p==null) break;
            System.out.println ("-- -- Super: " + p.getName());
        }
    }

    public static Map<String,String> getManifestEntries() throws IOException {
        HashMap<String, String> map = new HashMap<String, String>();
        Enumeration<java.net.URL> resources = Utils.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            Manifest manifest = null;
            InputStream strm = null;
            try {
                strm = resources.nextElement().openStream();
                manifest = new Manifest(strm);
                for (Entry<Object, Object> kvp: manifest.getMainAttributes().entrySet()) {
                    map.put(toString (kvp.getKey()), toString(kvp.getValue()));
                }
            } finally {
                if (strm!=null) strm.close();
            }
        }
        return map;

    }

    /** Return the classname, optionallywithout  without 'nl.bitmanager.elasticsearch.' */
    public static String getTrimmedClass (Object obj) {
        if (obj==null) return "NULL";

        String ret = obj.getClass().getName();
        return ret.startsWith("nl.bitmanager.elasticsearch.") ? ret.substring(28) : ret;
    }

    /**
     * Create a clone in the bytes from the parameter, or return empty if the bytes were null or []
     */
    public static byte[] getBytes(BytesRef b, byte[] empty) {
        if (b == null || b.length == 0)
            return empty;
        if (b.offset==0 && b.length==b.bytes.length) return b.bytes;
        return Arrays.copyOfRange(b.bytes, b.offset, b.offset + b.length);
    }
    /**
     * Create a clone in the bytes from the parameter, or return empty if the bytes were null or []
     */
    public static byte[] cloneBytes(BytesRef b, byte[] empty) {
        if (b == null || b.length == 0)
            return empty;
        return Arrays.copyOfRange(b.bytes, b.offset, b.offset + b.length);
    }

    /**
     * Create a clone in the bytes from the parameter, or return empty if the bytes were null or []
     */
    public static byte[] cloneBytes(byte[] b, byte[] empty) {
        if (b == null || b.length == 0)
            return empty;
        return Arrays.copyOf(b, b.length);
    }

    /**
     * Convert an object to a String, or return null if the object was null
     */
    public static String toString(BytesRef obj) {
        return obj==null ? null : obj.utf8ToString();
    }

    /**
     * Convert an object to a String, or return null if the object was null
     */
    public static String toString(Object obj) {
        return obj==null ? null : obj.toString();
    }

    public static String getClass (Object obj) {
        if (obj==null) return "null";
        return obj.getClass().getName();
    }


}
