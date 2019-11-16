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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * Helper class that offers services to get all public properties and format
 * them into a string value.
 *
 * @author pweerd
 *
 */
public class ReflectionHelper {
    private String sep;
    private boolean skipEmpty;
    private String[] excludes;

    /**
     * Creates a ReflectionHelper with a specific custom parameters
     *
     * @param sep
     *            the separator to be used when joining the public properties
     * @param skipEmpty
     *            determines whether empty properties should be included or not
     * @param excludes
     *            an array of properties to be excluded
     */
    public ReflectionHelper(String sep, boolean skipEmpty, String[] excludes) {
        this.sep = sep;
        this.skipEmpty = skipEmpty;
        setExcludes(excludes);
    }

    /**
     * Creates a ReflectionHelper with a specific custom parameters. There are
     * no excluded properties.
     *
     * @param sep
     *            the separator to be used when joining the public properties
     * @param skipEmpty
     *            determines whether empty properties should be included or not
     */
    public ReflectionHelper(String sep, boolean skipEmpty) {
        this.sep = sep;
        this.skipEmpty = skipEmpty;
    }

    /**
     * Creates a ReflectionHelper with default parameters: The separator will be
     * ', ' and empty properties will be included in the result.
     */
    public ReflectionHelper() {
        this.sep = ", ";
        this.skipEmpty = true;
    }

    /**
     * set the exludes. If a property is found in the exclude array, the
     * property is not emitted in the result.
     *
     * @param excludes
     *            a String of excludes, separated by a ',' a ';' or a '|'.
     */
    public void setExcludes(String excludes) {
        if (excludes == null || excludes.length() == 0)
            this.excludes = null;
        else
            setExcludes(excludes.split("[,;|]"));
    }
    /**
     * set the exludes. If a property is found in the exclude array, the
     * property is not emitted in the result.
     *
     * @param excludes
     *            an array of property names to be excluded from the result.
     */
    public void setExcludes(String[] excludes) {
        this.excludes = excludes == null || excludes.length == 0 ? null : excludes;
    }

    /**
     * Builds a String by emitting all public properties of the object as
     * key-value pairs. The result will be encapsulated in a &lt;classname&gt;[.....]
     * construction
     *
     * @param obj
     *            the object to be inspected
     */
    public String toString(Object obj) {
        StringBuilder bldr = new StringBuilder();
        toString(bldr, obj);
        return bldr.toString();
    }

    /**
     * Appends all public properties of the object as key-value pairs to the
     * StringBuilder. The result will be encapsulated in a &lt;classname&gt;[.....]
     * construction
     *
     * @param bldr
     *            the {@link StringBuilder} to append to.
     * @param obj
     *            the object to be inspected
     */
    public void toString(StringBuilder bldr, Object obj) {
        writeStart(bldr, obj);
        writeObject(bldr, 0, obj);
        writeEnd(bldr);
    }

    private boolean isExcluded(String name) {
        if (name.equals("getClass"))
            return true;
        if (excludes == null)
            return false;
        for (int j = 0; j < excludes.length; j++) {
            if (name.equals(excludes[j]))
                return true;
        }
        return false;
    }

    public void writeStart (StringBuilder bldr, Object obj) {
        Class<?> cls = obj.getClass();
        bldr.append(cls.getSimpleName());
        bldr.append(" [");
    }
    public void writeEnd (StringBuilder bldr) {
        int n = bldr.length();
        if (n > 0) {
            if (bldr.charAt(n-1) != '[') {
                if (n > sep.length()) bldr.setLength(n-sep.length());
            }
        }
        bldr.append("]");
    }

    /**
     * Appends all public properties of the object as key-value pairs to the
     * StringBuilder.
     *
     * @param bldr
     *            the {@link StringBuilder} to append to.
     * @param obj
     *            the object to be inspected
     */
    public void writeObject (StringBuilder bldr, int lvl, Object obj) {
        if (obj==null) {
            bldr.append ("NULL");
            return;
        }
        Class<?> cls = obj.getClass();
        if (lvl > 3 || isSimple(obj)) {
            bldr.append(decodeCrLf(obj.toString()) + " [" + cls.getName() + "]");
            return;
        }

        Method[] methods = cls.getMethods();
        // LogWrapper._debug ("Class {0} has {1} methods...", cls.getName(),
        // methods.length);
        for (int i = 0; i < methods.length; i++) {
            Method m = methods[i];
            m.setAccessible(true);
            String methodName = m.getName();
            String propName = null;
            // LogWrapper._debug ("-- handling {0}", methodName);
            if (isExcluded(methodName))
                continue;

            // LogWrapper._debug ("-- -- not excluded");
            if (methodName.startsWith("get"))
                propName = methodName.substring(3);
            else if (methodName.startsWith("is"))
                propName = methodName.substring(2);
            else
                continue;

            Type[] parms = m.getGenericParameterTypes();
            if (parms != null && parms.length != 0)
                continue; // Skip methods that need parameters

            Type ret = m.getGenericReturnType();
            if (ret.toString().equalsIgnoreCase("void"))
                continue;

            newLineAndIndent (bldr, lvl);
            bldr.append(propName);
            bldr.append("=");
            writeProp (bldr, lvl+1, obj, m);
        }
        //writeFields (bldr, lvl, cls, obj);
    }

    private static boolean isSimple (Object obj) {
        if (obj==null) return true;
        //String name = obj.getClass().getName();
        return false;
    }

    private static void newLineAndIndent (StringBuilder sb, int lvl) {
        sb.append("\r\n");
        for (int i=0; i<lvl; i++) {
            sb.append("-- ");
        }
    }

    /**
     * Appends all fields of the object as key-value pairs to the
     * StringBuilder.
     *
     * @param bldr
     *            the {@link StringBuilder} to append to.
     * @param obj
     *            the object to be inspected
     */
    public void writeFields (StringBuilder bldr, int lvl, Object obj) {
        Class<?> cls = obj.getClass();
        for (; cls != null && cls!=Object.class; cls = cls.getSuperclass()) {
            writeFields (bldr, lvl, cls, obj);
        }
    }
    private void writeFields (StringBuilder bldr, int lvl, Class<?> cls, Object obj) {
        //bldr.append(String.format ("{{%s}}", cls.getSimpleName()));
        Field[] fields = cls.getDeclaredFields();
        for (int i=0; i<fields.length; i++) {
            Field field = fields[i];
            String fieldName = field.getName();
            if (isExcluded(fieldName))
                continue;
            field.setAccessible(true);

            newLineAndIndent (bldr, lvl);
            bldr.append(fieldName);
            bldr.append("=");
            writeProp (bldr, lvl+1, obj, field);
        }
    }

    private void writeProp(StringBuilder sb, int lvl, Object obj, Field m) {
        try {
            writeObject (sb, lvl, m.get(obj));
        } catch (Exception e) {
            sb.append("[error: " + e.getMessage() + "]");
        }
    }
    private void writeProp(StringBuilder sb, int lvl, Object obj, Method m) {
        try {
            writeObject (sb, lvl, m.invoke(obj));
        } catch (Exception e) {
            sb.append("[error: " + e.getMessage() + "]");
        }
    }

    private static String decodeCrLf(String s) {
        StringBuilder bldr = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '\r' :
                    bldr.append("\\r");
                    break;
                case '\t' :
                    bldr.append("\\t");
                    break;
                case '\n' :
                    bldr.append("\\n");
                    break;
                default :
                    bldr.append(ch);
            }
        }
        return bldr.toString();
    }

}
