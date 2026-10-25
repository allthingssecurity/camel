/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.dataformat.bindy.fixed.delimited;

import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.bindy.annotation.DataField;
import org.apache.camel.dataformat.bindy.annotation.FixedLengthRecord;
import org.apache.camel.dataformat.bindy.fixed.BindyFixedLengthDataFormat;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A delimited field of a fixed-length record is read by code points (or graphemes with countGrapheme=true), like the
 * fixed fields, so the fields after it are read from the right place when it holds a character outside the BMP, a
 * combining character, or when the delimiter has more than one character.
 */
class BindyFixedLengthDelimitedFieldUnicodeTest extends CamelTestSupport {

    // U+1F600 (one code point, two UTF-16 chars)
    private static final String EMOJI = "😀";
    // U+20BB7, a CJK Extension B character used in Japanese family names (one code point, two UTF-16 chars)
    private static final String CJK_EXT_B = "𠮷";
    // e + combining acute accent (one grapheme, two code points)
    private static final String E_ACUTE = "é";

    @Test
    void unmarshalDelimitedFieldWithCharactersOutsideTheBmp() {
        Record out = template.requestBody("direct:unmarshal", "a" + EMOJI + "c^xy   def^\r\n", Record.class);
        assertEquals("a" + EMOJI + "c", out.first);
        assertEquals("xy", out.second);
        assertEquals("def", out.third);

        out = template.requestBody("direct:unmarshal", CJK_EXT_B + "野^xy   def^\r\n", Record.class);
        assertEquals(CJK_EXT_B + "野", out.first);
        assertEquals("xy", out.second);
        assertEquals("def", out.third);
    }

    @Test
    void marshalUnmarshalDelimitedFieldWithCharactersOutsideTheBmp() {
        Record in = new Record();
        in.first = "ok" + EMOJI;
        in.second = "x";
        in.third = EMOJI + EMOJI;
        String text = template.requestBody("direct:marshal", in, String.class);
        Record out = template.requestBody("direct:unmarshal", text, Record.class);
        assertEquals(in.first, out.first, text);
        assertEquals(in.second, out.second, text);
        assertEquals(in.third, out.third, text);
    }

    @Test
    void unmarshalDelimitedFieldWithGraphemes() {
        GraphemeRecord out = template.requestBody("direct:unmarshalGrapheme",
                "caf" + E_ACUTE + "^xy   def^\r\n", GraphemeRecord.class);
        assertEquals("caf" + E_ACUTE, out.first);
        assertEquals("xy", out.second);
        assertEquals("def", out.third);

        // Devanagari: 6 code points, 3 graphemes
        String namaste = "नमस्ते";
        out = template.requestBody("direct:unmarshalGrapheme", namaste + "^xy   def^\r\n", GraphemeRecord.class);
        assertEquals(namaste, out.first);
        assertEquals("xy", out.second);
        assertEquals("def", out.third);
    }

    @Test
    void unmarshalDelimiterOfTwoCharacters() {
        TwoCharDelimiterRecord out = template.requestBody("direct:unmarshalTwoChar", "abc##xy   def##\r\n",
                TwoCharDelimiterRecord.class);
        assertEquals("abc", out.first);
        assertEquals("xy", out.second);
        assertEquals("def", out.third);
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                BindyFixedLengthDataFormat codePoints = new BindyFixedLengthDataFormat(Record.class);
                from("direct:marshal").marshal(codePoints);
                from("direct:unmarshal").unmarshal(codePoints);

                from("direct:unmarshalGrapheme").unmarshal(new BindyFixedLengthDataFormat(GraphemeRecord.class));
                from("direct:unmarshalTwoChar").unmarshal(new BindyFixedLengthDataFormat(TwoCharDelimiterRecord.class));
            }
        };
    }

    @FixedLengthRecord
    public static class Record {

        @DataField(pos = 1, delimiter = "^")
        String first;

        @DataField(pos = 2, length = 5, align = "L", trim = true)
        String second;

        @DataField(pos = 3, delimiter = "^")
        String third;
    }

    @FixedLengthRecord(countGrapheme = true)
    public static class GraphemeRecord {

        @DataField(pos = 1, delimiter = "^")
        String first;

        @DataField(pos = 2, length = 5, align = "L", trim = true)
        String second;

        @DataField(pos = 3, delimiter = "^")
        String third;
    }

    @FixedLengthRecord
    public static class TwoCharDelimiterRecord {

        @DataField(pos = 1, delimiter = "##")
        String first;

        @DataField(pos = 2, length = 5, align = "L", trim = true)
        String second;

        @DataField(pos = 3, delimiter = "##")
        String third;
    }
}
