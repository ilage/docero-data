package org.docero.data.tests;


import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Test;

import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JsTest {
    @Test
    public void dummiesTests() {
        assertEquals("abc%d_e", "a'b\"c%d_e".replaceAll("[';\"]",""));
        assertEquals("a''b\"c%''''''d_e", "a'b\"c%'''d_e".replaceAll("[']","''"));
    }

    @Test
    public void test() throws java.io.IOException {
        com.fasterxml.jackson.databind.ObjectMapper jsonMapper = new com.fasterxml.jackson.databind.ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(IO1.class, new IO1Deserializer());
        jsonMapper.registerModule(module);

        O2 o2 = (O2) jsonMapper.readValue(new StringReader("{\"code\":2,\"val\":\"v\"}"), IO1.class);
        assertEquals("v", o2.getVal());

        O3 o3 = (O3) jsonMapper.readValue(new StringReader("{\"code\":3,\"val\":" + Long.MAX_VALUE + "}"), IO1.class);
        assertNotNull(o3.getVal());
        assertEquals(Long.MAX_VALUE, o3.getVal().longValue());

        o2 = (O2) jsonMapper.readValue(new StringReader("null"), IO1.class);
        assertNull(o2);

        Exception exception = null;
        try {
            o2 = (O2) jsonMapper.readValue(new StringReader("{\"val\":2}"), IO1.class);
        } catch (Exception e) {
            exception = e;
        }
        assertNotNull(exception);
    }

    public static class IO1Deserializer extends com.fasterxml.jackson.databind.JsonDeserializer<IO1> {
        @Override
        public IO1 deserialize(com.fasterxml.jackson.core.JsonParser p, com.fasterxml.jackson.databind.DeserializationContext ctxt) throws java.io.IOException, com.fasterxml.jackson.core.JsonProcessingException {
            com.fasterxml.jackson.databind.ObjectMapper mapper = (com.fasterxml.jackson.databind.ObjectMapper) p.getCodec();
            com.fasterxml.jackson.databind.node.ObjectNode root = mapper.readTree(p);
            if (root.isNull()) return null;
            if (root.has("code")) {
                String val = root.get("code").asText();
                if (val.equals("2")) return mapper.readValue(root.toString(), O2.class);
                else if (val.equals("3")) return mapper.readValue(root.toString(), O3.class);
            }
            throw new java.io.IOException("can't read object of ");
        }
    }

    public interface IO1 {
        int getCode();
    }

    public static class O2 implements IO1 {
        private int code;
        private String val;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getVal() {
            return val;
        }

        public void setVal(String v) {
            this.val = v;
        }
    }

    public static class O3 implements IO1 {
        private int code;
        private Long val;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public Long getVal() {
            return val;
        }

        public void setVal(Long v) {
            this.val = v;
        }
    }
}
