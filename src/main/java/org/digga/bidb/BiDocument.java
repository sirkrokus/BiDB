package org.digga.bidb;

import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class BiDocument extends Document implements DataExternalizable {

    public static final String ID = "_id";

    private static final short CODE_STRING = 1;
    private static final short CODE_LONG = 2;
    private static final short CODE_INT = 3;
    private static final short CODE_BOOL = 4;
    private static final short CODE_DATE = 5;
    private static final short CODE_FLOAT = 6;
    private static final short CODE_DOUBLE = 7;
    private static final short CODE_NUMBER = 8;

    public BiDocument() {
    }

    public BiDocument(Document document) {
        this(document, false);
    }

    public BiDocument(Document document, boolean idIncluded) {
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            if (ID.equals(entry.getKey()) && !idIncluded) {
                continue;
            }
            put(entry.getKey(), entry.getValue());
        }
    }

    public BiDocument(String field, String value) {
        put(field, value);
    }

    /**
     *   fieldNames = "name,tel,num"
     *   values = ["john", "793-98-27", 23]
     */
    public BiDocument(String fieldNames, Object... values) {
        String[] toks = fieldNames.split("[,;]");
        for (int i = 0; i < toks.length; i++) {
            put(toks[i].trim(), (values != null && i < values.length ? values[i] : null));
        }
    }

    /**
     *  ["addr=Some addr", "name:John Dow", "num=239"]
     */
    public BiDocument(String[] keyValues) {
        if (keyValues != null && keyValues.length > 0) {
            for (int i = 0; i < keyValues.length; i++) {
                String[] kv = keyValues[i].split("[=\\:]");
                put(kv[0].trim(), kv.length > 1 ? kv[1].trim() : null);
            }
        }
    }

    public boolean isNew() {
        return getId() == null || getId() == 0L;
    }

    public Long getId() {
        return getLong(ID);
    }

    public BiDocument setId(Long id) {
        put(ID, id);
        return this;
    }

    public BiDocument add(String field, Object value) {
        put(field, value);
        return this;
    }

    @Override
    public void writeExternal(DataOutput out) throws IOException {
        out.writeInt(size());
        for (String name : keySet()) {
            out.writeUTF(name);
            Object value = get(name);
            if (value instanceof Long) {
                out.writeShort(CODE_LONG);
                out.writeLong((Long) value);
            } else if (value instanceof Integer) {
                out.writeShort(CODE_INT);
                out.writeInt((Integer) value);
            } else if (value instanceof Boolean) {
                out.writeShort(CODE_BOOL);
                out.writeBoolean((Boolean) value);
            } else if (value instanceof Float) {
                out.writeShort(CODE_FLOAT);
                out.writeFloat((Float) value);
            } else if (value instanceof Double) {
                out.writeShort(CODE_DOUBLE);
                out.writeDouble((Double) value);
            } else {
                out.writeShort(CODE_STRING);
                out.writeUTF(value == null ? "__null" : String.valueOf(value));
            }
        }
    }

    @Override
    public void readExternal(DataInput in) throws IOException {
        clear();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            short classCode = in.readShort();
            if (classCode == CODE_LONG) {
                Long value = in.readLong();
                put(name, value);
            } else if (classCode == CODE_INT) {
                Integer value = in.readInt();
                put(name, value);
            } else if (classCode == CODE_BOOL) {
                Boolean value = in.readBoolean();
                put(name, value);
            } else if (classCode == CODE_FLOAT) {
                Float value = in.readFloat();
                put(name, value);
            } else if (classCode == CODE_DOUBLE) {
                Double value = in.readDouble();
                put(name, value);
            } else {
                String value = in.readUTF();
                put(name, value.equals("__null") ? null : value);
            }
        }
    }

    public String asString(boolean pretty) {
        StringBuilder sb = new StringBuilder("{");
        if (pretty) {
            sb.append("\n  ");
        }
        sb.append("_id=").append(get(ID)).append(", ");
        if (pretty) {
            sb.append("\n");
        }
        for (String key : keySet()) {
            if (ID.equals(key)) {
                continue;
            }
            if (pretty) {
                sb.append("  ");
            }
            sb.append(key).append("=").append(get(key)).append(", ");
            if (pretty) {
                sb.append("\n");
            }
        }
        sb.append("}");
        return sb.toString().replace(", }", "}");
    }

}
