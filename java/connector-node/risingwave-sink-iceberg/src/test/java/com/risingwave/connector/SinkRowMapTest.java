// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector;

import static org.junit.Assert.assertEquals;

import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.proto.Data;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class SinkRowMapTest {
    @Test
    public void testInsert() {
        SinkRowMap sinkRowMap = new SinkRowMap();
        SinkRow row = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1);
        List<Comparable<Object>> key = new ArrayList<>();
        key.add((Comparable<Object>) row.get(0));
        Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.IntegerType.get()));
        Record r = GenericRecord.create(schema);
        r.set(0, row.get(0));

        sinkRowMap.insert(key, r);
        assertEquals(1, sinkRowMap.map.size());
        assertEquals(null, sinkRowMap.map.get(key).getDelete());
        assertEquals(r, sinkRowMap.map.get(key).getInsert());
    }

    @Test
    public void testInsertAfterDelete() {
        SinkRowMap sinkRowMap = new SinkRowMap();
        Schema schema =
                new Schema(
                        Types.NestedField.optional(0, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(1, "name", Types.StringType.get()));

        SinkRow row1 = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1, "Alice");
        List<Comparable<Object>> key1 = new ArrayList<>();
        key1.add((Comparable<Object>) row1.get(0));
        Record r1 = GenericRecord.create(schema);
        r1.set(0, row1.get(0));
        r1.set(1, row1.get(1));
        SinkRow row2 = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1, "Bob");
        List<Comparable<Object>> key2 = new ArrayList<>();
        key2.add((Comparable<Object>) row2.get(0));
        Record r2 = GenericRecord.create(schema);
        r2.set(0, row2.get(0));
        r2.set(1, row2.get(1));

        sinkRowMap.delete(key1, r1);
        sinkRowMap.insert(key1, r2);
        assertEquals(1, sinkRowMap.map.size());
        assertEquals(r1, sinkRowMap.map.get(key1).getDelete());
        assertEquals(r2, sinkRowMap.map.get(key1).getInsert());
    }

    @Test
    public void testInsertAfterInsert() {
        SinkRowMap sinkRowMap = new SinkRowMap();
        SinkRow row = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1);
        List<Comparable<Object>> key = new ArrayList<>();
        key.add((Comparable<Object>) row.get(0));
        Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.IntegerType.get()));
        Record r = GenericRecord.create(schema);
        r.set(0, row.get(0));

        sinkRowMap.insert(key, r);
        boolean exceptionThrown = false;
        try {
            sinkRowMap.insert(key, r);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(
                    e.getMessage()
                            .toLowerCase()
                            .contains("try to insert a duplicated primary key"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `try to insert a duplicated primary key`");
        }
    }

    @Test
    public void testDelete() {
        SinkRowMap sinkRowMap = new SinkRowMap();

        SinkRow row = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1);
        List<Comparable<Object>> key = new ArrayList<>();
        key.add((Comparable<Object>) row.get(0));

        Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.IntegerType.get()));
        Record r = GenericRecord.create(schema);
        r.set(0, row.get(0));

        sinkRowMap.delete(key, r);
        assertEquals(1, sinkRowMap.map.size());
        assertEquals(null, sinkRowMap.map.get(key).getInsert());
        assertEquals(r, sinkRowMap.map.get(key).getDelete());
    }

    @Test
    public void testDeleteAfterDelete() {
        SinkRowMap sinkRowMap = new SinkRowMap();
        SinkRow row = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1);
        List<Comparable<Object>> key = new ArrayList<>();
        key.add((Comparable<Object>) row.get(0));

        Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.IntegerType.get()));
        Record r = GenericRecord.create(schema);
        r.set(0, row.get(0));

        sinkRowMap.delete(key, r);
        boolean exceptionThrown = false;
        try {
            sinkRowMap.delete(key, r);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(
                    e.getMessage().toLowerCase().contains("try to double delete a primary key"));
        }
        if (!exceptionThrown) {
            Assert.fail("Expected exception not thrown: `try to double delete a primary key`");
        }
    }

    @Test
    public void testDeleteAfterInsert() {
        SinkRowMap sinkRowMap = new SinkRowMap();

        SinkRow row = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1);
        List<Comparable<Object>> key = new ArrayList<>();
        key.add((Comparable<Object>) row.get(0));

        Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.IntegerType.get()));
        Record r = GenericRecord.create(schema);
        r.set(0, row.get(0));

        sinkRowMap.insert(key, r);
        sinkRowMap.delete(key, r);
        assertEquals(0, sinkRowMap.map.size());
    }

    @Test
    public void testDeleteAfterUpdate() {
        SinkRowMap sinkRowMap = new SinkRowMap();

        Schema schema =
                new Schema(
                        Types.NestedField.optional(0, "id", Types.IntegerType.get()),
                        Types.NestedField.optional(1, "name", Types.StringType.get()));

        SinkRow row1 = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1, "Alice");
        List<Comparable<Object>> key1 = new ArrayList<>();
        key1.add((Comparable<Object>) row1.get(0));
        Record r1 = GenericRecord.create(schema);
        r1.set(0, row1.get(0));
        r1.set(1, row1.get(1));

        SinkRow row2 = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1, "Clare");
        List<Comparable<Object>> key2 = new ArrayList<>();
        key2.add((Comparable<Object>) row2.get(0));
        Record r2 = GenericRecord.create(schema);
        r2.set(0, row2.get(0));
        r2.set(1, row2.get(1));

        sinkRowMap.delete(key1, r1);
        sinkRowMap.insert(key2, r2);
        sinkRowMap.delete(key2, r2);
        assertEquals(1, sinkRowMap.map.size());
        assertEquals(null, sinkRowMap.map.get(key1).getInsert());
        assertEquals(r1, sinkRowMap.map.get(key1).getDelete());
    }

    @Test
    public void testClear() {
        SinkRowMap sinkRowMap = new SinkRowMap();

        SinkRow row = new ArraySinkRow(Data.Op.OP_UNSPECIFIED, 1);
        List<Comparable<Object>> key = new ArrayList<>();
        key.add((Comparable<Object>) row.get(0));
        Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.IntegerType.get()));
        Record r = GenericRecord.create(schema);
        r.set(0, row.get(0));
        sinkRowMap.insert(key, r);

        sinkRowMap.clear();
        assertEquals(0, sinkRowMap.map.size());
    }
}
