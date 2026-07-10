/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.connector.hbase.util.SerializableMutation;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HBaseStateSerializerTest {

    private final HBaseStateSerializer serializer = new HBaseStateSerializer();

    @Test
    public void testSerdePut() throws IOException {
        SerializableMutation mut = new SerializableMutation(new Put(new byte[] {0x00, 0x01}, 1001));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        serializer.serializeRequestToStream(mut, out);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);

        Mutation mutationResult = serializer.deserializeRequestFromStream(0, in).get();

        assertThat(mutationResult.getRow()).isEqualTo(new byte[] {0x00, 0x01});
        assertThat(mutationResult.getTimestamp()).isEqualTo(1001);
    }

    @Test
    public void testSerdeDelete() throws IOException {
        SerializableMutation mut =
                new SerializableMutation(new Delete(new byte[] {0x00, 0x02}, 1002));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        serializer.serializeRequestToStream(mut, out);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);

        Mutation mutationResult = serializer.deserializeRequestFromStream(0, in).get();

        assertThat(mutationResult.getRow()).isEqualTo(new byte[] {0x00, 0x02});
        assertThat(mutationResult.getTimestamp()).isEqualTo(1002);
    }

    @Test
    public void testSerializationUnsupportedMutationType() {
        SerializableMutation mut = new SerializableMutation(new Append(new byte[] {0x00, 0x02}));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        assertThrows(
                IllegalArgumentException.class,
                () -> serializer.serializeRequestToStream(mut, out));
    }
}
