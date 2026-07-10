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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.hbase.util.SerializableMutation;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer class for state in HBase sink. */
@Internal
public class HBaseStateSerializer extends AsyncSinkWriterStateSerializer<SerializableMutation> {
    @Override
    protected void serializeRequestToStream(SerializableMutation request, DataOutputStream out)
            throws IOException {
        Mutation mutation = request.get();
        ClientProtos.MutationProto.MutationType type;

        if (mutation instanceof Put) {
            type = ClientProtos.MutationProto.MutationType.PUT;
        } else if (mutation instanceof Delete) {
            type = ClientProtos.MutationProto.MutationType.DELETE;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unknown HBase mutation type, cannot serialize: %s",
                            mutation.getClass()));
        }

        out.writeLong(request.getRecordWriteAttempts());
        ClientProtos.MutationProto proto = ProtobufUtil.toMutation(type, mutation);
        proto.writeDelimitedTo(out);
    }

    @Override
    protected SerializableMutation deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        long retryNum = in.readLong();
        ClientProtos.MutationProto proto = ClientProtos.MutationProto.parseDelimitedFrom(in);
        Mutation mutation = ProtobufUtil.toMutation(proto);
        return new SerializableMutation(mutation, retryNum);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
