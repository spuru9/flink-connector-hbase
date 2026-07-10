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

package org.apache.flink.connector.hbase.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import org.apache.hadoop.hbase.client.Mutation;

/**
 * This is a helper class used to wrap an {@link ElementConverter} supplied by the user that
 * converts the input data to {@link Mutation}. With this class, the elements will be seamlessly
 * converted to internal {@link SerializableMutation} objects that can be serialized by the sink.
 */
@Internal
public class WrappedElementConverter<InputT>
        implements ElementConverter<InputT, SerializableMutation> {
    private static final long serialVersionUID = 1L;

    private final ElementConverter<InputT, Mutation> originalElementConverter;

    public WrappedElementConverter(ElementConverter<InputT, Mutation> originalElementConverter) {
        this.originalElementConverter = originalElementConverter;
    }

    @Override
    public void open(WriterInitContext context) {
        ElementConverter.super.open(context);
        originalElementConverter.open(context);
    }

    @Override
    public SerializableMutation apply(InputT element, SinkWriter.Context context) {
        return new SerializableMutation(originalElementConverter.apply(element, context));
    }
}
