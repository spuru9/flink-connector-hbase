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

import org.apache.flink.annotation.PublicEvolving;

/** Exception that can be thrown by {@link HBaseWriter} wrapping the HBase {@link Throwable}. */
@PublicEvolving
public class HBaseSinkException extends RuntimeException {

    public HBaseSinkException(String message) {
        super(message);
    }

    public HBaseSinkException(String message, Throwable throwable) {
        super(message, throwable);
    }

    /** Exception thrown during HBase sink initialization. */
    public static class HBaseSinkInitException extends HBaseSinkException {
        private static final String ERROR_MESSAGE =
                "Exception while trying to initialize HBase sink.";

        public HBaseSinkInitException(Throwable throwable) {
            super(ERROR_MESSAGE, throwable);
        }

        public HBaseSinkInitException(String message) {
            super(message);
        }
    }

    /** Exception thrown when trying to persist HBase mutations. */
    public static class HBaseSinkMutationException extends HBaseSinkException {
        private static final String ERROR_MESSAGE =
                "Exception while trying to persist records in HBase sink, not retrying.";

        public HBaseSinkMutationException(Throwable throwable) {
            super(ERROR_MESSAGE, throwable);
        }
    }
}
