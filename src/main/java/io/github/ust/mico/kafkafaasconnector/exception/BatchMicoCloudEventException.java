/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.github.ust.mico.kafkafaasconnector.exception;

import lombok.extern.slf4j.Slf4j;

/**
 * Base exception for CloudEvent errors.
 */
@Slf4j
public class BatchMicoCloudEventException extends Exception {

    private static final long serialVersionUID = 7526812870651753814L;

    /**
     * The CloudEvent that produced this error.
     */
    public final MicoCloudEventException[] exceptions;

    public BatchMicoCloudEventException(MicoCloudEventException[] exceptions) {
        super();
        this.exceptions = exceptions;
    }

    public BatchMicoCloudEventException(String message, MicoCloudEventException[] exceptions) {
        super(message);
        this.exceptions = exceptions;
    }

    public BatchMicoCloudEventException(String message, Throwable source, MicoCloudEventException[] exceptions) {
        super(message, source);
        this.exceptions = exceptions;
    }

    public BatchMicoCloudEventException(Throwable source, MicoCloudEventException[] exceptions) {
        super(source);
        this.exceptions = exceptions;
    }

}
