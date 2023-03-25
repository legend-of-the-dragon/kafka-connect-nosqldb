/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.datacenter.kafka.sink.exception;

import org.apache.kafka.connect.errors.ConnectException;

public class DbDmlException extends ConnectException {

    public DbDmlException(String reason) {
        super(reason);
    }

    public DbDmlException(Throwable e) {
        super(e);
    }

    public DbDmlException(String reason, Throwable throwable) {
        super(reason, throwable);
    }


}
