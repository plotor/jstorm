/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backtype.storm.scheduler;

/**
 * 记录每个 Executor 对应的 startTask 和 endTask，从而保证在计算时每个 Executor 都是一个连续的任务集合
 */
public class ExecutorDetails {

    int startTask;
    int endTask;

    public ExecutorDetails(int startTask, int endTask) {
        this.startTask = startTask;
        this.endTask = endTask;
    }

    public int getStartTask() {
        return startTask;
    }

    public int getEndTask() {
        return endTask;
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof ExecutorDetails)) {
            return false;
        }

        ExecutorDetails executor = (ExecutorDetails) other;
        return (this.startTask == executor.startTask) && (this.endTask == executor.endTask);
    }

    public int hashCode() {
        return this.startTask + 13 * this.endTask;
    }

    @Override
    public String toString() {
        return "[" + this.startTask + ", " + this.endTask + "]";
    }
}
