/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mbalassi.stateful;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Timestamp;
import java.time.Instant;

public class StatefulTesterJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.addSource(new TimeSaverSource()).print();

		env.execute("StatefulTester");
	}

	public static final class TimeSaverSource
			extends RichParallelSourceFunction<Tuple2<Integer, Timestamp>>
			implements CheckpointedFunction {

		private transient ListState<Timestamp> initialStartTime;
		private transient Integer subtaskId;

		@Override
		public void open(Configuration parameters) throws Exception {
			subtaskId = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Timestamp>> ctx) throws Exception {
			while (true) {
				for (Timestamp startTime : initialStartTime.get()) {
					ctx.collect(Tuple2.of(subtaskId, startTime));
				}
				Thread.sleep(1000);
			}
		}

		@Override
		public void cancel() {}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ListStateDescriptor<Timestamp> timestampDescriptor =
					new ListStateDescriptor<Timestamp>("initialStartTime", Timestamp.class);

			initialStartTime = context.getOperatorStateStore().getListState(timestampDescriptor);

			if (!context.isRestored()) {
				initialStartTime.add(Timestamp.from(Instant.now()));
			}
		}
	}
}
