/*
 * Copyright (c) 2015 Spotify AB.
 *
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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import lombok.Data;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class Group implements Aggregation {
    public static final String NAME = "group";
    public static final String ALL = "*";

    private final Optional<List<String>> of;
    private final Optional<Aggregation> each;

    public Group(Optional<List<String>> of, Optional<Aggregation> each) {
        this.of = checkNotNull(of, "of");
        this.each = checkNotNull(each, "each");
    }

    @JsonCreator
    public static Group create(
        @JsonProperty("of") Optional<List<String>> of,
        @JsonProperty("each") Optional<AggregationOrList> each
    ) {
        return new Group(of, each.flatMap(AggregationOrList::toAggregation));
    }

    @Override
    public GroupInstance apply(final AggregationContext context) {
        final AggregationInstance instance = each.orElse(Empty.INSTANCE).apply(context);

        final Optional<List<String>> of = this.of.map(o -> {
            final ImmutableSet.Builder<String> b = ImmutableSet.builder();
            b.addAll(o).addAll(context.requiredTags());
            return ImmutableList.copyOf(b.build());
        });

        return new GroupInstance(of, instance);
    }
}
