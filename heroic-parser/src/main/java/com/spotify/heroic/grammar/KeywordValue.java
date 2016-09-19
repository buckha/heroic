package com.spotify.heroic.grammar;

import lombok.Data;

@Data
class KeywordValue {
    private final String key;
    private final Expression expression;
}
