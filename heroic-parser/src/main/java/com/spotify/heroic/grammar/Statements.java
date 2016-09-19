package com.spotify.heroic.grammar;

import lombok.Data;

import java.util.List;

@Data
class Statements {
    private final List<Expression> expressions;
}
