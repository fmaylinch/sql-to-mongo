package com.fmaylinch.sqlmongo.tokenizer;

import java.util.ArrayList;
import java.util.List;

/**
 * A line in the input
 */
public class Line implements Piece {

    private final List<Token> tokens = new ArrayList<>();
    private int indent;

    public List<Token> getTokens() {
        return tokens;
    }

    public int getIndent() {
        return indent;
    }

    public void setIndent(int indent) {
        this.indent = indent;
    }

    @Override
    public int getStart() {
        return tokens.get(0).getStart() - indent;
    }

    @Override
    public int getEnd() {
        return tokens.get(tokens.size() - 1).getEnd();
    }

    @Override
    public int getLine() {
        return tokens.get(0).getLine();
    }
}