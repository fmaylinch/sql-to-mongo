package com.fmaylinch.sqlmongo.tokenizer;

/** Piece of code */
public interface Piece {

    int getStart();

    int getEnd();

    int getLine();
}