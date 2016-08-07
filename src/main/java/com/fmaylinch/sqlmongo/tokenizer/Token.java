package com.fmaylinch.sqlmongo.tokenizer;

public class Token implements Piece
{
    public enum Type { KEYWORD, IDENTIFIER, STRING, NUMBER, BOOLEAN, SYMBOL, COMMENT, START, END }

    /** piece of string */
    private String string;
    /** Type of current token */
    private Type type;
    /** index in input */
    private int start;
    /** line in input */
    private int line;


    public Token() { }

    public Token(String string, Type type)
    {
            this.string = string;
            this.type = type;
    }


    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    @Override
    public int getStart() {
        return start;
    }

    public void setStart(int index) {
        this.start = index;
    }

    @Override
    public int getEnd() {
        return start + string.length();
    }

    @Override
    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }

    /** If this token is a special END token that signals the end of stream of tokens */
    public boolean isEndToken() {
            return type == Type.END;
    }

    @Override
    public String toString() {
        return type.name().toLowerCase() + " `" + string.replace("\n", "\\n") + "` at position " + start + ", line " + (line+1);
    }
}