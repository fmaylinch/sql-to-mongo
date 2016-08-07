package com.fmaylinch.sqlmongo.tokenizer;

public class TokenException extends RuntimeException {

    private final Token token;

    public TokenException(Token token, String message) {
        super(message);
        this.token = token;
    }

    public Token getToken() {
        return token;
    }
}