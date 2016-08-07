package com.fmaylinch.sqlmongo.tokenizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Tokenizer {

    // Listener

    public interface TokenListener {
        /** Called every time a token is read */
        void onToken(Token token);
    }

    private List<TokenListener> tokenListeners = new ArrayList<>();

    public void addTokenListener(TokenListener tokenListener) {
        tokenListeners.add(tokenListener);
    }


    // Input and related data

    /** Input parsed  */
    private CharSequence input;

    /** Index of input (where {@link #readToken()} should look for the next token) */
    private int index = 0;

    /** Tokens extracted */
    private List<Token> tokens = new ArrayList<>();

    /** Index for next token in current token line (used by {@link #nextToken()}) */
    private int nextTokenIndex = 0;

    /** Lines of tokens extracted */
    private List<Line> lines = new ArrayList<>();


    // Configurable tokens

    private Set<String> keywords = new HashSet<>();
    private Set<String> booleanKeywords = new HashSet<>(Arrays.asList("true", "false"));
    private String singleSymbols = "()[]{}:.,;!";
    private String combinedSymbols = "+-*/%=<>&|";

    public Set<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(Set<String> keywords) {
        this.keywords = keywords;
    }

    public Set<String> getBooleanKeywords() {
        return booleanKeywords;
    }

    public void setBooleanKeywords(Set<String> booleanKeywords) {
        this.booleanKeywords = booleanKeywords;
    }

    public String getSingleSymbols() {
        return singleSymbols;
    }

    public void setSingleSymbols(String singleSymbols) {
        this.singleSymbols = singleSymbols;
    }

    public String getCombinedSymbols() {
        return combinedSymbols;
    }

    public void setCombinedSymbols(String combinedSymbols) {
        this.combinedSymbols = combinedSymbols;
    }



    public Tokenizer(CharSequence input) {
        this.input = input;
        lines.add(new Line());
    }

    public Line getLine(int line) {
        return lines.get(line);
    }

    private Line getLastLine() {
        return getLine(lines.size() - 1);
    }

    /**
     * Returns the token that comes next in input.
     *
     * For example, if the underscore '_' represents the current position, nextToken() would return token C.
     * Token stream: A B _ C D
     */
    public Token nextToken() {
        return getToken(nextTokenIndex);
    }

    public Token getTokenRelative(int relativeIndex) {
        return getToken(nextTokenIndex + relativeIndex);
    }

    /**
     * Index of next token that comes next in input.
     *
     * For example, if the underscore '_' represents the current position, getNextTokenIndex() would return 2.
     * Token stream: A B _ C D
     */
    public int getNextTokenIndex() {
        return nextTokenIndex;
    }

    /**
     * Skips next token, so next token changes.
     *
     * For example, if the underscore '_' represents the current position, skipNextToken() would change
     * from stream  A B _ C D  to  A B C _ D
     *
     * @return skipped token
     */
    public Token skipNextToken() {
        Token skipped = nextToken();
        nextTokenIndex++;
        return skipped;
    }

    /**
     * Returns a token in a specific index. Reads more tokens from input if necessary.
     * If there's no token at that index, it returns an END token.
     *
     * @see Token#isEndToken()
     */
    public Token getToken(int tokenIndex) {
        while (tokens.size() <= tokenIndex && !isEndReached()) {
            readToken();
        }
        return tokens.get(Math.min(tokenIndex, tokens.size() - 1));
    }

    private boolean isEndReached() {
        return !tokens.isEmpty() && tokens.get(tokens.size()-1).isEndToken();
    }

    /** Adds another token to the lines of tokens, if there are more tokens available. */
    private void readToken()
    {
        skipWhitespace();
        final Token token = parseToken();

        if (token.getType() != Token.Type.COMMENT) {
            addToken(token);
        }

        for (TokenListener tokenListener : tokenListeners) {
            tokenListener.onToken(token);
        }
    }

    private void skipWhitespace()
    {
        int indent = 0;

        while (index < input.length() && Character.isWhitespace(input.charAt(index))) {
            if (input.charAt(index) == ' ') {
                indent++;
            } else if (input.charAt(index) == '\n') {
                indent = 0;
                lines.add(new Line());
            }
            index++;
        }

        // If we found the first token of the line, set indent
        if (getLastLine().getTokens().isEmpty() && index < input.length()) {
            getLastLine().setIndent(indent);
        }
    }

    private Token parseToken()
    {
        final Token token = new Token();
        token.setLine(lines.size() - 1);
        token.setStart(index);

        int i = index;

        String errorMessage = null;

        if (index >= input.length()) {

            token.setType(Token.Type.END);

        } else if (isValidFirstIdChar(input.charAt(i))) {

            token.setType( Token.Type.IDENTIFIER );

            while (i < input.length() && isValidIdChar(input.charAt(i))) {
                i++;
            }

        } else if (input.charAt(i) == '"' || input.charAt(i) == '\'') {

            token.setType( Token.Type.STRING );

            char quoteChar = input.charAt(i);

            i++;

            while (i < input.length() && input.charAt(i) != quoteChar && input.charAt(i) != '\n') {
                if (input.charAt(i) == '\\') i++;
                i++;
            }

            if (i >= input.length() || input.charAt(i) != quoteChar) {
                errorMessage = "String literal is not terminated with " + quoteChar;
            } else {
                i++;
            }

        } else if (Character.isDigit(input.charAt(i))) {

            token.setType( Token.Type.NUMBER );

            while (i < input.length() && Character.isDigit(input.charAt(i))) {
                i++;
            }
            if (i < input.length() && input.charAt(i) == '.') {
                i++;
                while (i < input.length() && Character.isDigit(input.charAt(i))) {
                    i++;
                }
            }

        } else if (input.charAt(i) == '/' && i < input.length()-1 && input.charAt(i+1) == '/') { // Line comment

            token.setType( Token.Type.COMMENT );

            // Skip until the end of line
            while (i < input.length() && input.charAt(i) != '\n') {
                i++;
            }

            if (i < input.length()) { // there's a new line
                lines.add(new Line());
                i++;
            }

        } else if (input.charAt(i) == '/' && i < input.length()-1 && input.charAt(i+1) == '*') { // Block comment

            token.setType( Token.Type.COMMENT );

            while (i < input.length()-1 && (input.charAt(i) != '*' || input.charAt(i+1) != '/')) {
                if (input.charAt(i) == '\n') {
                    lines.add(new Line());
                }
                i++;
            }

            if (i >= input.length()-1) {
                errorMessage = "Block comment is not terminated with '*/'";
            }

            i += 2;

        } else if (isSymbol(input.charAt(i))) { // Symbol

            token.setType( Token.Type.SYMBOL );

            if (isSingleSymbol(input.charAt(i))) {
                i++;
            } else {
                while (i < input.length() && isCombinedSymbol(input.charAt(i))) {
                    i++;
                }
            }

        } else {

            errorMessage = "Unexpected character `" + input.charAt(i) + "`";
            i++;
        }

        String string = input.subSequence(index, Math.min(i, input.length())).toString();
        token.setString(string);

        if (token.getType() == Token.Type.IDENTIFIER) {

            if (keywords.contains(token.getString())) {
                token.setType( Token.Type.KEYWORD );
            } else if (booleanKeywords.contains(token.getString())) {
                token.setType( Token.Type.BOOLEAN );
            }
        }

        if (errorMessage != null) {
            throwEx(token, errorMessage);
        }

        index += string.length();

        return token;
    }

    private void throwEx(Token token, String message) {
        throw new TokenException(token, message);
    }

    private boolean isSymbol(char c) {
        return isSingleSymbol(c) || isCombinedSymbol(c);
    }

    private boolean isSingleSymbol(char c) {
        return singleSymbols.indexOf(c) >= 0;
    }

    private boolean isCombinedSymbol(char c) {
        return combinedSymbols.indexOf(c) >= 0;
    }

    private boolean isValidIdChar(char c) {
        return isValidFirstIdChar(c) || Character.isDigit(c);
    }

    private boolean isValidFirstIdChar(char c) {
        return Character.isLetter(c) || c == '_' || c == '$';
    }


    private void addToken(Token token) {
        tokens.add(token);
        getLastLine().getTokens().add(token);
    }
}
 