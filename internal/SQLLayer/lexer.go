package sqllayer

import (
	"fmt"
	"strings"
)

type TokenType int

const (
	TOKEN_KEYWORD    TokenType = iota // Keywords like SELECT, FROM, WHERE, etc.
	TOKEN_IDENTIFIER                  // Table names, column names, etc.
	TOKEN_OPERATOR                    // Operators like =, <, >, etc.
	TOKEN_NUMBER                      // Numeric literals
	TOKEN_STRING                      // String literals
	TOKEN_COMMA                       // Comma (,)
	TOKEN_LPAREN                      // Left parenthesis (
	TOKEN_RPAREN                      // Right parenthesis
	TOKEN_DATATYPE                    // Data types like INT, VARCHAR, etc.
)

type Token struct {
	Type  TokenType
	Value string
}

func CreateToken(tokenType TokenType, value string) Token {
	return Token{
		Type:  tokenType,
		Value: value,
	}
}

// map of SQL keywords for quick lookup
var keywords = map[string]bool{
	"SELECT": true,
	"FROM":   true,
	"WHERE":  true,
	"INSERT": true,
	"INTO":   true,
	"VALUES": true,
	"UPDATE": true,
	"SET":    true,
	"DELETE": true,
	"CREATE": true,
	"TABLE":  true,
	"DROP":   true,
}

func isKeyword(word string) bool {
	_, exists := keywords[word]
	return exists
}

// map of SQL data types for quick lookup
var dataTypes = map[string]bool{
	"INT":  true,
	"TEXT": true,
	"BOOL": true,
}

func isDataType(word string) bool {
	_, exists := dataTypes[word]
	return exists
}

// map of SQL operators for quick lookup
var operators = map[string]bool{
	"=":   true,
	"<":   true,
	">":   true,
	"<=":  true,
	">=":  true,
	"!=":  true,
	"AND": true,
	"OR":  true,
}

// tokenize takes an input SQL query and returns a slice of tokens
func Tokenize(query string) ([]Token, error) {
	var tokens []Token

	for i := 0; i < len(query); i++ {
		ch := query[i]

		switch {
		case ch == ' ' || ch == '\t' || ch == '\n':
			// skip whitespace

		case ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '_':
			j := i
			for j < len(query) && (query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z' || query[j] >= '0' && query[j] <= '9' || query[j] == '_') {
				j++
			}
			word := query[i:j]
			upper := strings.ToUpper(word)
			i = j - 1
			if isKeyword(upper) {
				tokens = append(tokens, CreateToken(TOKEN_KEYWORD, upper))
			} else if isDataType(upper) {
				tokens = append(tokens, CreateToken(TOKEN_DATATYPE, upper))
			} else if upper == "AND" || upper == "OR" {
				tokens = append(tokens, CreateToken(TOKEN_OPERATOR, upper))
			} else {
				tokens = append(tokens, CreateToken(TOKEN_IDENTIFIER, word))
			}

		case ch >= '0' && ch <= '9':
			j := i
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			tokens = append(tokens, CreateToken(TOKEN_NUMBER, query[i:j]))
			i = j - 1

		case ch == '\'':
			j := i + 1
			for j < len(query) && query[j] != '\'' {
				j++
			}
			if j >= len(query) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			tokens = append(tokens, CreateToken(TOKEN_STRING, query[i+1:j]))
			i = j

		case ch == '=':
			tokens = append(tokens, CreateToken(TOKEN_OPERATOR, "="))

		case ch == '!':
			if i+1 < len(query) && query[i+1] == '=' {
				tokens = append(tokens, CreateToken(TOKEN_OPERATOR, "!="))
				i++
			} else {
				return nil, fmt.Errorf("unexpected character '!' at position %d", i)
			}

		case ch == '>':
			if i+1 < len(query) && query[i+1] == '=' {
				tokens = append(tokens, CreateToken(TOKEN_OPERATOR, ">="))
				i++
			} else {
				tokens = append(tokens, CreateToken(TOKEN_OPERATOR, ">"))
			}

		case ch == '<':
			if i+1 < len(query) && query[i+1] == '=' {
				tokens = append(tokens, CreateToken(TOKEN_OPERATOR, "<="))
				i++
			} else {
				tokens = append(tokens, CreateToken(TOKEN_OPERATOR, "<"))
			}

		case ch == ',':
			tokens = append(tokens, CreateToken(TOKEN_COMMA, ","))

		case ch == '(':
			tokens = append(tokens, CreateToken(TOKEN_LPAREN, "("))

		case ch == ')':
			tokens = append(tokens, CreateToken(TOKEN_RPAREN, ")"))

		default:
			return nil, fmt.Errorf("unexpected character '%c' at position %d", ch, i)
		}
	}

	return tokens, nil
}
