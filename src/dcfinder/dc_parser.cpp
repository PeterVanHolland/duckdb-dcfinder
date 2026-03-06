#include "dcfinder/dc_parser.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace dcfinder {

void DCParser::SkipWhitespace(const string &text, idx_t &pos) {
	while (pos < text.size() && std::isspace(text[pos])) {
		pos++;
	}
}

string DCParser::ReadToken(const string &text, idx_t &pos) {
	SkipWhitespace(text, pos);
	string token;

	// Check for multi-char operators
	if (pos < text.size()) {
		if (pos + 1 < text.size()) {
			string two = text.substr(pos, 2);
			if (two == "!=" || two == "<=" || two == ">=") {
				pos += 2;
				return two;
			}
		}
		char c = text[pos];
		if (c == '(' || c == ')' || c == '=' || c == '<' || c == '>') {
			pos++;
			return string(1, c);
		}
	}

	// Read identifier (with dots for t1.col)
	while (pos < text.size() && !std::isspace(text[pos]) && text[pos] != '(' && text[pos] != ')' && text[pos] != '=' &&
	       text[pos] != '<' && text[pos] != '>' && text[pos] != '!') {
		token += text[pos];
		pos++;
	}
	return token;
}

PredicateOperator DCParser::ParseOperator(const string &op_str) {
	if (op_str == "=") {
		return PredicateOperator::EQ;
	}
	if (op_str == "!=") {
		return PredicateOperator::NEQ;
	}
	if (op_str == "<") {
		return PredicateOperator::LT;
	}
	if (op_str == "<=") {
		return PredicateOperator::LEQ;
	}
	if (op_str == ">") {
		return PredicateOperator::GT;
	}
	if (op_str == ">=") {
		return PredicateOperator::GEQ;
	}
	throw InvalidInputException("Unknown operator in DC: '%s'", op_str);
}

ParsedDC DCParser::Parse(const string &text) {
	ParsedDC result;
	idx_t pos = 0;

	// Expect: NOT( ... )
	string tok = ReadToken(text, pos);
	// Case-insensitive NOT
	string tok_upper = tok;
	std::transform(tok_upper.begin(), tok_upper.end(), tok_upper.begin(), ::toupper);
	if (tok_upper != "NOT") {
		throw InvalidInputException("DC must start with 'NOT(', got '%s'", tok);
	}

	tok = ReadToken(text, pos);
	if (tok != "(") {
		throw InvalidInputException("Expected '(' after NOT, got '%s'", tok);
	}

	// Parse predicates separated by AND
	while (true) {
		// Read: t1.col op t2.col
		string left = ReadToken(text, pos);
		if (left == ")" || left.empty()) {
			break;
		}

		// Parse table.column
		auto dot1 = left.find('.');
		if (dot1 == string::npos) {
			throw InvalidInputException("Expected table.column, got '%s'", left);
		}
		string left_table = left.substr(0, dot1);
		string left_col = left.substr(dot1 + 1);

		string op_str = ReadToken(text, pos);
		PredicateOperator op = ParseOperator(op_str);

		string right = ReadToken(text, pos);
		auto dot2 = right.find('.');
		if (dot2 == string::npos) {
			throw InvalidInputException("Expected table.column, got '%s'", right);
		}
		string right_table = right.substr(0, dot2);
		string right_col = right.substr(dot2 + 1);

		result.predicates.push_back({left_table, left_col, right_table, right_col, op});

		// Check for AND or )
		SkipWhitespace(text, pos);
		if (pos >= text.size()) {
			break;
		}
		tok = ReadToken(text, pos);
		tok_upper = tok;
		std::transform(tok_upper.begin(), tok_upper.end(), tok_upper.begin(), ::toupper);
		if (tok == ")") {
			break;
		}
		if (tok_upper != "AND") {
			throw InvalidInputException("Expected 'AND' or ')', got '%s'", tok);
		}
	}

	if (result.predicates.empty()) {
		throw InvalidInputException("DC has no predicates");
	}
	return result;
}

string ParsedDC::ToString() const {
	string result = "NOT(";
	for (idx_t i = 0; i < predicates.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		auto &p = predicates[i];
		result += p.left_table + "." + p.left_column + " " + Predicate::OperatorToString(p.op) + " " + p.right_table +
		          "." + p.right_column;
	}
	result += ")";
	return result;
}

} // namespace dcfinder
} // namespace duckdb
