#pragma once

#include "duckdb.hpp"
#include "dcfinder/predicate.hpp"
#include <vector>
#include <string>

namespace duckdb {
namespace dcfinder {

//! Parsed denial constraint from user-provided text
struct ParsedDC {
	struct ParsedPredicate {
		string left_table; //! "t1" or "t2"
		string left_column;
		string right_table;
		string right_column;
		PredicateOperator op;
	};
	vector<ParsedPredicate> predicates;

	string ToString() const;
};

//! Parse DC text like: NOT(t1.col1 = t2.col1 AND t1.col2 != t2.col2)
struct DCParser {
	static ParsedDC Parse(const string &text);

private:
	static PredicateOperator ParseOperator(const string &op_str);
	static void SkipWhitespace(const string &text, idx_t &pos);
	static string ReadToken(const string &text, idx_t &pos);
};

} // namespace dcfinder
} // namespace duckdb
