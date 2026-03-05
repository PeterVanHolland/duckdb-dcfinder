#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include <vector>
#include <string>

namespace duckdb {
namespace dcfinder {

enum class PredicateOperator : uint8_t {
	EQ = 0,  // =
	NEQ = 1, // !=
	LT = 2,  // <
	LEQ = 3, // <=
	GT = 4,  // >
	GEQ = 5  // >=
};

struct Predicate {
	idx_t left_attr;
	idx_t right_attr;
	PredicateOperator op;

	//! Returns the negation operator
	static PredicateOperator Negate(PredicateOperator op);
	//! Returns the implied operators (e.g., = implies <=, >=)
	static vector<PredicateOperator> Implications(PredicateOperator op);
	//! Check if two predicates are on the same attributes
	bool SameAttributes(const Predicate &other) const;
	//! Human-readable string
	string ToString(const vector<string> &column_names) const;
	static string OperatorToString(PredicateOperator op);
};

struct PredicateSpace {
	vector<Predicate> predicates;
	vector<string> column_names;
	vector<LogicalType> column_types;
	idx_t num_columns;

	//! Build predicate space from schema
	void Build(const vector<string> &col_names, const vector<LogicalType> &col_types,
	           const vector<vector<Value>> &column_data);

	//! Check if an attribute type is numeric
	static bool IsNumeric(const LogicalType &type);

	//! Get the index of a predicate matching (left, right, op)
	int32_t FindPredicate(idx_t left, idx_t right, PredicateOperator op) const;
};

} // namespace dcfinder
} // namespace duckdb
