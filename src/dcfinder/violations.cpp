#include "dcfinder/violations.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {
namespace dcfinder {

static bool CompareValuesForViolation(const Value &left, const Value &right, PredicateOperator op) {
	if (left.IsNull() || right.IsNull()) {
		return false;
	}
	try {
		switch (op) {
		case PredicateOperator::EQ:
			return Value::NotDistinctFrom(left, right);
		case PredicateOperator::NEQ:
			return !Value::NotDistinctFrom(left, right);
		case PredicateOperator::LT:
			return left < right;
		case PredicateOperator::LEQ:
			return left <= right;
		case PredicateOperator::GT:
			return left > right;
		case PredicateOperator::GEQ:
			return left >= right;
		default:
			return false;
		}
	} catch (...) {
		return false;
	}
}

static int32_t FindColumn(const vector<string> &col_names, const string &name) {
	for (idx_t i = 0; i < col_names.size(); i++) {
		// Case-insensitive compare
		string lower_col = col_names[i];
		string lower_name = name;
		std::transform(lower_col.begin(), lower_col.end(), lower_col.begin(), ::tolower);
		std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
		if (lower_col == lower_name) {
			return static_cast<int32_t>(i);
		}
	}
	return -1;
}

vector<Violation> ViolationDetector::FindViolations(const ParsedDC &dc, const vector<string> &col_names,
                                                    const vector<LogicalType> &col_types,
                                                    const vector<vector<Value>> &column_data, idx_t num_tuples) {
	// Resolve column references
	struct ResolvedPred {
		idx_t left_col;
		idx_t right_col;
		bool left_is_t1;  // true if left references t1 (row x)
		bool right_is_t1; // true if right references t1 (row x)
		PredicateOperator op;
	};
	vector<ResolvedPred> resolved;

	for (auto &p : dc.predicates) {
		ResolvedPred rp;
		int32_t lcol = FindColumn(col_names, p.left_column);
		int32_t rcol = FindColumn(col_names, p.right_column);
		if (lcol < 0) {
			throw InvalidInputException("Column '%s' not found in table", p.left_column);
		}
		if (rcol < 0) {
			throw InvalidInputException("Column '%s' not found in table", p.right_column);
		}
		rp.left_col = static_cast<idx_t>(lcol);
		rp.right_col = static_cast<idx_t>(rcol);
		rp.left_is_t1 = (p.left_table == "t1");
		rp.right_is_t1 = (p.right_table == "t1");
		rp.op = p.op;
		resolved.push_back(rp);
	}

	vector<Violation> violations;

	// Check all ordered pairs
	for (idx_t x = 0; x < num_tuples; x++) {
		for (idx_t y = x + 1; y < num_tuples; y++) {
			// A violation occurs when ALL predicates are satisfied simultaneously
			// Check (x as t1, y as t2)
			bool all_satisfied_fwd = true;
			for (auto &rp : resolved) {
				idx_t left_row = rp.left_is_t1 ? x : y;
				idx_t right_row = rp.right_is_t1 ? x : y;
				const Value &left_val = column_data[rp.left_col][left_row];
				const Value &right_val = column_data[rp.right_col][right_row];
				if (!CompareValuesForViolation(left_val, right_val, rp.op)) {
					all_satisfied_fwd = false;
					break;
				}
			}
			if (all_satisfied_fwd) {
				violations.push_back({x, y});
			}

			// Check (y as t1, x as t2) — DCs are symmetric but we check both orderings
			bool all_satisfied_rev = true;
			for (auto &rp : resolved) {
				idx_t left_row = rp.left_is_t1 ? y : x;
				idx_t right_row = rp.right_is_t1 ? y : x;
				const Value &left_val = column_data[rp.left_col][left_row];
				const Value &right_val = column_data[rp.right_col][right_row];
				if (!CompareValuesForViolation(left_val, right_val, rp.op)) {
					all_satisfied_rev = false;
					break;
				}
			}
			if (all_satisfied_rev && !all_satisfied_fwd) {
				// Only add if the forward check didn't already catch it
				violations.push_back({y, x});
			}
		}
	}

	return violations;
}

} // namespace dcfinder
} // namespace duckdb
