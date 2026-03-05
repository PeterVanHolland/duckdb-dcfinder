#include "dcfinder/repairs.hpp"
#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {
namespace dcfinder {

static int32_t FindColumnIdx(const vector<string> &col_names, const string &name) {
	string lower_name = name;
	std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
	for (idx_t i = 0; i < col_names.size(); i++) {
		string lower_col = col_names[i];
		std::transform(lower_col.begin(), lower_col.end(), lower_col.begin(), ::tolower);
		if (lower_col == lower_name) {
			return static_cast<int32_t>(i);
		}
	}
	return -1;
}

//! For an error cell involved in FD-like violations (equality predicates),
//! find the most frequent value among the "partner" rows and suggest it.
static Value FindMostFrequentPartnerValue(idx_t error_row, idx_t error_col, idx_t partner_col,
                                          const vector<Violation> &violations, const ParsedDC &dc,
                                          const vector<string> &col_names, const vector<vector<Value>> &column_data) {
	// Collect partner values: for each violation involving error_row,
	// get the value from the other tuple's corresponding column
	unordered_map<string, idx_t> value_counts;
	Value most_frequent;
	idx_t max_count = 0;

	for (auto &v : violations) {
		idx_t other_row = static_cast<idx_t>(-1);
		if (v.tuple1_rowid == error_row) {
			other_row = v.tuple2_rowid;
		} else if (v.tuple2_rowid == error_row) {
			other_row = v.tuple1_rowid;
		}
		if (other_row == static_cast<idx_t>(-1)) {
			continue;
		}

		// Get the partner's value for the error column
		const Value &partner_val = column_data[error_col][other_row];
		if (partner_val.IsNull()) {
			continue;
		}
		string key = partner_val.ToString();
		value_counts[key]++;
		if (value_counts[key] > max_count) {
			max_count = value_counts[key];
			most_frequent = partner_val;
		}
	}

	// Also count the current column's value across ALL rows that share
	// the same value in the determinant column (for FD X -> Y)
	// This gives better repair suggestions for FD violations
	if (partner_col != static_cast<idx_t>(-1) && partner_col < column_data.size()) {
		const Value &error_partner_val = column_data[partner_col][error_row];
		if (!error_partner_val.IsNull()) {
			unordered_map<string, idx_t> group_counts;
			Value group_most_frequent;
			idx_t group_max = 0;

			for (idx_t r = 0; r < column_data[partner_col].size(); r++) {
				const Value &pv = column_data[partner_col][r];
				if (!pv.IsNull() && Value::NotDistinctFrom(pv, error_partner_val)) {
					const Value &cv = column_data[error_col][r];
					if (!cv.IsNull()) {
						string k = cv.ToString();
						group_counts[k]++;
						if (group_counts[k] > group_max) {
							group_max = group_counts[k];
							group_most_frequent = cv;
						}
					}
				}
			}

			if (group_max > 0) {
				return group_most_frequent;
			}
		}
	}

	return most_frequent;
}

vector<RepairSuggestion> RepairGenerator::SuggestRepairs(const vector<Violation> &violations,
                                                         const vector<ErrorCell> &error_cells, const ParsedDC &dc,
                                                         const vector<string> &col_names,
                                                         const vector<LogicalType> &col_types,
                                                         const vector<vector<Value>> &column_data, idx_t num_tuples) {
	vector<RepairSuggestion> suggestions;

	if (violations.empty() || error_cells.empty()) {
		return suggestions;
	}

	// Classify the DC's predicates to determine repair strategy
	// For FD violations (EQ + NEQ pattern): use most_frequent
	// For order violations: use boundary values
	bool has_eq = false;
	bool has_ineq = false;
	idx_t eq_determinant_col = static_cast<idx_t>(-1); // The "X" in X -> Y

	for (auto &pred : dc.predicates) {
		if (pred.op == PredicateOperator::EQ) {
			has_eq = true;
			int32_t col = FindColumnIdx(col_names, pred.left_column);
			if (col >= 0) {
				eq_determinant_col = static_cast<idx_t>(col);
			}
		}
		if (pred.op == PredicateOperator::LT || pred.op == PredicateOperator::LEQ || pred.op == PredicateOperator::GT ||
		    pred.op == PredicateOperator::GEQ || pred.op == PredicateOperator::NEQ) {
			has_ineq = true;
		}
	}

	for (auto &ec : error_cells) {
		RepairSuggestion suggestion;
		suggestion.row_id = ec.row_id;
		suggestion.col_id = ec.col_id;
		suggestion.current_value = column_data[ec.col_id][ec.row_id];
		suggestion.confidence = ec.error_likelihood;

		if (has_eq) {
			// FD-like: suggest the most frequent value
			suggestion.suggested_value = FindMostFrequentPartnerValue(ec.row_id, ec.col_id, eq_determinant_col,
			                                                          violations, dc, col_names, column_data);
			suggestion.repair_type = "most_frequent";
		} else {
			// Order violation: suggest a boundary value
			// For the error cell, look at the constraint partner values
			// and suggest the min/max that would satisfy the constraint
			Value boundary;
			for (auto &v : violations) {
				idx_t other_row = static_cast<idx_t>(-1);
				if (v.tuple1_rowid == ec.row_id) {
					other_row = v.tuple2_rowid;
				} else if (v.tuple2_rowid == ec.row_id) {
					other_row = v.tuple1_rowid;
				}
				if (other_row == static_cast<idx_t>(-1)) {
					continue;
				}
				const Value &other_val = column_data[ec.col_id][other_row];
				if (!other_val.IsNull()) {
					if (boundary.IsNull() || other_val < boundary) {
						boundary = other_val;
					}
				}
			}
			suggestion.suggested_value = boundary;
			suggestion.repair_type = "boundary";
		}

		if (!suggestion.suggested_value.IsNull()) {
			suggestions.push_back(std::move(suggestion));
		}
	}

	return suggestions;
}

} // namespace dcfinder
} // namespace duckdb
