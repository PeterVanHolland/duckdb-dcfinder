#pragma once

#include "duckdb.hpp"
#include "dcfinder/dc_parser.hpp"
#include "dcfinder/violations.hpp"
#include "dcfinder/error_cells.hpp"
#include <vector>

namespace duckdb {
namespace dcfinder {

//! A suggested repair for an error cell
struct RepairSuggestion {
	idx_t row_id;
	idx_t col_id;
	Value current_value;
	Value suggested_value;
	string repair_type; //! "most_frequent", "median", "boundary"
	double confidence;
};

//! Suggests minimal-cost repairs for DC violations.
//! Based on the minimum-cost repair principle from Holistic Data Cleaning
//! (Chu, Ilyas, Papotti — ICDE 2013):
//!
//! For equality predicates (FD violations):
//!   Use the most frequent value in the equivalence class — changing the
//!   minority values to the majority value minimizes total changes.
//!
//! For inequality predicates (order violations):
//!   Suggest boundary values that restore the ordering constraint.
struct RepairGenerator {
	static vector<RepairSuggestion> SuggestRepairs(const vector<Violation> &violations,
	                                               const vector<ErrorCell> &error_cells, const ParsedDC &dc,
	                                               const vector<string> &col_names,
	                                               const vector<LogicalType> &col_types,
	                                               const vector<vector<Value>> &column_data, idx_t num_tuples);
};

} // namespace dcfinder
} // namespace duckdb
