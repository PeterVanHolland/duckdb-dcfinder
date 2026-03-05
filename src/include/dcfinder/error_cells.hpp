#pragma once

#include "duckdb.hpp"
#include "dcfinder/dc_parser.hpp"
#include "dcfinder/violations.hpp"
#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {
namespace dcfinder {

//! A cell identified as potentially erroneous
struct ErrorCell {
	idx_t row_id;
	idx_t col_id;
	idx_t violation_count;   //! Number of violations this cell participates in
	double error_likelihood; //! Score: violations / max_possible_violations
};

//! Detects likely error cells from DC violations using conflict graph analysis.
//! Based on Chu et al. "Holistic Data Cleaning: Putting Violations into Context" (ICDE 2013).
//!
//! The key insight: each violation involves a set of cells (the cells referenced by the
//! DC predicates). At least one cell in each violation must be wrong. Cells that
//! participate in many violations are more likely to be erroneous.
struct ErrorCellDetector {
	//! Find cells that are likely erroneous based on violation participation
	//! Uses greedy weighted minimum vertex cover on the conflict hypergraph
	static vector<ErrorCell> FindErrorCells(const vector<Violation> &violations, const ParsedDC &dc,
	                                        const vector<string> &col_names, const vector<vector<Value>> &column_data,
	                                        idx_t num_tuples);
};

} // namespace dcfinder
} // namespace duckdb
