#pragma once

#include "duckdb.hpp"
#include "dcfinder/dc_parser.hpp"
#include <vector>

namespace duckdb {
namespace dcfinder {

//! A single violation: a pair of tuples that jointly violate a DC
struct Violation {
	idx_t tuple1_rowid;
	idx_t tuple2_rowid;
};

//! Find all tuple pairs that violate a parsed DC
struct ViolationDetector {
	static vector<Violation> FindViolations(const ParsedDC &dc, const vector<string> &col_names,
	                                        const vector<LogicalType> &col_types,
	                                        const vector<vector<Value>> &column_data, idx_t num_tuples);
};

} // namespace dcfinder
} // namespace duckdb
