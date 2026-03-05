#include "dcfinder/error_cells.hpp"
#include <algorithm>
#include <set>

namespace duckdb {
namespace dcfinder {

//! Encode (row, col) as a single 64-bit key
static uint64_t CellKey(idx_t row, idx_t col) {
	return (static_cast<uint64_t>(row) << 32) | static_cast<uint64_t>(col);
}

static idx_t CellRow(uint64_t key) {
	return static_cast<idx_t>(key >> 32);
}

static idx_t CellCol(uint64_t key) {
	return static_cast<idx_t>(key & 0xFFFFFFFF);
}

vector<ErrorCell> ErrorCellDetector::FindErrorCells(const vector<Violation> &violations, const ParsedDC &dc,
                                                    const vector<string> &col_names,
                                                    const vector<vector<Value>> &column_data, idx_t num_tuples) {
	if (violations.empty()) {
		return {};
	}

	// Resolve column names from the DC predicates
	auto find_col = [&](const string &name) -> idx_t {
		string lower_name = name;
		std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
		for (idx_t i = 0; i < col_names.size(); i++) {
			string lower_col = col_names[i];
			std::transform(lower_col.begin(), lower_col.end(), lower_col.begin(), ::tolower);
			if (lower_col == lower_name) {
				return i;
			}
		}
		return 0;
	};

	// Build conflict hypergraph:
	// Each violation is a hyperedge connecting the cells involved in its predicates.
	// A cell (row_id, col_id) participates in a violation if it is referenced by
	// one of the DC's predicates for either tuple in the violating pair.
	//
	// cell_violations[cell_key] = set of violation indices
	unordered_map<uint64_t, vector<idx_t>> cell_violations;

	for (idx_t v = 0; v < violations.size(); v++) {
		idx_t t1_row = violations[v].tuple1_rowid;
		idx_t t2_row = violations[v].tuple2_rowid;

		for (auto &pred : dc.predicates) {
			idx_t left_col = find_col(pred.left_column);
			idx_t right_col = find_col(pred.right_column);
			idx_t left_row = (pred.left_table == "t1") ? t1_row : t2_row;
			idx_t right_row = (pred.right_table == "t1") ? t1_row : t2_row;

			cell_violations[CellKey(left_row, left_col)].push_back(v);
			cell_violations[CellKey(right_row, right_col)].push_back(v);
		}
	}

	// Greedy weighted minimum vertex cover:
	// Repeatedly pick the cell with the most uncovered violations,
	// mark those violations as covered, and add the cell to the result.
	// This approximates the minimum set of cells to fix.
	vector<bool> covered(violations.size(), false);
	idx_t uncovered_count = violations.size();
	vector<ErrorCell> result;

	while (uncovered_count > 0) {
		// Find cell with most uncovered violations
		uint64_t best_cell = 0;
		idx_t best_count = 0;

		for (auto &kv : cell_violations) {
			idx_t count = 0;
			for (auto vi : kv.second) {
				if (!covered[vi]) {
					count++;
				}
			}
			if (count > best_count) {
				best_count = count;
				best_cell = kv.first;
			}
		}

		if (best_count == 0) {
			break;
		}

		// Mark violations as covered
		for (auto vi : cell_violations[best_cell]) {
			if (!covered[vi]) {
				covered[vi] = true;
				uncovered_count--;
			}
		}

		// Count total violations for this cell (not just uncovered)
		idx_t total_cell_violations = cell_violations[best_cell].size();

		ErrorCell ec;
		ec.row_id = CellRow(best_cell);
		ec.col_id = CellCol(best_cell);
		ec.violation_count = total_cell_violations;
		ec.error_likelihood = static_cast<double>(total_cell_violations) / static_cast<double>(violations.size());
		result.push_back(ec);
	}

	// Sort by violation count descending
	std::sort(result.begin(), result.end(),
	          [](const ErrorCell &a, const ErrorCell &b) { return a.violation_count > b.violation_count; });

	return result;
}

} // namespace dcfinder
} // namespace duckdb
