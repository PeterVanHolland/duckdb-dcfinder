#pragma once

#include "duckdb.hpp"
#include <vector>
#include <unordered_map>

namespace duckdb {
namespace dcfinder {

struct PLICluster {
	Value key;
	vector<idx_t> tuple_ids; // sorted ascending
};

struct PLI {
	vector<PLICluster> clusters;
	bool is_numeric;

	//! Build PLI from column data
	void Build(const vector<Value> &column_data, bool numeric);

	//! Find cluster by key (for probing)
	const PLICluster *FindCluster(const Value &key) const;
};

struct PLISet {
	vector<PLI> plis; // one per attribute

	//! Build all PLIs from column data
	void Build(const vector<vector<Value>> &column_data, const vector<LogicalType> &types);
};

} // namespace dcfinder
} // namespace duckdb
