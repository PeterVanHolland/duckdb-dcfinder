#include "dcfinder/pli.hpp"
#include "dcfinder/predicate.hpp"
#include <algorithm>
#include <map>

namespace duckdb {
namespace dcfinder {

void PLI::Build(const vector<Value> &column_data, bool numeric) {
	is_numeric = numeric;
	clusters.clear();

	// Group tuple IDs by value
	map<string, vector<idx_t>> groups;
	for (idx_t i = 0; i < column_data.size(); i++) {
		if (column_data[i].IsNull()) {
			continue;
		}
		groups[column_data[i].ToString()].push_back(i);
	}

	// Build clusters
	for (auto &kv : groups) {
		PLICluster cluster;
		for (idx_t i = 0; i < column_data.size(); i++) {
			if (!column_data[i].IsNull() && column_data[i].ToString() == kv.first) {
				cluster.key = column_data[i];
				break;
			}
		}
		cluster.tuple_ids = std::move(kv.second);
		std::sort(cluster.tuple_ids.begin(), cluster.tuple_ids.end());
		clusters.push_back(std::move(cluster));
	}

	if (numeric && !clusters.empty()) {
		std::sort(clusters.begin(), clusters.end(),
		          [](const PLICluster &a, const PLICluster &b) { return a.key > b.key; });
	}
}

const PLICluster *PLI::FindCluster(const Value &key) const {
	for (auto &c : clusters) {
		if (!c.key.IsNull() && !key.IsNull()) {
			try {
				if (Value::NotDistinctFrom(c.key, key)) {
					return &c;
				}
			} catch (...) {
				continue;
			}
		}
	}
	return nullptr;
}

void PLISet::Build(const vector<vector<Value>> &column_data, const vector<LogicalType> &types) {
	plis.clear();
	for (idx_t i = 0; i < column_data.size(); i++) {
		PLI pli;
		pli.Build(column_data[i], PredicateSpace::IsNumeric(types[i]));
		plis.push_back(std::move(pli));
	}
}

} // namespace dcfinder
} // namespace duckdb
