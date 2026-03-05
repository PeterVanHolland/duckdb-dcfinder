#pragma once

#include "duckdb.hpp"
#include "dcfinder/predicate.hpp"
#include "dcfinder/pli.hpp"
#include <vector>
#include <unordered_map>

namespace duckdb {
namespace dcfinder {

//! Bitset evidence representation using uint64_t words
struct EvidenceBitset {
	vector<uint64_t> words;

	EvidenceBitset() {
	}
	explicit EvidenceBitset(idx_t num_predicates);

	void SetBit(idx_t pos);
	void ClearBit(idx_t pos);
	bool GetBit(idx_t pos) const;
	void XorWith(const EvidenceBitset &other);

	bool operator==(const EvidenceBitset &other) const;
	bool Intersects(const EvidenceBitset &other) const;

	struct Hash {
		size_t operator()(const EvidenceBitset &bs) const;
	};
};

struct EvidenceSet {
	//! Map from evidence -> count (multiplicity)
	unordered_map<EvidenceBitset, idx_t, EvidenceBitset::Hash> evidences;
	idx_t total_pairs;

	//! Build evidence set from data
	void Build(const PredicateSpace &pred_space, const PLISet &pli_set, const vector<vector<Value>> &column_data,
	           idx_t num_tuples);
};

} // namespace dcfinder
} // namespace duckdb
