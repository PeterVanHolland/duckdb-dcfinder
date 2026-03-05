#pragma once

#include "duckdb.hpp"
#include "dcfinder/predicate.hpp"
#include "dcfinder/evidence.hpp"
#include <vector>

namespace duckdb {
namespace dcfinder {

struct DenialConstraint {
	vector<idx_t> predicate_indices; // indices into PredicateSpace
	idx_t violation_count;
	double approximation; // g1 measure
	double succinctness;  // interestingness

	string ToString(const PredicateSpace &pred_space) const;
};

struct CoverSearch {
	//! Find all minimal (approximate) DCs
	static vector<DenialConstraint> FindMinimalDCs(
	    const EvidenceSet &evidence_set,
	    const PredicateSpace &pred_space,
	    double epsilon,
	    idx_t num_tuples);

private:
	static void FindCover(
	    vector<idx_t> &path,
	    vector<pair<EvidenceBitset, idx_t>> &uncovered,
	    vector<idx_t> &available_preds,
	    vector<DenialConstraint> &results,
	    const PredicateSpace &pred_space,
	    const EvidenceSet &evidence_set,
	    double epsilon,
	    idx_t num_tuples,
	    idx_t total_pairs);
};

} // namespace dcfinder
} // namespace duckdb
