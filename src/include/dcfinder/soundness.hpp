#pragma once

#include "duckdb.hpp"
#include "dcfinder/predicate.hpp"
#include "dcfinder/evidence.hpp"
#include "dcfinder/cover_search.hpp"
#include <vector>
#include <cmath>

namespace duckdb {
namespace dcfinder {

//! Implements the soundness rule from Martin et al. (PVLDB 2025):
//! "How and Why False Denial Constraints are Discovered"
//!
//! A DC is sound (atomic) if its predicates are genuinely dependent —
//! no subset of predicates can be removed without changing the behavior
//! of the remaining predicates. This filters out >95% of false DCs.
struct SoundnessChecker {
	//! Check if a DC is sound (atomic) — its predicates are genuinely dependent
	//! Uses Bayesian log-odds ratio test on predicate conditional probabilities
	static bool IsSound(const DenialConstraint &dc, const EvidenceSet &evidence_set);

	//! Filter a list of DCs, keeping only sound ones
	static vector<DenialConstraint> FilterSound(const vector<DenialConstraint> &dcs, const EvidenceSet &evidence_set);

private:
	//! Digamma function psi(x) for positive x
	//! Uses asymptotic expansion with recurrence relation
	static double Digamma(double x);

	//! Trigamma function psi_1(x) for positive x
	//! Uses asymptotic expansion with recurrence relation
	static double Trigamma(double x);
};

} // namespace dcfinder
} // namespace duckdb
