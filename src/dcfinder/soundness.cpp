#include "dcfinder/soundness.hpp"
#include <algorithm>
#include <cmath>

namespace duckdb {
namespace dcfinder {

double SoundnessChecker::Digamma(double x) {
	// Use recurrence psi(x+1) = psi(x) + 1/x to shift x to a large value
	double result = 0.0;
	while (x < 8.0) {
		result -= 1.0 / x;
		x += 1.0;
	}
	// Asymptotic expansion for large x:
	// psi(x) ~ ln(x) - 1/(2x) - 1/(12x^2) + 1/(120x^4) - 1/(252x^6)
	double inv_x = 1.0 / x;
	double inv_x2 = inv_x * inv_x;
	result += std::log(x) - 0.5 * inv_x - inv_x2 * (1.0 / 12.0 - inv_x2 * (1.0 / 120.0 - inv_x2 * (1.0 / 252.0)));
	return result;
}

double SoundnessChecker::Trigamma(double x) {
	// Use recurrence psi_1(x+1) = psi_1(x) - 1/x^2 to shift x to a large value
	double result = 0.0;
	while (x < 8.0) {
		result += 1.0 / (x * x);
		x += 1.0;
	}
	// Asymptotic expansion for large x:
	// psi_1(x) ~ 1/x + 1/(2x^2) + 1/(6x^3) - 1/(30x^5) + 1/(42x^7)
	double inv_x = 1.0 / x;
	double inv_x2 = inv_x * inv_x;
	result += inv_x + 0.5 * inv_x2 + inv_x2 * inv_x * (1.0 / 6.0 - inv_x2 * (1.0 / 30.0 - inv_x2 * (1.0 / 42.0)));
	return result;
}

bool SoundnessChecker::IsSound(const DenialConstraint &dc, const EvidenceSet &evidence_set) {
	auto &indices = dc.predicate_indices;
	idx_t k = indices.size();

	// Single-predicate DCs are sound by definition (nothing to be independent of)
	if (k <= 1) {
		return true;
	}

	// Precompute counts for all 2^k subsets of DC predicates.
	// subset_counts[mask] = number of tuple pairs where ALL DC predicates
	// indicated by the mask are satisfied.
	//
	// DC predicates are negations of cover predicates, so a DC predicate
	// is satisfied when the corresponding cover predicate is NOT satisfied
	// (bit NOT set in evidence).
	idx_t num_subsets = 1ULL << k;
	vector<idx_t> subset_counts(num_subsets, 0);

	for (auto &kv : evidence_set.evidences) {
		auto &evidence = kv.first;
		idx_t mult = kv.second;

		// Determine which of the DC's cover predicates are satisfied in this evidence
		idx_t dc_cover_bits = 0;
		for (idx_t j = 0; j < k; j++) {
			if (evidence.GetBit(indices[j])) {
				dc_cover_bits |= (1ULL << j);
			}
		}

		// DC predicates satisfied = complement of cover predicates satisfied
		idx_t dc_pred_satisfied = (num_subsets - 1) & ~dc_cover_bits;

		// This evidence contributes to subset_counts[S] for all S that are
		// subsets of dc_pred_satisfied (all requested DC preds must be satisfied)
		for (idx_t s = dc_pred_satisfied;; s = (s - 1) & dc_pred_satisfied) {
			subset_counts[s] += mult;
			if (s == 0) {
				break;
			}
		}
	}

	idx_t full_mask = num_subsets - 1;

	// For each predicate in the DC, verify it is statistically dependent
	// on the remaining predicates (compared to every proper subset of the rest).
	//
	// From Martin et al.: we model p(X|Y,Z,...) as Beta(a+1, b+1) and compute
	// the expected log-odds ratio. If u + 2s > 0, the predicate is independent
	// of the removed predicates -> NOT sound.
	for (idx_t i = 0; i < k; i++) {
		idx_t pred_bit = 1ULL << i;
		idx_t others_mask = full_mask & ~pred_bit;

		// Full conditioning: p(pred_i | all others)
		// a1 = count(all DC preds satisfied) = subset_counts[full_mask]
		// b1 = count(all others satisfied but NOT pred_i) = subset_counts[others_mask] - a1
		idx_t a1 = subset_counts[full_mask];
		idx_t b1 = subset_counts[others_mask] - a1;

		// Check against every PROPER subset of others_mask
		for (idx_t s = 0; s <= others_mask; s++) {
			// Skip non-subsets and the full set (proper subsets only)
			if (s == others_mask) {
				continue;
			}
			if ((s & others_mask) != s) {
				continue;
			}

			// Reduced conditioning: p(pred_i | subset S)
			idx_t s_plus_pred = s | pred_bit;
			idx_t a2 = subset_counts[s_plus_pred];
			idx_t b2 = subset_counts[s] - a2;

			// Bayesian log-odds ratio test
			// u = E[log(p1/(1-p1))] - E[log(p2/(1-p2))]
			// s_val = SD of the difference
			// If u + 2*s_val > 0: pred_i is NOT more concentrated given all others
			//   vs given subset S -> independent -> NOT sound
			double u = Digamma(static_cast<double>(a1) + 1.0) - Digamma(static_cast<double>(b1) + 1.0) -
			           Digamma(static_cast<double>(a2) + 1.0) + Digamma(static_cast<double>(b2) + 1.0);

			double s_val = std::sqrt(Trigamma(static_cast<double>(a1) + 1.0) + Trigamma(static_cast<double>(b1) + 1.0) +
			                         Trigamma(static_cast<double>(a2) + 1.0) + Trigamma(static_cast<double>(b2) + 1.0));

			if (u + 2.0 * s_val > 0.0) {
				return false; // Independent predicate found -> NOT sound
			}
		}
	}

	return true; // All predicates are genuinely dependent -> sound
}

vector<DenialConstraint> SoundnessChecker::FilterSound(const vector<DenialConstraint> &dcs,
                                                       const EvidenceSet &evidence_set) {
	vector<DenialConstraint> sound_dcs;
	for (auto &dc : dcs) {
		if (IsSound(dc, evidence_set)) {
			sound_dcs.push_back(dc);
		}
	}
	return sound_dcs;
}

} // namespace dcfinder
} // namespace duckdb
