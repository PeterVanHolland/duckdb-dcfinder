#include "dcfinder/cover_search.hpp"
#include <algorithm>
#include <numeric>

namespace duckdb {
namespace dcfinder {

string DenialConstraint::ToString(const PredicateSpace &pred_space) const {
	string result = "NOT(";
	for (idx_t i = 0; i < predicate_indices.size(); i++) {
		if (i > 0)
			result += " AND ";
		auto &pred = pred_space.predicates[predicate_indices[i]];
		// The cover contains predicates that must be TRUE in every evidence.
		// The DC says: the NEGATION of these predicates cannot all be true simultaneously.
		// So DC predicates are the COMPLEMENT of the cover predicates.
		PredicateOperator negated_op = Predicate::Negate(pred.op);
		result += "t1." + pred_space.column_names[pred.left_attr] + " " + Predicate::OperatorToString(negated_op) +
		          " t2." + pred_space.column_names[pred.right_attr];
	}
	result += ")";
	return result;
}

static idx_t CountCoverage(idx_t pred_idx, const vector<pair<EvidenceBitset, idx_t>> &uncovered) {
	idx_t count = 0;
	for (auto &ev : uncovered) {
		if (ev.first.GetBit(pred_idx)) {
			count += ev.second;
		}
	}
	return count;
}

static idx_t TotalMultiplicity(const vector<pair<EvidenceBitset, idx_t>> &uncovered) {
	idx_t total = 0;
	for (auto &ev : uncovered) {
		total += ev.second;
	}
	return total;
}

static bool IsImpliedByResults(const vector<idx_t> &path, const vector<DenialConstraint> &results) {
	for (auto &dc : results) {
		bool is_subset = true;
		for (auto &pi : dc.predicate_indices) {
			bool found = false;
			for (auto &pj : path) {
				if (pi == pj) {
					found = true;
					break;
				}
			}
			if (!found) {
				is_subset = false;
				break;
			}
		}
		if (is_subset)
			return true;
	}
	return false;
}

static const idx_t MAX_DC_SIZE = 6;

void CoverSearch::FindCover(vector<idx_t> &path, vector<pair<EvidenceBitset, idx_t>> &uncovered,
                            vector<idx_t> &available_preds, vector<DenialConstraint> &results,
                            const PredicateSpace &pred_space, const EvidenceSet &evidence_set, double epsilon,
                            idx_t num_tuples, idx_t total_pairs) {

	idx_t uncovered_mult = TotalMultiplicity(uncovered);

	if ((double)uncovered_mult <= epsilon * (double)total_pairs) {
		if (path.empty())
			return;

		bool has_smaller_cover = false;
		if (path.size() > 1) {
			for (idx_t skip = 0; skip < path.size(); skip++) {
				idx_t subset_uncovered = 0;
				for (auto &ev_pair : evidence_set.evidences) {
					bool covered = false;
					for (idx_t i = 0; i < path.size(); i++) {
						if (i == skip)
							continue;
						if (ev_pair.first.GetBit(path[i])) {
							covered = true;
							break;
						}
					}
					if (!covered) {
						subset_uncovered += ev_pair.second;
					}
				}
				if ((double)subset_uncovered <= epsilon * (double)total_pairs) {
					has_smaller_cover = true;
					break;
				}
			}
		}

		if (!has_smaller_cover) {
			DenialConstraint dc;
			dc.predicate_indices = path;
			std::sort(dc.predicate_indices.begin(), dc.predicate_indices.end());
			dc.violation_count = uncovered_mult;
			dc.approximation = total_pairs > 0 ? (double)uncovered_mult / (double)total_pairs : 0.0;
			dc.succinctness = 1.0 / (double)path.size();
			results.push_back(dc);
		}
		return;
	}

	if (available_preds.empty() || path.size() >= MAX_DC_SIZE) {
		return;
	}

	std::sort(available_preds.begin(), available_preds.end(),
	          [&](idx_t a, idx_t b) { return CountCoverage(a, uncovered) > CountCoverage(b, uncovered); });

	if (CountCoverage(available_preds[0], uncovered) == 0) {
		return;
	}

	vector<idx_t> remaining_preds = available_preds;
	for (idx_t i = 0; i < remaining_preds.size(); i++) {
		idx_t padd = remaining_preds[i];

		if (CountCoverage(padd, uncovered) == 0) {
			break;
		}

		path.push_back(padd);

		if (IsImpliedByResults(path, results)) {
			path.pop_back();
			continue;
		}

		vector<pair<EvidenceBitset, idx_t>> new_uncovered;
		for (auto &ev : uncovered) {
			if (!ev.first.GetBit(padd)) {
				new_uncovered.push_back(ev);
			}
		}

		vector<idx_t> new_available;
		for (idx_t j = i + 1; j < remaining_preds.size(); j++) {
			idx_t pidx = remaining_preds[j];
			if (!pred_space.predicates[pidx].SameAttributes(pred_space.predicates[padd])) {
				new_available.push_back(pidx);
			}
		}

		FindCover(path, new_uncovered, new_available, results, pred_space, evidence_set, epsilon, num_tuples,
		          total_pairs);

		path.pop_back();
	}
}

vector<DenialConstraint> CoverSearch::FindMinimalDCs(const EvidenceSet &evidence_set, const PredicateSpace &pred_space,
                                                     double epsilon, idx_t num_tuples) {

	vector<DenialConstraint> results;
	idx_t total_pairs = num_tuples * (num_tuples - 1);

	if (evidence_set.evidences.empty() || num_tuples <= 1) {
		return results;
	}

	vector<pair<EvidenceBitset, idx_t>> uncovered;
	for (auto &kv : evidence_set.evidences) {
		uncovered.push_back({kv.first, kv.second});
	}

	vector<idx_t> available;
	for (idx_t i = 0; i < pred_space.predicates.size(); i++) {
		available.push_back(i);
	}

	vector<idx_t> path;
	FindCover(path, uncovered, available, results, pred_space, evidence_set, epsilon, num_tuples, total_pairs);

	std::sort(results.begin(), results.end(), [](const DenialConstraint &a, const DenialConstraint &b) {
		if (a.predicate_indices.size() != b.predicate_indices.size()) {
			return a.predicate_indices.size() < b.predicate_indices.size();
		}
		return a.approximation < b.approximation;
	});

	return results;
}

} // namespace dcfinder
} // namespace duckdb
