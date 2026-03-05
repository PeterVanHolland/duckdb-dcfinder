#include "dcfinder/evidence.hpp"
#include <algorithm>

namespace duckdb {
namespace dcfinder {

EvidenceBitset::EvidenceBitset(idx_t num_predicates) {
	idx_t num_words = (num_predicates + 63) / 64;
	words.resize(num_words, 0ULL);
}

void EvidenceBitset::SetBit(idx_t pos) {
	idx_t word_idx = pos / 64;
	idx_t bit_idx = pos % 64;
	if (word_idx < words.size()) {
		words[word_idx] |= (1ULL << bit_idx);
	}
}

void EvidenceBitset::ClearBit(idx_t pos) {
	idx_t word_idx = pos / 64;
	idx_t bit_idx = pos % 64;
	if (word_idx < words.size()) {
		words[word_idx] &= ~(1ULL << bit_idx);
	}
}

bool EvidenceBitset::GetBit(idx_t pos) const {
	idx_t word_idx = pos / 64;
	idx_t bit_idx = pos % 64;
	if (word_idx < words.size()) {
		return (words[word_idx] >> bit_idx) & 1ULL;
	}
	return false;
}

void EvidenceBitset::XorWith(const EvidenceBitset &other) {
	for (idx_t i = 0; i < words.size() && i < other.words.size(); i++) {
		words[i] ^= other.words[i];
	}
}

bool EvidenceBitset::operator==(const EvidenceBitset &other) const {
	if (words.size() != other.words.size())
		return false;
	for (idx_t i = 0; i < words.size(); i++) {
		if (words[i] != other.words[i])
			return false;
	}
	return true;
}

bool EvidenceBitset::Intersects(const EvidenceBitset &other) const {
	for (idx_t i = 0; i < words.size() && i < other.words.size(); i++) {
		if (words[i] & other.words[i])
			return true;
	}
	return false;
}

size_t EvidenceBitset::Hash::operator()(const EvidenceBitset &bs) const {
	size_t h = 0;
	for (auto w : bs.words) {
		h ^= std::hash<uint64_t>()(w) + 0x9e3779b9 + (h << 6) + (h >> 2);
	}
	return h;
}

static bool CompareValues(const Value &left, const Value &right, PredicateOperator op) {
	if (left.IsNull() || right.IsNull())
		return false;
	try {
		switch (op) {
		case PredicateOperator::EQ:
			return Value::NotDistinctFrom(left, right);
		case PredicateOperator::NEQ:
			return !Value::NotDistinctFrom(left, right);
		case PredicateOperator::LT:
			return left < right;
		case PredicateOperator::LEQ:
			return left <= right;
		case PredicateOperator::GT:
			return left > right;
		case PredicateOperator::GEQ:
			return left >= right;
		default:
			return false;
		}
	} catch (...) {
		return false;
	}
}

void EvidenceSet::Build(const PredicateSpace &pred_space, const PLISet &pli_set,
                        const vector<vector<Value>> &column_data, idx_t num_tuples) {
	evidences.clear();
	total_pairs = num_tuples * (num_tuples - 1);

	if (num_tuples <= 1)
		return;

	idx_t num_preds = pred_space.predicates.size();

	for (idx_t x = 0; x < num_tuples; x++) {
		for (idx_t y = 0; y < num_tuples; y++) {
			if (x == y)
				continue;

			EvidenceBitset evidence(num_preds);

			for (idx_t p = 0; p < num_preds; p++) {
				auto &pred = pred_space.predicates[p];
				const Value &left_val = column_data[pred.left_attr][x];
				const Value &right_val = column_data[pred.right_attr][y];

				if (CompareValues(left_val, right_val, pred.op)) {
					evidence.SetBit(p);
				}
			}

			evidences[evidence]++;
		}
	}
}

} // namespace dcfinder
} // namespace duckdb
