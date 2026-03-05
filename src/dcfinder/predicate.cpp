#include "dcfinder/predicate.hpp"
#include <unordered_set>

namespace duckdb {
namespace dcfinder {

PredicateOperator Predicate::Negate(PredicateOperator op) {
	switch (op) {
	case PredicateOperator::EQ:  return PredicateOperator::NEQ;
	case PredicateOperator::NEQ: return PredicateOperator::EQ;
	case PredicateOperator::LT:  return PredicateOperator::GEQ;
	case PredicateOperator::LEQ: return PredicateOperator::GT;
	case PredicateOperator::GT:  return PredicateOperator::LEQ;
	case PredicateOperator::GEQ: return PredicateOperator::LT;
	default: return PredicateOperator::EQ;
	}
}

vector<PredicateOperator> Predicate::Implications(PredicateOperator op) {
	switch (op) {
	case PredicateOperator::EQ:  return {PredicateOperator::EQ, PredicateOperator::LEQ, PredicateOperator::GEQ};
	case PredicateOperator::NEQ: return {PredicateOperator::NEQ};
	case PredicateOperator::LT:  return {PredicateOperator::LT, PredicateOperator::LEQ, PredicateOperator::NEQ};
	case PredicateOperator::LEQ: return {PredicateOperator::LEQ};
	case PredicateOperator::GT:  return {PredicateOperator::GT, PredicateOperator::GEQ, PredicateOperator::NEQ};
	case PredicateOperator::GEQ: return {PredicateOperator::GEQ};
	default: return {};
	}
}

bool Predicate::SameAttributes(const Predicate &other) const {
	return left_attr == other.left_attr && right_attr == other.right_attr;
}

string Predicate::OperatorToString(PredicateOperator op) {
	switch (op) {
	case PredicateOperator::EQ:  return "=";
	case PredicateOperator::NEQ: return "!=";
	case PredicateOperator::LT:  return "<";
	case PredicateOperator::LEQ: return "<=";
	case PredicateOperator::GT:  return ">";
	case PredicateOperator::GEQ: return ">=";
	default: return "?";
	}
}

string Predicate::ToString(const vector<string> &column_names) const {
	return "t1." + column_names[left_attr] + " " + OperatorToString(op) + " t2." + column_names[right_attr];
}

bool PredicateSpace::IsNumeric(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::HUGEINT:
		return true;
	default:
		return false;
	}
}

void PredicateSpace::Build(const vector<string> &col_names, const vector<LogicalType> &col_types,
                           const vector<vector<Value>> &column_data) {
	column_names = col_names;
	column_types = col_types;
	num_columns = col_names.size();
	predicates.clear();

	// Same-attribute predicates
	for (idx_t i = 0; i < num_columns; i++) {
		bool numeric = IsNumeric(col_types[i]);
		// Always add = and !=
		predicates.push_back({i, i, PredicateOperator::EQ});
		predicates.push_back({i, i, PredicateOperator::NEQ});
		if (numeric) {
			predicates.push_back({i, i, PredicateOperator::LT});
			predicates.push_back({i, i, PredicateOperator::LEQ});
			predicates.push_back({i, i, PredicateOperator::GT});
			predicates.push_back({i, i, PredicateOperator::GEQ});
		}
	}

	// Cross-attribute predicates: only if same type and >= 30% value overlap
	for (idx_t i = 0; i < num_columns; i++) {
		for (idx_t j = i + 1; j < num_columns; j++) {
			if (col_types[i].id() != col_types[j].id()) {
				continue;
			}
			// Check value overlap
			unordered_set<string> vals_i, vals_j;
			for (auto &v : column_data[i]) {
				if (!v.IsNull()) vals_i.insert(v.ToString());
			}
			for (auto &v : column_data[j]) {
				if (!v.IsNull()) vals_j.insert(v.ToString());
			}
			idx_t overlap = 0;
			for (auto &v : vals_i) {
				if (vals_j.count(v)) overlap++;
			}
			double ratio = 0;
			if (!vals_i.empty() && !vals_j.empty()) {
				ratio = (double)overlap / (double)std::min(vals_i.size(), vals_j.size());
			}
			if (ratio < 0.3) {
				continue;
			}

			bool numeric = IsNumeric(col_types[i]);
			predicates.push_back({i, j, PredicateOperator::EQ});
			predicates.push_back({i, j, PredicateOperator::NEQ});
			if (numeric) {
				predicates.push_back({i, j, PredicateOperator::LT});
				predicates.push_back({i, j, PredicateOperator::LEQ});
				predicates.push_back({i, j, PredicateOperator::GT});
				predicates.push_back({i, j, PredicateOperator::GEQ});
			}
		}
	}
}

int32_t PredicateSpace::FindPredicate(idx_t left, idx_t right, PredicateOperator op) const {
	for (idx_t i = 0; i < predicates.size(); i++) {
		if (predicates[i].left_attr == left && predicates[i].right_attr == right && predicates[i].op == op) {
			return (int32_t)i;
		}
	}
	return -1;
}

} // namespace dcfinder
} // namespace duckdb
