#include "dcfinder/profiler.hpp"

namespace duckdb {
namespace dcfinder {

vector<ClassifiedConstraint> DCProfiler::Classify(const vector<DenialConstraint> &dcs, const PredicateSpace &pred_space,
                                                  const vector<bool> &soundness) {
	vector<ClassifiedConstraint> result;

	for (idx_t i = 0; i < dcs.size(); i++) {
		auto &dc = dcs[i];
		ClassifiedConstraint cc;
		cc.dc_text = dc.ToString(pred_space);
		cc.num_predicates = dc.predicate_indices.size();
		cc.is_sound = i < soundness.size() ? soundness[i] : true;

		if (dc.predicate_indices.size() == 1) {
			// Single predicate: UCC (unique column combination)
			// DC is NOT(neg(p)), and cover predicate p has NEQ or EQ
			auto &pred = pred_space.predicates[dc.predicate_indices[0]];
			// After negation in the DC: if cover is NEQ, DC shows EQ
			// NOT(t1.A = t2.A) means A is unique
			PredicateOperator negated_op = Predicate::Negate(pred.op);
			if (negated_op == PredicateOperator::EQ && pred.left_attr == pred.right_attr) {
				cc.constraint_type = "UCC";
				cc.description = pred_space.column_names[pred.left_attr] + " is unique";
			} else {
				cc.constraint_type = "DC";
				cc.description = "General denial constraint";
			}
		} else if (dc.predicate_indices.size() == 2) {
			// Two predicates: check for FD or OD patterns
			auto &pred1 = pred_space.predicates[dc.predicate_indices[0]];
			auto &pred2 = pred_space.predicates[dc.predicate_indices[1]];
			PredicateOperator neg_op1 = Predicate::Negate(pred1.op);
			PredicateOperator neg_op2 = Predicate::Negate(pred2.op);

			// FD pattern: NOT(t1.A = t2.A AND t1.B != t2.B)
			// This means: if A values are equal, B values must be equal too → A → B
			bool is_fd = false;
			string determinant;
			string dependent;

			if (neg_op1 == PredicateOperator::EQ && pred1.left_attr == pred1.right_attr &&
			    neg_op2 == PredicateOperator::NEQ && pred2.left_attr == pred2.right_attr) {
				is_fd = true;
				determinant = pred_space.column_names[pred1.left_attr];
				dependent = pred_space.column_names[pred2.left_attr];
			} else if (neg_op2 == PredicateOperator::EQ && pred2.left_attr == pred2.right_attr &&
			           neg_op1 == PredicateOperator::NEQ && pred1.left_attr == pred1.right_attr) {
				is_fd = true;
				determinant = pred_space.column_names[pred2.left_attr];
				dependent = pred_space.column_names[pred1.left_attr];
			}

			if (is_fd) {
				cc.constraint_type = "FD";
				cc.description = determinant + " -> " + dependent;
			} else {
				// Check for OD pattern: inequality predicates
				bool has_order1 = (neg_op1 == PredicateOperator::LT || neg_op1 == PredicateOperator::LEQ ||
				                   neg_op1 == PredicateOperator::GT || neg_op1 == PredicateOperator::GEQ);
				bool has_order2 = (neg_op2 == PredicateOperator::LT || neg_op2 == PredicateOperator::LEQ ||
				                   neg_op2 == PredicateOperator::GT || neg_op2 == PredicateOperator::GEQ);

				if (has_order1 && has_order2) {
					cc.constraint_type = "OD";
					string col1 = pred_space.column_names[pred1.left_attr];
					string col2 = pred_space.column_names[pred2.left_attr];
					cc.description = col1 + " and " + col2 + " are order-dependent";
				} else if (has_order1 || has_order2) {
					// Mixed: one equality, one order = conditional ordering
					cc.constraint_type = "OD";
					string col1 = pred_space.column_names[pred1.left_attr];
					string col2 = pred_space.column_names[pred2.left_attr];
					cc.description = col1 + " and " + col2 + " have conditional ordering";
				} else {
					cc.constraint_type = "DC";
					cc.description = "General denial constraint";
				}
			}
		} else {
			// 3+ predicates: check for composite FDs or classify as DC
			// Composite FD: NOT(t1.A = t2.A AND t1.B = t2.B AND t1.C != t2.C)
			// → (A, B) → C
			vector<string> eq_cols;
			vector<string> neq_cols;
			bool all_same_col_pairs = true;

			for (auto idx : dc.predicate_indices) {
				auto &pred = pred_space.predicates[idx];
				PredicateOperator neg_op = Predicate::Negate(pred.op);
				if (pred.left_attr != pred.right_attr) {
					all_same_col_pairs = false;
					break;
				}
				if (neg_op == PredicateOperator::EQ) {
					eq_cols.push_back(pred_space.column_names[pred.left_attr]);
				} else if (neg_op == PredicateOperator::NEQ) {
					neq_cols.push_back(pred_space.column_names[pred.left_attr]);
				}
			}

			if (all_same_col_pairs && !eq_cols.empty() && !neq_cols.empty() &&
			    eq_cols.size() + neq_cols.size() == dc.predicate_indices.size()) {
				cc.constraint_type = "FD";
				string det;
				for (idx_t j = 0; j < eq_cols.size(); j++) {
					if (j > 0) {
						det += ", ";
					}
					det += eq_cols[j];
				}
				string dep;
				for (idx_t j = 0; j < neq_cols.size(); j++) {
					if (j > 0) {
						dep += ", ";
					}
					dep += neq_cols[j];
				}
				cc.description = "(" + det + ") -> " + dep;
			} else {
				cc.constraint_type = "DC";
				cc.description = "General denial constraint";
			}
		}

		result.push_back(std::move(cc));
	}

	return result;
}

} // namespace dcfinder
} // namespace duckdb
