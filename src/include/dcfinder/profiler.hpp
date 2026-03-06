#pragma once

#include "duckdb.hpp"
#include "dcfinder/predicate.hpp"
#include "dcfinder/cover_search.hpp"
#include <vector>

namespace duckdb {
namespace dcfinder {

//! Classified constraint from DC discovery
struct ClassifiedConstraint {
	string dc_text;
	string constraint_type; //! "UCC", "FD", "OD", "DC"
	string description;     //! Human-readable description
	idx_t num_predicates;
	bool is_sound;
};

//! Classifies discovered DCs into human-readable constraint types.
//!
//! - UCC (Unique Column Combination): NOT(t1.A = t2.A)
//!   → Column A is unique
//!
//! - FD (Functional Dependency): NOT(t1.A = t2.A AND t1.B != t2.B)
//!   → A functionally determines B (A → B)
//!
//! - OD (Order Dependency): NOT(t1.A < t2.A AND t1.B > t2.B)
//!   → A and B have order dependency
//!
//! - DC (General Denial Constraint): everything else
struct DCProfiler {
	static vector<ClassifiedConstraint> Classify(const vector<DenialConstraint> &dcs, const PredicateSpace &pred_space,
	                                             const vector<bool> &soundness);
};

} // namespace dcfinder
} // namespace duckdb
