#define DUCKDB_EXTENSION_MAIN

#include "dcfinder_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

#include "dcfinder/predicate.hpp"
#include "dcfinder/pli.hpp"
#include "dcfinder/evidence.hpp"
#include "dcfinder/cover_search.hpp"
#include "dcfinder/soundness.hpp"

namespace duckdb {

struct DCFinderBindData : public TableFunctionData {
	string table_name;
	double threshold;
	bool soundness;
};

struct DCFinderGlobalState : public GlobalTableFunctionState {
	vector<dcfinder::DenialConstraint> results;
	vector<bool> is_sound;
	dcfinder::PredicateSpace pred_space;
	idx_t current_idx;
	bool done;

	DCFinderGlobalState() : current_idx(0), done(false) {
	}
};

static unique_ptr<FunctionData> DCFinderBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DCFinderBindData>();

	result->table_name = input.inputs[0].GetValue<string>();
	result->threshold = 0.0;
	result->soundness = true;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "threshold") {
			result->threshold = kv.second.GetValue<double>();
		} else if (kv.first == "soundness") {
			result->soundness = kv.second.GetValue<bool>();
		}
	}

	names.push_back("dc_id");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("dc");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("num_predicates");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("violation_count");
	return_types.push_back(LogicalType::BIGINT);

	names.push_back("approximation");
	return_types.push_back(LogicalType::DOUBLE);

	names.push_back("succinctness");
	return_types.push_back(LogicalType::DOUBLE);

	names.push_back("is_sound");
	return_types.push_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DCFinderInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DCFinderBindData>();
	auto result = make_uniq<DCFinderGlobalState>();

	// Use a separate connection to read the table data
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	auto query_result = con.Query("SELECT * FROM " + KeywordHelper::WriteOptionallyQuoted(bind_data.table_name));
	if (query_result->HasError()) {
		throw InvalidInputException("Failed to read table '%s': %s", bind_data.table_name, query_result->GetError());
	}

	// Get column info
	vector<string> col_names;
	vector<LogicalType> col_types;
	for (idx_t i = 0; i < query_result->ColumnCount(); i++) {
		col_names.push_back(query_result->ColumnName(i));
		col_types.push_back(query_result->types[i]);
	}

	// Materialize into column-major format
	idx_t num_columns = col_names.size();
	vector<vector<Value>> column_data(num_columns);
	idx_t num_tuples = 0;

	while (true) {
		auto chunk = query_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t col = 0; col < num_columns; col++) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				column_data[col].push_back(chunk->GetValue(col, row));
			}
		}
		num_tuples += chunk->size();
	}

	if (num_tuples <= 1) {
		result->done = true;
		return std::move(result);
	}

	// Phase 1: Build predicate space
	result->pred_space.Build(col_names, col_types, column_data);

	// Phase 2: Build PLIs
	dcfinder::PLISet pli_set;
	pli_set.Build(column_data, col_types);

	// Phase 3: Build evidence set
	dcfinder::EvidenceSet evidence_set;
	evidence_set.Build(result->pred_space, pli_set, column_data, num_tuples);

	// Phase 4: Find minimal DCs
	auto all_dcs =
	    dcfinder::CoverSearch::FindMinimalDCs(evidence_set, result->pred_space, bind_data.threshold, num_tuples);

	// Phase 5: Soundness check (Martin et al., PVLDB 2025)
	// Filters out DCs whose predicates are statistically independent
	if (bind_data.soundness) {
		// Filter mode: only keep sound DCs
		for (auto &dc : all_dcs) {
			bool sound = dcfinder::SoundnessChecker::IsSound(dc, evidence_set);
			if (sound) {
				result->is_sound.push_back(true);
				result->results.push_back(std::move(dc));
			}
		}
	} else {
		// Report mode: keep all DCs, tag each with is_sound
		for (auto &dc : all_dcs) {
			bool sound = dcfinder::SoundnessChecker::IsSound(dc, evidence_set);
			result->is_sound.push_back(sound);
			result->results.push_back(std::move(dc));
		}
	}

	return std::move(result);
}

static void DCFinderFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DCFinderGlobalState>();

	if (state.done || state.current_idx >= state.results.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (state.current_idx < state.results.size() && count < max_count) {
		auto &dc = state.results[state.current_idx];

		output.SetValue(0, count, Value::INTEGER(static_cast<int32_t>(state.current_idx + 1)));
		output.SetValue(1, count, Value(dc.ToString(state.pred_space)));
		output.SetValue(2, count, Value::INTEGER(static_cast<int32_t>(dc.predicate_indices.size())));
		output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(dc.violation_count)));
		output.SetValue(4, count, Value::DOUBLE(dc.approximation));
		output.SetValue(5, count, Value::DOUBLE(dc.succinctness));
		output.SetValue(6, count, Value::BOOLEAN(state.is_sound[state.current_idx]));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction dcfinder_func("dcfinder", {LogicalType::VARCHAR}, DCFinderFunction, DCFinderBind, DCFinderInit);
	dcfinder_func.named_parameters["threshold"] = LogicalType::DOUBLE;
	dcfinder_func.named_parameters["soundness"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(dcfinder_func);
}

void DcfinderExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DcfinderExtension::Name() {
	return "dcfinder";
}

std::string DcfinderExtension::Version() const {
#ifdef EXT_VERSION_DCFINDER
	return EXT_VERSION_DCFINDER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(dcfinder, loader) {
	duckdb::LoadInternal(loader);
}
}
