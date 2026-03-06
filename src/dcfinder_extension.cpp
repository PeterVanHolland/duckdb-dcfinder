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
#include "dcfinder/dc_parser.hpp"
#include "dcfinder/violations.hpp"
#include "dcfinder/error_cells.hpp"
#include "dcfinder/repairs.hpp"
#include "dcfinder/profiler.hpp"

namespace duckdb {

// ============================================================
// Helper: materialize a table into column-major format
// ============================================================
struct MaterializedTable {
	vector<string> col_names;
	vector<LogicalType> col_types;
	vector<vector<Value>> column_data;
	idx_t num_tuples;
	idx_t num_columns;
};

static MaterializedTable MaterializeTable(ClientContext &context, const string &table_name) {
	MaterializedTable result;
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	auto query_result = con.Query("SELECT * FROM " + KeywordHelper::WriteOptionallyQuoted(table_name));
	if (query_result->HasError()) {
		throw InvalidInputException("Failed to read table '%s': %s", table_name, query_result->GetError());
	}

	for (idx_t i = 0; i < query_result->ColumnCount(); i++) {
		result.col_names.push_back(query_result->ColumnName(i));
		result.col_types.push_back(query_result->types[i]);
	}

	result.num_columns = result.col_names.size();
	result.column_data.resize(result.num_columns);
	result.num_tuples = 0;

	while (true) {
		auto chunk = query_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t col = 0; col < result.num_columns; col++) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				result.column_data[col].push_back(chunk->GetValue(col, row));
			}
		}
		result.num_tuples += chunk->size();
	}

	return result;
}

// ============================================================
// dcfinder() — DC Discovery
// ============================================================
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

	names.emplace_back("dc_id");
	return_types.push_back(LogicalType::INTEGER);
	names.emplace_back("dc");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("num_predicates");
	return_types.push_back(LogicalType::INTEGER);
	names.emplace_back("violation_count");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("approximation");
	return_types.push_back(LogicalType::DOUBLE);
	names.emplace_back("succinctness");
	return_types.push_back(LogicalType::DOUBLE);
	names.emplace_back("is_sound");
	return_types.push_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DCFinderInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DCFinderBindData>();
	auto result = make_uniq<DCFinderGlobalState>();

	auto table = MaterializeTable(context, bind_data.table_name);

	if (table.num_tuples <= 1) {
		result->done = true;
		return std::move(result);
	}

	result->pred_space.Build(table.col_names, table.col_types, table.column_data);

	dcfinder::PLISet pli_set;
	pli_set.Build(table.column_data, table.col_types);

	dcfinder::EvidenceSet evidence_set;
	evidence_set.Build(result->pred_space, pli_set, table.column_data, table.num_tuples);

	auto all_dcs =
	    dcfinder::CoverSearch::FindMinimalDCs(evidence_set, result->pred_space, bind_data.threshold, table.num_tuples);

	if (bind_data.soundness) {
		for (auto &dc : all_dcs) {
			if (dcfinder::SoundnessChecker::IsSound(dc, evidence_set)) {
				result->is_sound.push_back(true);
				result->results.push_back(std::move(dc));
			}
		}
	} else {
		for (auto &dc : all_dcs) {
			result->is_sound.push_back(dcfinder::SoundnessChecker::IsSound(dc, evidence_set));
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
	while (state.current_idx < state.results.size() && count < STANDARD_VECTOR_SIZE) {
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

// ============================================================
// dc_violations() — Violation Detection (FACET-inspired)
// ============================================================
struct DCViolationsBindData : public TableFunctionData {
	string table_name;
	string dc_text;
};

struct DCViolationsGlobalState : public GlobalTableFunctionState {
	vector<dcfinder::Violation> violations;
	dcfinder::ParsedDC parsed_dc;
	vector<string> col_names;
	vector<vector<Value>> column_data;
	idx_t current_idx;
	bool done;
	DCViolationsGlobalState() : current_idx(0), done(false) {
	}
};

static unique_ptr<FunctionData> DCViolationsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DCViolationsBindData>();
	result->table_name = input.inputs[0].GetValue<string>();
	result->dc_text = input.inputs[1].GetValue<string>();

	names.emplace_back("violation_id");
	return_types.push_back(LogicalType::INTEGER);
	names.emplace_back("rowid_1");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("rowid_2");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("dc");
	return_types.push_back(LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DCViolationsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DCViolationsBindData>();
	auto result = make_uniq<DCViolationsGlobalState>();

	result->parsed_dc = dcfinder::DCParser::Parse(bind_data.dc_text);
	auto table = MaterializeTable(context, bind_data.table_name);

	if (table.num_tuples <= 1) {
		result->done = true;
		return std::move(result);
	}

	result->col_names = table.col_names;
	result->column_data = std::move(table.column_data);
	result->violations = dcfinder::ViolationDetector::FindViolations(
	    result->parsed_dc, result->col_names, table.col_types, result->column_data, table.num_tuples);

	return std::move(result);
}

static void DCViolationsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DCViolationsGlobalState>();
	if (state.done || state.current_idx >= state.violations.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	while (state.current_idx < state.violations.size() && count < STANDARD_VECTOR_SIZE) {
		auto &v = state.violations[state.current_idx];
		output.SetValue(0, count, Value::INTEGER(static_cast<int32_t>(state.current_idx + 1)));
		output.SetValue(1, count, Value::BIGINT(static_cast<int64_t>(v.tuple1_rowid)));
		output.SetValue(2, count, Value::BIGINT(static_cast<int64_t>(v.tuple2_rowid)));
		output.SetValue(3, count, Value(state.parsed_dc.ToString()));
		state.current_idx++;
		count++;
	}
	output.SetCardinality(count);
}

// ============================================================
// dc_error_cells() — Error Cell Detection (Holistic-inspired)
// ============================================================
struct DCErrorCellsBindData : public TableFunctionData {
	string table_name;
	string dc_text;
};

struct DCErrorCellsGlobalState : public GlobalTableFunctionState {
	vector<dcfinder::ErrorCell> error_cells;
	vector<string> col_names;
	vector<vector<Value>> column_data;
	idx_t current_idx;
	bool done;
	DCErrorCellsGlobalState() : current_idx(0), done(false) {
	}
};

static unique_ptr<FunctionData> DCErrorCellsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DCErrorCellsBindData>();
	result->table_name = input.inputs[0].GetValue<string>();
	result->dc_text = input.inputs[1].GetValue<string>();

	names.emplace_back("row_id");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("column_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("current_value");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("violation_count");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("error_likelihood");
	return_types.push_back(LogicalType::DOUBLE);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DCErrorCellsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DCErrorCellsBindData>();
	auto result = make_uniq<DCErrorCellsGlobalState>();

	auto parsed_dc = dcfinder::DCParser::Parse(bind_data.dc_text);
	auto table = MaterializeTable(context, bind_data.table_name);

	if (table.num_tuples <= 1) {
		result->done = true;
		return std::move(result);
	}

	result->col_names = table.col_names;
	result->column_data = std::move(table.column_data);

	auto violations = dcfinder::ViolationDetector::FindViolations(parsed_dc, result->col_names, table.col_types,
	                                                              result->column_data, table.num_tuples);

	result->error_cells = dcfinder::ErrorCellDetector::FindErrorCells(violations, parsed_dc, result->col_names,
	                                                                  result->column_data, table.num_tuples);

	return std::move(result);
}

static void DCErrorCellsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DCErrorCellsGlobalState>();
	if (state.done || state.current_idx >= state.error_cells.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	while (state.current_idx < state.error_cells.size() && count < STANDARD_VECTOR_SIZE) {
		auto &ec = state.error_cells[state.current_idx];
		output.SetValue(0, count, Value::BIGINT(static_cast<int64_t>(ec.row_id)));
		output.SetValue(1, count, Value(state.col_names[ec.col_id]));
		// Get the current cell value as string
		string cell_val;
		if (ec.row_id < state.column_data[ec.col_id].size()) {
			cell_val = state.column_data[ec.col_id][ec.row_id].ToString();
		}
		output.SetValue(2, count, Value(cell_val));
		output.SetValue(3, count, Value::BIGINT(static_cast<int64_t>(ec.violation_count)));
		output.SetValue(4, count, Value::DOUBLE(ec.error_likelihood));
		state.current_idx++;
		count++;
	}
	output.SetCardinality(count);
}

// ============================================================
// dc_repairs() — Repair Suggestions (Holistic-inspired)
// ============================================================
struct DCRepairsBindData : public TableFunctionData {
	string table_name;
	string dc_text;
};

struct DCRepairsGlobalState : public GlobalTableFunctionState {
	vector<dcfinder::RepairSuggestion> repairs;
	vector<string> col_names;
	idx_t current_idx;
	bool done;
	DCRepairsGlobalState() : current_idx(0), done(false) {
	}
};

static unique_ptr<FunctionData> DCRepairsBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DCRepairsBindData>();
	result->table_name = input.inputs[0].GetValue<string>();
	result->dc_text = input.inputs[1].GetValue<string>();

	names.emplace_back("row_id");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("column_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("current_value");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("suggested_value");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("repair_type");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("confidence");
	return_types.push_back(LogicalType::DOUBLE);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DCRepairsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DCRepairsBindData>();
	auto result = make_uniq<DCRepairsGlobalState>();

	auto parsed_dc = dcfinder::DCParser::Parse(bind_data.dc_text);
	auto table = MaterializeTable(context, bind_data.table_name);

	if (table.num_tuples <= 1) {
		result->done = true;
		return std::move(result);
	}

	result->col_names = table.col_names;

	auto violations = dcfinder::ViolationDetector::FindViolations(parsed_dc, table.col_names, table.col_types,
	                                                              table.column_data, table.num_tuples);

	auto error_cells = dcfinder::ErrorCellDetector::FindErrorCells(violations, parsed_dc, table.col_names,
	                                                               table.column_data, table.num_tuples);

	result->repairs = dcfinder::RepairGenerator::SuggestRepairs(violations, error_cells, parsed_dc, table.col_names,
	                                                            table.col_types, table.column_data, table.num_tuples);

	return std::move(result);
}

static void DCRepairsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DCRepairsGlobalState>();
	if (state.done || state.current_idx >= state.repairs.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	while (state.current_idx < state.repairs.size() && count < STANDARD_VECTOR_SIZE) {
		auto &r = state.repairs[state.current_idx];
		output.SetValue(0, count, Value::BIGINT(static_cast<int64_t>(r.row_id)));
		output.SetValue(1, count, Value(state.col_names[r.col_id]));
		output.SetValue(2, count, Value(r.current_value.ToString()));
		output.SetValue(3, count, Value(r.suggested_value.ToString()));
		output.SetValue(4, count, Value(r.repair_type));
		output.SetValue(5, count, Value::DOUBLE(r.confidence));
		state.current_idx++;
		count++;
	}
	output.SetCardinality(count);
}

// ============================================================
// dc_profile() — Data Profiling / Constraint Classification
// ============================================================
struct DCProfileBindData : public TableFunctionData {
	string table_name;
	double threshold;
	bool soundness;
};

struct DCProfileGlobalState : public GlobalTableFunctionState {
	vector<dcfinder::ClassifiedConstraint> classified;
	idx_t current_idx;
	bool done;
	DCProfileGlobalState() : current_idx(0), done(false) {
	}
};

static unique_ptr<FunctionData> DCProfileBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DCProfileBindData>();
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

	names.emplace_back("constraint_type");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("description");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("dc");
	return_types.push_back(LogicalType::VARCHAR);
	names.emplace_back("num_predicates");
	return_types.push_back(LogicalType::INTEGER);
	names.emplace_back("is_sound");
	return_types.push_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DCProfileInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DCProfileBindData>();
	auto result = make_uniq<DCProfileGlobalState>();

	auto table = MaterializeTable(context, bind_data.table_name);

	if (table.num_tuples <= 1) {
		result->done = true;
		return std::move(result);
	}

	dcfinder::PredicateSpace pred_space;
	pred_space.Build(table.col_names, table.col_types, table.column_data);

	dcfinder::PLISet pli_set;
	pli_set.Build(table.column_data, table.col_types);

	dcfinder::EvidenceSet evidence_set;
	evidence_set.Build(pred_space, pli_set, table.column_data, table.num_tuples);

	auto all_dcs =
	    dcfinder::CoverSearch::FindMinimalDCs(evidence_set, pred_space, bind_data.threshold, table.num_tuples);

	vector<dcfinder::DenialConstraint> filtered;
	vector<bool> sound_flags;

	if (bind_data.soundness) {
		for (auto &dc : all_dcs) {
			bool s = dcfinder::SoundnessChecker::IsSound(dc, evidence_set);
			if (s) {
				sound_flags.push_back(true);
				filtered.push_back(std::move(dc));
			}
		}
	} else {
		for (auto &dc : all_dcs) {
			sound_flags.push_back(dcfinder::SoundnessChecker::IsSound(dc, evidence_set));
			filtered.push_back(std::move(dc));
		}
	}

	result->classified = dcfinder::DCProfiler::Classify(filtered, pred_space, sound_flags);

	return std::move(result);
}

static void DCProfileFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<DCProfileGlobalState>();
	if (state.done || state.current_idx >= state.classified.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t count = 0;
	while (state.current_idx < state.classified.size() && count < STANDARD_VECTOR_SIZE) {
		auto &cc = state.classified[state.current_idx];
		output.SetValue(0, count, Value(cc.constraint_type));
		output.SetValue(1, count, Value(cc.description));
		output.SetValue(2, count, Value(cc.dc_text));
		output.SetValue(3, count, Value::INTEGER(static_cast<int32_t>(cc.num_predicates)));
		output.SetValue(4, count, Value::BOOLEAN(cc.is_sound));
		state.current_idx++;
		count++;
	}
	output.SetCardinality(count);
}
// ============================================================
// Registration
// ============================================================
static void LoadInternal(ExtensionLoader &loader) {
	// dcfinder: DC Discovery
	TableFunction dcfinder_func("dcfinder", {LogicalType::VARCHAR}, DCFinderFunction, DCFinderBind, DCFinderInit);
	dcfinder_func.named_parameters["threshold"] = LogicalType::DOUBLE;
	dcfinder_func.named_parameters["soundness"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(dcfinder_func);

	// dc_violations: Violation Detection
	TableFunction violations_func("dc_violations", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DCViolationsFunction,
	                              DCViolationsBind, DCViolationsInit);
	loader.RegisterFunction(violations_func);

	// dc_error_cells: Error Cell Detection

	TableFunction error_cells_func("dc_error_cells", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DCErrorCellsFunction,
	                               DCErrorCellsBind, DCErrorCellsInit);
	loader.RegisterFunction(error_cells_func);

	// dc_repairs: Repair Suggestions
	TableFunction repairs_func("dc_repairs", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DCRepairsFunction,
	                           DCRepairsBind, DCRepairsInit);
	loader.RegisterFunction(repairs_func);

	// dc_profile: Data Profiling
	TableFunction profile_func("dc_profile", {LogicalType::VARCHAR}, DCProfileFunction, DCProfileBind, DCProfileInit);
	profile_func.named_parameters["threshold"] = LogicalType::DOUBLE;
	profile_func.named_parameters["soundness"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(profile_func);
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
