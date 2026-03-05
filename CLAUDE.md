# DCFinder DuckDB Extension

## What to Build

Implement the **DCFinder** algorithm from the PVLDB paper "Discovery of Approximate (and Exact) Denial Constraints" (Pena, de Almeida, Naumann, PVLDB Vol 13, 2019) as a DuckDB extension.

## Extension Name

Rename everything from `quack` to `dcfinder`. The extension should expose a **table function** called `dcfinder` (and optionally `dc_find`).

## Paper Summary — DCFinder Algorithm

### What are Denial Constraints (DCs)?

A DC is: for all tx, ty in r, NOT(p1 AND ... AND pm)

It says: for any two tuples tx, ty, the predicates p1...pm cannot ALL be true simultaneously.

DCs subsume FDs, UCCs, and order dependencies. Examples:
- FD: NOT(tx.Name = ty.Name AND tx.Phone = ty.Phone AND tx.Position != ty.Position)
- Order constraint: NOT(tx.Position = ty.Position AND tx.Hired < ty.Hired AND tx.Salary < ty.Salary)

### Approximate DCs

An epsilon-approximate DC allows some violations. The approximation measure:
g1(phi, r) = |violating_pairs| / (|r| * (|r| - 1))

### Algorithm Overview (4 phases)

#### Phase 1: Build Predicate Space
From the schema, generate all predicates:
- For numeric attributes: all operators {=, !=, <, <=, >, >=}
- For string/non-numeric: only {=, !=}
- Cross-attribute predicates only if same type and >=30% value overlap

#### Phase 2: Build Position List Indexes (PLIs)
For each attribute, build a PLI: map from value -> list of tuple IDs with that value.
Numeric PLIs sorted descending by key.

#### Phase 3: Build Evidence Set
This is the core of DCFinder. For each tuple pair, compute which predicates it satisfies (its "evidence").

Key optimization -- Evidence Ahead:
1. Initialize eahead with all predicates where operator in {!=, <, <=} (these are likely satisfied by most pairs)
2. Create array B where every element starts as a copy of eahead
3. Use PLIs to find tuple pairs that satisfy = and > predicates
4. For those pairs, XOR (symmetric difference) the evidence to reconstruct the correct state
5. Deduplicate evidence and track multiplicity (count)

The evidence reconstruction uses the implication/negation table:
- = implies {=, <=, >=}
- < implies {<, <=, !=}
- > implies {>, >=, !=}
- Negation: = <-> !=, < <-> >=, > <-> <=

Scaling: Use chunked processing. Partition tuple pair ID space into chunks of size omega, process each chunk independently, merge partial evidence sets. This enables parallelism.

#### Phase 4: Cover Search (find minimal DCs)
The problem transforms to: find all minimal covers of the evidence set.
A cover Q is a set of predicates that intersects with every evidence.

Uses DFS with:
- Sort predicates by coverage of uncovered evidence
- Prune if Q is implied by already-found minimal covers
- For approximate DCs: Q is a cover if uncovered evidence multiplicity <= epsilon * |r| * (|r| - 1)
- Check minimality: no subset of Q is also a cover

### Evidence represented as bitsets
Each evidence is a bitset over the predicate space. XOR operations for reconstruction are very efficient.

## DuckDB Extension Interface

### Table Function: dcfinder(table_name, [threshold])

```sql
-- Discover exact DCs
SELECT * FROM dcfinder('employees');

-- Discover approximate DCs with threshold 0.01
SELECT * FROM dcfinder('employees', threshold := 0.01);
```

Output columns:
- dc_id (INTEGER): unique ID
- dc (VARCHAR): human-readable DC string, e.g., "NOT(t1.Name = t2.Name AND t1.Phone = t2.Phone AND t1.Position != t2.Position)"
- num_predicates (INTEGER): number of predicates in the DC
- violation_count (BIGINT): number of violating tuple pairs
- approximation (DOUBLE): g1 measure (violation ratio)
- succinctness (DOUBLE): interestingness score

## Implementation Structure

```
src/
  include/
    dcfinder_extension.hpp       -- Extension header
    dcfinder/
      predicate.hpp              -- Predicate types and space
      pli.hpp                    -- Position List Index
      evidence.hpp               -- Evidence set building
      cover_search.hpp           -- DFS cover search
      dc.hpp                     -- Denial constraint representation
  dcfinder_extension.cpp         -- Extension entry point, register functions
  dcfinder/
    predicate.cpp
    pli.cpp
    evidence.cpp
    cover_search.cpp
```

## Key Implementation Details

### Predicate Representation
```cpp
struct Predicate {
    idx_t left_attr;   // attribute index
    idx_t right_attr;  // attribute index (can equal left)
    Operator op;       // =, !=, <, <=, >, >=
};
enum class Operator { EQ, NEQ, LT, LEQ, GT, GEQ };
```

### PLI
```cpp
struct PLICluster {
    Value key;
    vector<idx_t> tuple_ids;  // sorted ascending
};
struct PLI {
    vector<PLICluster> clusters;  // numeric: sorted descending by key
};
```

### Evidence as Bitset
Use a fixed-size bitset (or dynamic_bitset) indexed by predicate ID.
Use vector<bool> or bitset or just uint64_t bitmasks if predicates fit.
For larger predicate spaces, use vector<uint64_t> as a manual bitset.

### Evidence Set
```cpp
unordered_map<Evidence, idx_t> evidence_set;  // evidence -> count
```

### Tuple Pair ID
```cpp
int64_t tpid(idx_t x, idx_t y, idx_t n) { return n * x + y; }
```

### Cover Search
DFS recursive function. See Algorithm 3 in the paper description above.

## CMake Changes

1. Remove OpenSSL dependency (not needed)
2. Rename target from quack to dcfinder
3. Update extension_config.cmake accordingly
4. Update vcpkg.json (remove openssl)

## Tests

Create test/sql/dcfinder.test:
```
# name: test/sql/dcfinder.test
# description: Test DCFinder extension
# group: [dcfinder]

require dcfinder

# Basic test with a small table
statement ok
CREATE TABLE employees (
    name VARCHAR, phone VARCHAR, position VARCHAR,
    salary INTEGER, hired INTEGER
);

statement ok
INSERT INTO employees VALUES
    ('W. Jones', '202-222', 'Developer', 2000, 2012),
    ('B. Jones', '202-222', 'Developer', 3000, 2010),
    ('J. Miller', '202-333', 'Developer', 4000, 2010),
    ('D. Miller', '202-333', 'DBA', 8000, 2010),
    ('W. Jones', '202-555', 'DBA', 7000, 2010),
    ('W. Jones', '202-222', 'Developer', 1000, 2012);

# Should discover DCs
query I
SELECT count(*) > 0 FROM dcfinder('employees');
----
true

# With threshold
query I
SELECT count(*) > 0 FROM dcfinder('employees', threshold := 0.01);
----
true
```

## Priority

1. Get the extension compiling with the renamed structure
2. Implement PLI building
3. Implement predicate space generation
4. Implement evidence set building (this is the hard part -- start simple without chunking)
5. Implement cover search
6. Wire everything into the table function
7. Add tests

## Build & Test
```bash
GEN=ninja make
make test
```

Submodules need to be initialized:
```bash
git submodule update --init --recursive
```

## Important Notes
- This is a DuckDB extension. Use DuckDB APIs (DataChunk, Vector, etc.)
- The table function should read from any DuckDB table by name
- Start with a single-threaded implementation, optimize later
- Use idx_t for indices (DuckDB convention)
- Keep evidence as bitsets for XOR efficiency
- The paper's employees table from Section 1 is the canonical test case
