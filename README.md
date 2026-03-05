# DCFinder — DuckDB Extension

A DuckDB extension that implements the **DCFinder** algorithm for discovering **Denial Constraints (DCs)** from data.

Based on the paper:
> Eduardo H. M. Pena, Eduardo C. de Almeida, and Felix Naumann. **"Discovery of Approximate (and Exact) Denial Constraints."** *Proceedings of the VLDB Endowment*, Vol. 13, No. 3, pp. 266-278, 2019.
> [Paper PDF](http://www.vldb.org/pvldb/vol13/p266-pena.pdf)

## What are Denial Constraints?

Denial Constraints (DCs) are a powerful class of integrity constraints that subsume many other types, including:
- **Functional Dependencies** (FDs)
- **Unique Column Combinations** (UCCs)
- **Order Dependencies** (ODs)

A DC states that for any two tuples, a certain combination of predicates cannot all be true simultaneously. For example:
- `NOT(t1.name = t2.name AND t1.phone = t2.phone AND t1.position != t2.position)` — same name & phone implies same position (an FD)
- `NOT(t1.salary = t2.salary)` — all salaries are unique (a UCC)
- `NOT(t1.position = t2.position AND t1.hired < t2.hired AND t1.salary > t2.salary)` — among employees with the same position, seniority determines salary (an order constraint)

## Usage

```sql
-- Discover exact denial constraints
SELECT * FROM dcfinder('my_table');

-- Discover approximate DCs (allowing some violations)
SELECT * FROM dcfinder('my_table', threshold := 0.05);
```

### Output Columns

| Column | Type | Description |
|--------|------|-------------|
| `dc_id` | INTEGER | Unique constraint ID |
| `dc` | VARCHAR | Human-readable DC string |
| `num_predicates` | INTEGER | Number of predicates in the DC |
| `violation_count` | BIGINT | Number of violating tuple pairs |
| `approximation` | DOUBLE | g1 measure (violation ratio) |
| `succinctness` | DOUBLE | Interestingness score (1/num_predicates) |

### Example

```sql
CREATE TABLE employees (
    name VARCHAR, phone VARCHAR, position VARCHAR,
    salary INTEGER, hired INTEGER
);

INSERT INTO employees VALUES
    ('W. Jones', '202-222', 'Developer', 2000, 2012),
    ('B. Jones', '202-222', 'Developer', 3000, 2010),
    ('J. Miller', '202-333', 'Developer', 4000, 2010),
    ('D. Miller', '202-333', 'DBA', 8000, 2010),
    ('W. Jones', '202-555', 'DBA', 7000, 2010),
    ('W. Jones', '202-222', 'Developer', 1000, 2012);

-- Find exact DCs
SELECT dc_id, dc, num_predicates
FROM dcfinder('employees')
WHERE num_predicates <= 3
ORDER BY num_predicates;
```

## Building

```bash
git submodule update --init --recursive
GEN=ninja make
```

## Testing

```bash
make test
```

## Algorithm

The extension implements the four phases of DCFinder:

1. **Predicate Space Generation** — builds all candidate predicates from the schema (equality, inequality, ordering operators)
2. **Position List Index (PLI) Construction** — indexes column values for efficient tuple pair lookup
3. **Evidence Set Building** — for each tuple pair, records which predicates are satisfied
4. **Minimal Cover Search** — DFS-based search to find minimal sets of predicates that form valid DCs

For approximate DCs, the algorithm allows a configurable violation threshold (ε), finding DCs that hold for most but not all tuple pairs.

## License

MIT
