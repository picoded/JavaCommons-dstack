# Performance Testing between different JSQL implementations.

Most of the testing was based on on the following [Github commit action build](https://github.com/picoded/JavaCommons-dstack/commit/82a3be3076563a9a2700e0cf998dc77a88b32526)
Due to the lengthy test timing of some of the JSql based test, this would be disabled in future commits / build (to ensure quick CI/CD), so this is recorded for future references.

The following summarizes the overall performance benchmark between JSql / JSql_json

| Test Type                      | DataObject Mode | Overall Build Time | Test result link                                                              |
|--------------------------------|-----------------|--------------------|-------------------------------------------------------------------------------|
| MySQL 8.0 (baseline)           | JSql            | 40m 17s            | [link](https://github.com/picoded/JavaCommons-dstack/actions/runs/1520128402) |
| Postgres 14.1                  | JSql            | 2h 52m 17s         | [link](https://github.com/picoded/JavaCommons-dstack/actions/runs/1520128394) |
| Cockroach DB v21 (single node) | JSql            | 3h +++             | [link](https://github.com/picoded/JavaCommons-dstack/actions/runs/1520128403) |
| Postgres 14.1                  | JSql_json       | 4m 32s             | [link](https://github.com/picoded/JavaCommons-dstack/actions/runs/1520128404) |
| Cockroach DB v21 (single node) | JSql_json       | 19m 39s            | [link](https://github.com/picoded/JavaCommons-dstack/actions/runs/1520128400) |

In overall, JSql_json implementation shows a remarkable 10x improvement from MySQL (our current baseline). 
This is negated when switching over to Cockroach DB. Which only shows a 2x improvement against the baseline.