# mbchallenge

Keith's challenge solution

# Approach

1. Parse the query and build a tree of typed objects.
1. Typed objects all implement `AsSQL` protocol.
1. Call `as-sql` recursively down the tree to build a vector of SQL tokens.
1. Join tokens to produce SQL string.

Dialects implement quoting and LIMIT syntax (could be expanded to more areas of syntax in future if desired).

Dialects, literals and clauses are all extensible via protocols and multimethods.

Support for nested macros.
