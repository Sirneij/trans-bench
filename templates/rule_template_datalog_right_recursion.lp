% Datalog Template: Right Recursion (Clingo, XSB, Soufflé, etc.)
% Description: Recursive datalog query for transitive closure
% Execution model: Forward chaining / SLD resolution
%
% How to use this template:
% 1. Fact input: edge(1, 2). edge(2, 3). etc. (parsed from TSV at runtime)
% 2. Rules define transitive closure computation
% 3. #show directive tells the engine what output to produce
% 4. Adjust rule names and predicates as needed for your domain

% Base case: direct edges are part of the closure
tc(X, Y) :- edge(X, Y).

% Recursive case: if X reaches Y and Y reaches Z, then X reaches Z
% This rule fires repeatedly until no new facts are generated (fixpoint)
tc(X, Z) :- tc(X, Y), edge(Y, Z).

% Output directive: tell the engine what facts to export
#show tc/2.

% Optional: Include statistics for validation
#show num_pairs(N) :- N = #count { X,Y : tc(X,Y) }.
