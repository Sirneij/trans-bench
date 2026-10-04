% Template: transitive closure with right recursion, for Clingo.
%
% The facts edge(X, Y) are read from the input file of the run. Only path/2 may be shown, because
% the runner writes every shown atom as one pair of the result. For XSB, the same two rules follow
% the directive ":- auto_table." and the #show line is left out (systems/xsb/rules/).

path(X, Y) :- edge(X, Y).
path(X, Y) :- edge(X, Z), path(Z, Y).

#show path/2.
