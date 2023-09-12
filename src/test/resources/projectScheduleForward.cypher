// example from https://www.pmcalculators.com/how-to-calculate-the-critical-path/

CREATE
    (start:Action {name: "Start", duration: 0}),
    (a:Action {name:"A", duration: 3}),
    (b:Action {name:"B", duration: 4}),
    (c:Action {name:"C", duration: 6}),
    (d:Action {name:"D", duration: 6}),
    (e:Action {name:"E", duration: 4}),
    (f:Action {name:"F", duration: 4}),
    (g:Action {name:"G", duration: 6}),
    (h:Action {name:"H", duration: 8}),
    (end:Action {name: "End", duration:0})

CREATE
    (start)-[:PRECEDES]->(a),
    (a)-[:PRECEDES]->(b),
    (a)-[:PRECEDES]->(c),
    (b)-[:PRECEDES]->(d),
    (b)-[:PRECEDES]->(e),
    (c)-[:PRECEDES]->(f),
    (d)-[:PRECEDES]->(g),
    (e)-[:PRECEDES]->(h),
    (f)-[:PRECEDES]->(h),
    (g)-[:PRECEDES]->(end),
    (h)-[:PRECEDES]->(end)