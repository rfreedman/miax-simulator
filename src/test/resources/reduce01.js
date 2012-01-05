/*
input: [{"c1":{"baz":6}},{"c2":{"baz":10}},{"c1":{"baz":3}},{"c2":{"baz":100}}]
output: [{"c1":{"baz":9},"c2":{"baz":110}}]
*/
function reduce(values){
  return [values.reduce(function(acc, item){
    for(field in item){
      if(acc[field]) {
             // lhs + rhs
            var lhs = acc[field];
            var rhs = item[field];
            for(f2 in rhs) {
                if(lhs[f2]) {
                    lhs[f2] += rhs[f2];
                } else {
                    lhs[f2] = rhs[f2];
                }
            }
      }
      else {
         acc[field] = item[field];
      }
      return acc;
    }
  })];
}
