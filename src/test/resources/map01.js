// group by cloud, remove cloud from the item props, and make "c" + cloud number the aggregate key
//
// input: [{"baz":10,"cloud":2},{"baz":6,"cloud":1},{"baz":100,"cloud":2},{"baz":3,"cloud":1}]
// output: [{"c1":{"baz":6}},{"c2":{"baz":10}},{"c1":{"baz":3}},{"c2":{"baz":100}}]
function map(object){
  //var data = Riak.mapValuesJson(object)[0];
  /*
  var result = {};

  var d2 = {};
  for(var prop in data) {
      if(prop != 'cloud') {
          d2[prop] = data[prop];
      }
  }
  result["c" + data.cloud] = d2;
  return [result];
  */
   return [];
}

