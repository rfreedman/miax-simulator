function(values, arg) {
    return values.reduce(function(acc, item) {
        if(acc[baz]) {
            acc[baz] += item.baz;
        } else {
            acc[baz] = item.baz;
        }
        return acc;
    });
}


function(values, arg) {return values.reduce(function(acc, item) {if(acc[baz]) { acc[baz] += item.baz;} else {acc[baz] = item.baz;}return acc;});}




function(values, arg) {
    return values.reduce(function(acc, item) {
        if(acc[baz]) {
            acc[baz] += item.baz;
        } else {
            acc[baz] = item.baz;
        }
        if(acc[foo]) {
            acc[foo] = null;
        }
        return acc;
    });
}




function(values, arg) {return values.reduce(function(acc, item) {if(acc[baz]) { acc[baz] += item.baz;} else {acc[baz] = item.baz;}if(acc[foo]) {acc[foo] = null;}return acc;});}
