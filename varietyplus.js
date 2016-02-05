/* Variety: A MongoDB Schema Analyzer

This tool helps you get a sense of your application's schema, as well as any
outliers to that schema. Particularly useful when you inherit a codebase with
data dump and want to quickly learn how the data's structured. Also useful for
finding rare keys.

Released by Maypop Inc, © 2012-2015, under the MIT License. */

var log = function(message) {
  if(!__quiet) { // mongo shell param, coming from https://github.com/mongodb/mongo/blob/5fc306543cd3ba2637e5cb0662cc375f36868b28/src/mongo/shell/dbshell.cpp#L624
      print(message);
    }
};

log('Variety: A MongoDB Schema Analyzer');

var dbs = [];
var emptyDbs = [];

if (typeof slaveOk !== 'undefined') {
  if (slaveOk === true) {
    db.getMongo().setSlaveOk();
  }
}

var knownDatabases = db.adminCommand('listDatabases').databases;
if(typeof knownDatabases !== 'undefined') { // not authorized user receives error response (json) without databases key
  knownDatabases.forEach(function(d){
    if(db.getSisterDB(d.name).getCollectionNames().length > 0) {
      dbs.push(d.name);
    }
    if(db.getSisterDB(d.name).getCollectionNames().length === 0) {
      emptyDbs.push(d.name);
    }
  });

  if (emptyDbs.indexOf(db.getName()) !== -1) {
    throw 'The database specified ('+ db +') is empty.\n'+
          'Possible database options are: ' + dbs.join(', ') + '.';
  }

  if (dbs.indexOf(db.getName()) === -1) {
    throw 'The database specified ('+ db +') does not exist.\n'+
          'Possible database options are: ' + dbs.join(', ') + '.';
  }
}

var collNames = db.getCollectionNames().join(', ');
if (typeof collection === 'undefined') {
  throw 'You have to supply a \'collection\' variable, à la --eval \'var collection = "animals"\'.\n'+
        'Possible collection options for database specified: ' + collNames + '.\n'+
        'Please see https://github.com/variety/variety for details.';
}

if (db[collection].count() === 0) {
  throw 'The collection specified (' + collection + ') in the database specified ('+ db +') does not exist or is empty.\n'+
        'Possible collection options for database specified: ' + collNames + '.';
}

var readConfig = function(configProvider) {
  var config = {};
  var read = function(name, defaultValue) {
    var value = typeof configProvider[name] !== 'undefined' ? configProvider[name] : defaultValue;
    config[name] = value;
    log('Using '+name+' of ' + tojson(value));
  };
  read('collection', null);
  read('query', {});
  read('limit', db[config.collection].find(config.query).count());
  read('maxDepth', 99);
  read('sort', {_id: -1});
  read('outputFormat', 'ascii');
  read('persistResults', false);
  read('resultsDatabase', 'varietyResults');
  read('resultsCollection', collection + 'Keys');
  read('resultsUser', null);
  read('resultsPass', null);
  return config;
};

var config = readConfig(this);

var PluginsClass = function(context) {
  var parsePath = function(val) { return val.slice(-3) !== '.js' ? val + '.js' : val;};
  var parseConfig = function(val) {
    var config = {};
    val.split('&').reduce(function(acc, val) {
      var parts = val.split('=');
      acc[parts[0]] = parts[1];
      return acc;
    }, config);
    return config;
  };

  if(typeof context.plugins !== 'undefined') {
    this.plugins = context.plugins.split(',')
      .map(function(path){return path.trim();})
      .map(function(definition){
        var path = parsePath(definition.split('|')[0]);
        var config = parseConfig(definition.split('|')[1] || '');
        context.module = context.module || {};
        load(path);
        var plugin = context.module.exports;
        plugin.path = path;
        if(typeof plugin.init === 'function') {
          plugin.init(config);
        }
        return plugin;
      }, this);
  } else {
    this.plugins = [];
  }

  this.execute = function(methodName) {
    var args = Array.prototype.slice.call(arguments, 1);
    var applicablePlugins = this.plugins.filter(function(plugin){return typeof plugin[methodName] === 'function';});
    return applicablePlugins.map(function(plugin) {
      return plugin[methodName].apply(plugin, args);
    });
  };

  log('Using plugins of ' + tojson(this.plugins.map(function(plugin){return plugin.path;})));
};

var $plugins = new PluginsClass(this);
$plugins.execute('onConfig', config);

var varietyTypeOf = function(thing) {
  // p2f@20160102 start
  /*
  if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }
  */
  if (typeof thing === 'undefined') { 
	return 'undefined';  // 6
  }
  // p2f@20160102 end

  if (typeof thing !== 'object') {
    // simple data types
    // number 
    	// int 16
    	// double 1 
    // string 2
    // boolean 8 
    var typeofThing = typeof thing; // edgecase of JSHint's "singleGroups"
    return typeofThing[0].toUpperCase() + typeofThing.slice(1);
  }
  else {
    if (thing && thing.constructor === Array) {
      return 'Array';  // 4
    }
    else if (thing === null) {
      return 'null'; // 10 
    }
    else if (thing instanceof Timestamp) {
      return 'Timestamp'; // 17
    }
    else if (thing instanceof NumberLong) {
      return 'NumberLong'; // 18
    }
    else if (thing instanceof Date) {
      return 'Date'; // 9
    }
    else if (thing instanceof ObjectId) {
      return 'ObjectId'; // 7
    }
    else if (thing instanceof BinData) {
      // p2f@20160102 start
      /*
      var binDataTypes = {};
      binDataTypes[0x00] = 'generic';
      binDataTypes[0x01] = 'function';
      binDataTypes[0x02] = 'old';
      binDataTypes[0x03] = 'UUID';
      binDataTypes[0x05] = 'MD5';
      binDataTypes[0x80] = 'user';
      return 'BinData-' + binDataTypes[thing.subtype()];
	*/
      return 'BinData'; // 5
    } else {
      return 'Object'; // 3
    }
  }
};

//flattens object keys to 1D. i.e. {'key1':1,{'key2':{'key3':2}}} becomes {'key1':1,'key2.key3':2}
//we assume no '.' characters in the keys, which is an OK assumption for MongoDB
//

var serializeDoc = function(doc, maxDepth) {
  var result = {};

  //determining if an object is a Hash vs Array vs something else is hard
  //returns true, if object in argument may have nested objects and makes sense to analyse its content
  function isHash(v) {
    var isArray = Array.isArray(v);
    var isObject = typeof v === 'object';
    var specialObject = v instanceof Date ||
                        v instanceof ObjectId ||
                        v instanceof BinData;
    return !specialObject && (isArray || isObject);
  }

  function serialize(document, parentKey, maxDepth){
    for(var key in document){
      //skip over inherited properties such as string, length, etch
      if(!document.hasOwnProperty(key)) {
        continue;
      }
      var value = document[key];
      //objects are skipped here and recursed into later
      //if(typeof value != 'object')
      result[parentKey+key] = value;
      //it's an object, recurse...only if we haven't reached max depth
      if(isHash(value) && maxDepth > 1) {
        // serialize(value, parentKey+key+'.',maxDepth-1);
        serialize(value, parentKey+key, maxDepth-1);
      }
    }
  }
  serialize(doc, '', maxDepth);
  return result;
};

// convert document to key-value map, where value is always an array with types as plain strings
var analyseDocument = function(document) {
  var result = {};
  for (var key in document) {
    var value = document[key];
    //translate unnamed object key from {_parent_name_}.{_index_} to {_parent_name_}.XX
    key = key.replace(/\.\d+/g,'.XX');
    if(typeof result[key] === 'undefined') {
      result[key] = {};
    }
    var type = varietyTypeOf(value);
    result[key][type] = true;
  }
  return result;
};

var mergeDocument = function(docResult, interimResults) {
  for (var key in docResult) {
    if(key in interimResults) {
      var existing = interimResults[key];

      for(var type in docResult[key]) {
        if (type in existing.types) {
          existing.types[type] = existing.types[type] + 1;
        } else {
          existing.types[type] = 1;
        }
      }
      existing.totalOccurrences = existing.totalOccurrences + 1;
    } else {
      var types = {};
      for (var newType in docResult[key]) {
        types[newType] = 1;
      }
      interimResults[key] = {'types': types,'totalOccurrences':1};
    }
  }
};

var serializeDocstrc = function(doc, maxDepth) {
  var result = {};

  //determining if an object is a Hash vs Array vs something else is hard
  //returns true, if object in argument may have nested objects and makes sense to analyse its content
  function isHash(v) {
    var isArray = Array.isArray(v);
    var isObject = typeof v === 'object';
    var specialObject = v instanceof Date ||
                        v instanceof ObjectId ||
                        v instanceof BinData;
    return !specialObject && (isArray || isObject);
  }

  function serializestruct(document, parentKey, maxDepth){
    for(var key in document){
      //skip over inherited properties such as string, length, etch
      if(!document.hasOwnProperty(key)) {
        continue;
      }
      var value = document[key];
      //objects are skipped here and recursed into later
      //if(typeof value != 'object')
      result[parentKey+key] = value;
    }
  }
  serializestruct(doc, '', maxDepth)
  return result;
};

// convert document to key-value map, where value is always an array with types as plain strings
var analyseDocumentstrc = function(document, maxDepth) {
  var result = {};

  var recurresult = {};
  
  function serializeAValue(value, maxDepth, pararesult) {
	  for(var key in value) {
		  pararesult[key] = {};
		  var tempv = value[key];
		  var type = varietyTypeOf(tempv);
		  if(type == 'Object' && maxDepth > 1) {
			  pararesult[key][type] = {};
			  // serializeAValue(tempv, maxDepth-1, pararesult[key]);
			  serializeAValue(tempv, maxDepth-1, pararesult[key][type]);
		  } else {
			  pararesult[key][type]=true; 
		  }
	  }
  }

  for (var key in document) {
    var value = document[key];
    // print("key " + key + " value " + tojson(value));
    //translate unnamed object key from {_parent_name_}.{_index_} to {_parent_name_}.XX
    key = key.replace(/\.\d+/g,'.XX');
    if(typeof result[key] === 'undefined') {
      result[key] = {};
    }
    var type = varietyTypeOf(value);
    // p2f@20160103 start
    if (type === "Object") {
	serializeAValue(value, maxDepth, recurresult);
	result[key][type] = recurresult;
    } else {
    	result[key][type] = true;
    }
    // p2f@20160103 end
  }
  return result;
};

var mergeDocumentstrc = function(docResultstrc, interimResultsstrc) {
  for (var key in docResultstrc) {
    if(key in interimResultsstrc) {
      var existing = interimResultsstrc[key];

      for(var type in docResultstrc[key]) {
        if (type in existing.types) {
          existing.types[type] = existing.types[type] + 1;
        } else {
          existing.types[type] = 1;
        }
      }
      existing.totalOccurrences = existing.totalOccurrences + 1;
    } else {
      var types = {};
      for (var newType in docResultstrc[key]) {
        types[newType] = 1;
      }
      // p2f@20160103 start
      // interimResults[key] = {'types': types,'totalOccurrences':1};
      var value = docResultstrc[key];
      var temp = Object.keys(value);
      if (temp == 'Object') {
	// value = value[temp];
      	interimResultsstrc[key] = {'types': types,'totalOccurrences':1, value};
      } else {
      	interimResultsstrc[key] = {'types': types,'totalOccurrences':1};
      }
      // p2f@20160103 end
    }
  }
};

// p2f@20160102 start

var convertSchema = function(interimResultsstrc, documentsCount, nestedschemaset, interimResults) {
	var getuUType = function(type) {
		var TypeToNumber = {
			"Double"	:	1,
			"String" 	:	2,
			"Object"	:	3,
			"Array"		: 	4,
			"BinData"	:	5,
			"undefined"	:	6,
			"ObjectId"	:	7,
			"Boolean"	:	8,
			"Date"		:	9,
			"Null"		:	10,
			"Regex"		:	11,
			"DBPointer"	:	12,
			"JavaScript"	:	13,
			"Symbol"	:	14,
			"JSWithScope"	:	15,	
			"Number"	:	16,  // here should be modified next
			"Timestamp"	:	17,
			"NumberLong"	:	18,
			"MinKey"	:	-1,
			"MaxKey"	:	127
		};
		return TypeToNumber[type];
	}

	var getKeyType = function(type) {
		var TypeToType = {
			"Double"	:	"Float64",	
			"String" 	:	"StrZero",
			"Object"	:	"Nested",
			"Array"		: 	"Nested",
			"BinData"	:	"CarBin",
			"undefined"	:	"",
			"ObjectId"	:	"Fixed",
			"Boolean"	:	"Uint08",
			"Date"		:	"Sint64",
			"Null"		:	"",
			"Regex"		:	"TwoStrZero",
			"DBPointer"	:	"",
			"JavaScript"	:	"",
			"Symbol"	:	"StrZero",
			"JSWithScope"	:	"CarBin",	
			"Number"	:	"Sint32",  // here should be modified next
			"Timestamp"	:	"Sint64",
			"NumberLong"	:	"Sint64",
			"MinKey"	:	"",
			"MaxKey"	:	""
		};
		return TypeToType[type];
	}
	
	var columns = {};
	var differfield = {};
	var nestedindex = 1;

	function serializeCValue(value, pararesult) {
		pararesult["nested"] = {};
		// print("value " + tojson(value));
		for(var key in value) {
			var tempv = value[key];
			var type = Object.keys(tempv);
			if(type == 'Object') {
				pararesult["nested"][key] = {'types':getKeyType(type.toString()), 'uType':getuUType(type.toString())};
				serializeCValue(tempv['Object'], pararesult["nested"][key]);
			} else {
				pararesult["nested"][key] = {'types':getKeyType(type.toString()), 'uType':getuUType(type.toString())};
			}
		}
	}

	for (var key in interimResultsstrc) {
		var entry = interimResultsstrc[key];
		var nested = {};
		var typeKeys = Object.keys(entry.types).toString();
		if (entry.totalOccurrences === documentsCount) {
		  	// all the records containe the field
			if (typeKeys === "ObjectId") {
				columns[key] = {'types': getKeyType(typeKeys), 'uType': getuUType(typeKeys), 'length': 12};
			} else if (typeKeys === "Object") {
				// print("entry " + tojson(entry));
				// serializeCValue(entry["value"], nested);
				serializeCValue(entry["value"]['Object'], nested);
				var k = Object.keys(nested);
				var nested = nested[k.toString()];
				columns[key] = {'types': getKeyType(typeKeys), 'uType': getuUType(typeKeys), nested};
			} else {
				columns[key] = {'types': getKeyType(typeKeys), 'uType': getuUType(typeKeys)};
			}
		} else {
			// just some records containe the field
			if (typeKeys === "Object") {
				// object structure ==> nestedschemaset
				var tempnestedschemaset = {};
				// serializeCValue(entry["value"], tempnestedschemaset);
				serializeCValue(entry["value"]['Object'], tempnestedschemaset);
				var k = Object.keys(tempnestedschemaset);
				tempnestedschemaset = tempnestedschemaset[k.toString()];
				tempnestedschemaset["$$"] = {"type": "CarBin"};
				
				nestedschemaset[nestedindex.toString()] = {'columns': tempnestedschemaset};
				nestedindex += 1
			} else {
				// not object ==> $$
				differfield[key] = "";
			}
			
			 
		}
	}
	columns["$$"] = {"type": "CarBin", differfield};
	return columns;
}

// p2f@20160102 end

var convertResults = function(interimResults, documentsCount) {
  var getKeys = function(obj) {
    var keys = {};
    for(var key in obj) {
      keys[key] = obj[key];
    }
    return keys;
    //return keys.sort();
  };
  var varietyResults = [];
  //now convert the interimResults into the proper format
  for(var key in interimResults) {
    var entry = interimResults[key];
    varietyResults.push({
        '_id': {'key':key},
        'value': {'types':getKeys(entry.types)},
        'totalOccurrences': entry.totalOccurrences,
    });
  }
  return varietyResults;
};

// Merge the keys and types of current object into accumulator object
var reduceDocuments = function(accumulator, object) {
  var docResult = analyseDocument(serializeDoc(object, config.maxDepth));
  mergeDocument(docResult, accumulator);
  return accumulator;
};

// p2f@20160103 start
var reduceDocumentsstrc = function(accumulatorstrc, object) {
  var docResultstrc = analyseDocumentstrc(serializeDocstrc(object, config.maxDepth), config.maxDepth);
  // print("docResultstrc " + tojson(docResultstrc));
  mergeDocumentstrc(docResultstrc, accumulatorstrc);
  return accumulatorstrc;
};

// p2f@20160103 end

// We throw away keys which end in an array index, since they are not useful
// for our analysis. (We still keep the key of their parent array, though.) -JC
var filter = function(item) {
  return !item._id.key.match(/\.XX$/);
};

// sort desc by totalOccurrences or by key asc if occurrences equal
var comparator = function(a, b) {
  var countsDiff = b.totalOccurrences - a.totalOccurrences;
  return countsDiff !== 0 ? countsDiff : a._id.key.localeCompare(b._id.key);
};

// extend standard MongoDB cursor of reduce method - call forEach and combine the results
DBQuery.prototype.reduce = function(callback, initialValue) {
  var result = initialValue;
  this.forEach(function(obj){
    result = callback(result, obj);
  });
  return result;
};

var cursor = db[config.collection].find(config.query).sort(config.sort).limit(config.limit);
var cursorstrc = db[config.collection].find(config.query).sort(config.sort).limit(config.limit);

// p2f@20160102 start

// this is the part of columns
// ========================================================================
var interimResults = cursor.reduce(reduceDocuments, {});
print("orign interimResults " + tojson(interimResults));
var interimResultsstrc = cursorstrc.reduce(reduceDocumentsstrc, {});
print("new interimResults " + tojson(interimResultsstrc));


// var varietyResults = convertResults(interimResults, cursor.size());
var nestedschemaset = {};
var columns = convertSchema(interimResultsstrc, cursorstrc.size(), nestedschemaset, interimResults);
// print("columns " + tojson(columns));
// ========================================================================


// this is the part of TableIndex
// ========================================================================
var originalindex = db[config.collection].getIndexes();
var generateIndex = function(originalindex) {
	var results = [];
	for(var index in originalindex) {
    		if ("_id_" !== originalindex[index].name) {
			var field = Object.keys(originalindex[index].key);
			var part = [];
			var choice = {
				"1" : "+",
				"-1": "-",
			}			
			// 1 is up, and -1 is down
			var ordered = true;
			var unique = originalindex[index].unique;
			if (unique !== true)
				unique = false;
			for (var key in field) {
				var upordown = originalindex[index].key[field[key]];
				if (upordown === "hashed") {
					part.push(field[key]);
					ordered = false;
				} else {
					part.push(choice[upordown] + field[key]);
				}
			}
			results.push({
				'fields':part.toString(),
				'ordered':ordered,
				'unique':unique,
			});
		}
	}
	return results;
}

var tableindex = generateIndex(originalindex);
//print("tableindex " + tojson(tableindex));
// ========================================================================


// this is the result: RowSchema + TableIndex + NestedSchemaSet 
// ========================================================================
var Results = {};

Results["RowSchema"] = columns;
if (Object.keys(tableindex).length) {
	Results["TableIndex"] = tableindex;
}

if (Object.keys(nestedschemaset).length) {
	Results["NestedSchemaSet"] = nestedschemaset;
}

//print("p2f@ interimResults "+ tojson(interimResults));
// print("p2f Result "+ tojson(Results));
// ========================================================================

// p2f@20160102 end

if(config.persistResults) {
  var resultsDB;
  var resultsCollectionName = config.resultsCollection;

  if (config.resultsDatabase.indexOf('/') === -1) {
    // Local database; don't reconnect
    resultsDB = db.getMongo().getDB(config.resultsDatabase);
  } else {
    // Remote database, establish new connection
    resultsDB = connect(config.resultsDatabase);
  }

  if (config.resultsUser !== null && config.resultsPass !== null) {
    resultsDB.auth(config.resultsUser, config.resultsPass);
  }

  // replace results collection
  log('replacing results collection: '+ resultsCollectionName);
  resultsDB[resultsCollectionName].drop();
  resultsDB[resultsCollectionName].insert(varietyResults);
}

var createAsciiTable = function(results) {
  var headers = ['key', 'types', 'occurrences', 'percents'];
  // return the number of decimal places or 1, if the number is int (1.23=>2, 100=>1, 0.1415=>4)
  var significantDigits = function(value) {
    var res = value.toString().match(/^[0-9]+\.([0-9]+)$/);
    return res !== null ? res[1].length : 1;
  };

  var maxDigits = varietyResults.map(function(value){return significantDigits(value.percentContaining);}).reduce(function(acc,val){return acc>val?acc:val;});

  var rows = results.map(function(row) {
    var types = [];
    var typeKeys = Object.keys(row.value.types);
    if (typeKeys.length > 1) {
      for (var type in row.value.types) {
          var typestring = type + ' (' + row.value.types[type] + ')';
          types.push(typestring);
      }
    } else {
      types = typeKeys;
    }

    return [row._id.key, types, row.totalOccurrences, row.percentContaining.toFixed(maxDigits)];
  });
  var table = [headers, headers.map(function(){return '';})].concat(rows);
  var colMaxWidth = function(arr, index) {return Math.max.apply(null, arr.map(function(row){return row[index].toString().length;}));};
  var pad = function(width, string, symbol) { return width <= string.length ? string : pad(width, isNaN(string) ? string + symbol : symbol + string, symbol); };
  table = table.map(function(row, ri){
    return '| ' + row.map(function(cell, i) {return pad(colMaxWidth(table, i), cell.toString(), ri === 1 ? '-' : ' ');}).join(' | ') + ' |';
  });
  var border = '+' + pad(table[0].length - 2, '', '-') + '+';
  return [border].concat(table).concat(border).join('\n');
};

/*
var pluginsOutput = $plugins.execute('formatResults', varietyResults);
if (pluginsOutput.length > 0) {
  pluginsOutput.forEach(function(i){print(i);});
} else if(config.outputFormat === 'json') {
  printjson(varietyResults); // valid formatted json output, compressed variant is printjsononeline()
} else {
   print(createAsciiTable(varietyResults)); // output nice ascii table with results
}
*/
