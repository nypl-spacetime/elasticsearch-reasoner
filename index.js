var fs = require('fs');
var path = require('path');
var util = require('util');
var turf = require('turf');
var _ = require('highland');
var elasticsearch = require('elasticsearch');
var config = require('histograph-config');
var query = require('./query.json');
var normalize = require('histograph-uri-normalizer').normalize;
var client = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
});
var argv = require('minimist')(process.argv.slice(2));
require('colors');

function isFunction(functionToCheck) {
  var getType = {};
  return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}

function getUriOrId(dataset, uri, id) {
  var result = uri;
  if (id) {
    result = id;
    if (id.toString().indexOf('/') > -1) {
      result = dataset + '/' + id;
    }
  }
  return result;
}

function ensureDir(inferredDir, dataset) {
  try {
    fs.mkdirSync(path.join('.', inferredDir, util.format('%s.%s', dataset, inferredDir)));
  } catch(e) {
    // Ha! Do nothing!
  }
}

function createWriteStream(inferredDir, dataset, str) {
  return fs.createWriteStream(path.join('.', inferredDir, util.format('%s.%s', dataset, inferredDir), util.format('%s.%s.%s', dataset, inferredDir, str)), {encoding: 'utf8'});
}

function infer(data, callback) {
  var rule = data.rule;
  var pit = data.pit;
  var urn = normalize(pit.id || pit.uri, data.ruleDatasetid);

  // Check if PIT's URI or ID is present in rule's override list
  if (data.normalizedOverride[urn] !== undefined) {
    if (data.normalizedOverride[urn]) {

      var relation = {
        from: getUriOrId(data.dataset, pit.uri, pit.id),
        to: data.normalizedOverride[urn].to,
        type: rule.relation
      };

      var result = {
        relation: relation,
        from: pit,
        to: {
          override: data.normalizedOverride[urn].to
        }
      };

      callback(null, result);

    } else {
      callback(null, {});
    }
  } else {

    // Create Elasticsearch query
    var centroid;
    if (pit.geometry) {
      centroid = turf.centroid(pit.geometry).geometry.coordinates;
    }

    query.query.filtered.filter.bool.must = [
      {
        term: {
          type: rule.types.to
        }
      }
    ];

    if (rule.geoDistance) {
      query.query.filtered.filter.bool.must.push({
        geo_shape: {
          geometry: {
            shape: {
              type: 'circle',
              coordinates: centroid,
              radius: util.format('%dm', rule.geoDistance)
            }
          }
        }
      });
    }

    // TODO: alles in rules: of default, of constant, of function(pit)

    var name;
    if (isFunction(rule.name)) {
      name = rule.name(pit);
    } else if (rule.name) {
      name = rule.name;
    } else {
      name = pit.name;
    }

    name = name.replace(')', '').replace('(', '');

    var textDistance = rule.textDistance ? rule.textDistance : 0;
    query.query.filtered.query.query_string.query = util.format('%s~%d', name, textDistance);

    client.search({
      index: data.ruleDatasetid,
      //type: 'hg:Place',
      body: query
    }).then(function(res) {
      var result = {};

      if (res.hits.hits.length > 0) {
        var hit = res.hits.hits[0];

        var relation = {
          from: getUriOrId(data.dataset, pit.uri, pit.id),
          to: getUriOrId(data.ruleDatasetid, hit._source.uri, hit._source.id),
          type: rule.relation
        };

        result = {
          relation: relation,
          to: hit._source
        };
      }

      result.from = pit;
      callback(null, result);
    },

    function(error) {
      callback(error);
    });
  }
}

function inferDataset(obj, callback) {
  var dataset = obj.dataset
  var rules = require('./' + obj.filename);
  var pitsFile = path.join(config.api.dataDir, 'datasets', dataset, 'current', 'pits.ndjson');
  var inferredDir = 'inferred';

  ensureDir(inferredDir, dataset);

  var relationsStream = createWriteStream(inferredDir, dataset, 'relations.ndjson');
  var errorsStream = createWriteStream(inferredDir, dataset, 'errors.ndjson');
  var logStream = createWriteStream(inferredDir, dataset, 'log');

  var datasetMeta = {
    title: util.format('%s (inferred)', obj.dataset),
    author: 'Histograph Reasoner 🚀',
    id: util.format('%s.%s', obj.dataset, inferredDir),
    description: 'Created by Histograph Reasoner',
    license: 'GPL-3.0'
  };

  var metaStream = createWriteStream(inferredDir, dataset, 'dataset.json');
  metaStream.write(JSON.stringify(datasetMeta, null, 2));

  var stream = _(fs.createReadStream(pitsFile, {encoding: 'utf8'}))
    .split()
    .compact()
    .map(JSON.parse)
    .map(function(pit) {
      return _(Object.keys(rules)).map(function(ruleDatasetid) {
        return _(rules[ruleDatasetid])
          .filter(function(rule) {
            // Filter on PIT type for which rule is defined
            return rule.types.from === pit.type ||
              (rule.types.from.constructor === Array && rule.types.from.indexOf(pit.type) > -1);
          })
          .filter(function(rule) {
            // Apply the rule's general filter function (if defined)
            if (rule.filter && isFunction(rule.filter)) {
              return rule.filter(pit);
            }

            return true;
          })
          .map(function(rule) {

            // Normalize URLs and IDs in rule's override list to URNs
            // using histograph-uri-normalizer
            var normalizedOverride = {};
            if (rule.override) {
              rule.override.forEach(function(o) {
                var from = normalize(o.from, obj.dataset);
                var to = {
                  to: o.to
                };

                if (o.to) {
                  to.urn = normalize(o.to, ruleDatasetid);
                }

                normalizedOverride[from] = to;
              });
            }

            return {
              dataset: obj.dataset,
              pit: pit,
              ruleDatasetid: ruleDatasetid,
              rule: rule,
              normalizedOverride: normalizedOverride
            };
          });
      });
    })
    .flatten()
    .map(function(data) {
      return _.curry(infer, data);
    })
    .nfcall([])
    .parallel(10)
    .errors(function(err) {
      console.log(err);
    });

  stream
    .fork()
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(logStream)
    .on('close', function() {
      callback();
    });

  stream
    .fork()
    .pluck('relation')
    .compact()
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(relationsStream);

  stream
    .fork()
    .filter(function(obj) {
      return !obj.relation;
    })
    .pluck('from')
    .compact()
    .map(JSON.stringify)
    .intersperse('\n')
    .pipe(errorsStream);
}

var readDir = _.wrapCallback(fs.readdir);
var readFile = _.wrapCallback(function(filename, callback) {
  return fs.readFile(filename, {encoding: 'utf8'}, callback);
});

var rulesDir = 'rules';

var rulesFiles = readDir(path.join('.', rulesDir))
  .flatten();

_(rulesFiles)
  .map(function(filename) {
    var parts = filename.split('.');
    return {
      dataset: parts[0],
      filename: path.join('.', rulesDir, filename)
    };
  })
  .filter(function(obj) {
    return argv._.length === 0 || argv._.indexOf(obj.dataset) > -1;
  })
  .map(function(obj) {
    return _.curry(inferDataset, obj);
  })
  .nfcall([])
  .series()
  .done(function() {
    client.close();
  });
