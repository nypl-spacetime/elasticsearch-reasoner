var fs = require('fs');
var path = require('path');
var util = require('util');
var turf = require('turf');
var _ = require('highland');
var elasticsearch = require('elasticsearch');
var config = require(process.env.HISTOGRAPH_CONFIG);
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

function getUriOrId(source, uri, id) {
  var result = uri;
  if (id) {
    result = id;
    if (id.indexOf('/') > -1) {
      result = source + '/' + id;
    }
  }
  return result;
}

function ensureDir(inferredDir, source) {
  try {
    fs.mkdirSync(path.join('.', inferredDir, util.format('%s.%s', source, inferredDir)));
  } catch(e) {
    // Ha! Do nothing!
  }
}

function createWriteStream(inferredDir, source, str) {
  return fs.createWriteStream(path.join('.', inferredDir, util.format('%s.%s', source, inferredDir), util.format('%s.%s.%s', source, inferredDir, str)), {encoding: 'utf8'});
}

function infer(data, callback) {
  var rule = data.rule;
  var pit = data.pit;
  var urn = normalize(pit.id || pit.uri, data.ruleSourceid);

  // Check if PIT's URI or ID is present in rule's override list
  if (data.normalizedOverride[urn] !== undefined) {
    if (data.normalizedOverride[urn]) {

      var relation = {
        from: getUriOrId(data.source, pit.uri, pit.id),
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
      index: data.ruleSourceid,
      //type: 'hg:Place',
      body: query
    }).then(function(res) {
      var result = {};

      if (res.hits.hits.length > 0) {
        var hit = res.hits.hits[0];

        var relation = {
          from: getUriOrId(data.source, pit.uri, pit.id),
          to: getUriOrId(data.ruleSourceid, hit._source.uri, hit._source.id),
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

function inferSource(obj, callback) {
  var source = obj.source
  var rules = require('./' + obj.filename);
  var pitsFile = path.join(config.api.dataDir, 'sources', source, 'current', 'pits.ndjson');
  var inferredDir = 'inferred';

  ensureDir(inferredDir, source);

  var relationsStream = createWriteStream(inferredDir, source, 'relations.ndjson');
  var errorsStream = createWriteStream(inferredDir, source, 'errors.ndjson');
  var logStream = createWriteStream(inferredDir, source, 'log');

  var sourceMeta = {
    title: util.format('%s (inferred)', obj.source),
    author: 'Histograph Reasoning Engine 🚀',
    id: util.format('%s.%s', obj.source, inferredDir),
    description: 'Created by Histograph Reasoning Engine',
    license: 'MIT'
  };

  var metaStream = createWriteStream(inferredDir, source, 'source.json');
  metaStream.write(JSON.stringify(sourceMeta, null, 2));

  var stream = _(fs.createReadStream(pitsFile, {encoding: 'utf8'}))
    .split()
    .compact()
    .map(JSON.parse)
    .map(function(pit) {
      return _(Object.keys(rules)).map(function(ruleSourceid) {
        return _(rules[ruleSourceid])
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
                var from = normalize(o.from, obj.source);
                var to = {
                  to: o.to
                };

                if (o.to) {
                  to.urn = normalize(o.to, ruleSourceid);
                }

                normalizedOverride[from] = to;
              });
            }

            return {
              source: obj.source,
              pit: pit,
              ruleSourceid: ruleSourceid,
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
      source: parts[0],
      filename: path.join('.', rulesDir, filename)
    };
  })
  .filter(function(obj) {
    return argv._.length === 0 || argv._.indexOf(obj.source) > -1;
  })
  .map(function(obj) {
    return _.curry(inferSource, obj);
  })
  .nfcall([])
  .series()
  .done(function() {
    client.close();
  });
