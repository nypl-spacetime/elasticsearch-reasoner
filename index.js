var fs = require('fs');
var path = require('path');
var util = require('util');
var turf = require('turf');
var elasticsearch = require('elasticsearch');
var config = require(process.env.HISTOGRAPH_CONFIG);
var query = require('./query.json');
var client = new elasticsearch.Client({
  host: config.elasticsearch.host + ':' + config.elasticsearch.port
});
var async = require('async');
var _ = require('highland');
require('colors');

var sources = [
  'bag'
  // 'tgn',
  // 'nwb.leiden'
];

function isFunction(functionToCheck) {
 var getType = {};
 return functionToCheck && getType.toString.call(functionToCheck) === '[object Function]';
}

function infer(rules, pit) {
  var centroid;
  if (pit.geometry) {
    centroid = turf.centroid(pit.geometry).geometry.coordinates;
  }

  _(Object.keys(rules))
    .map(function(toSource) {
      return rules[toSource].map(function(rule) {
        return {
          sourceid: toSource,
          rule: rule
        };
      });
    })
    .sequence()
    .filter(function(r) {
      return r.rule.types.from === pit.type || (r.rule.types.from.constructor === Array
        && r.rule.types.from.indexOf(pit.type) > -1);
    })
    .filter(function(r) {
      if (r.rule.filters && r.rule.filters.from) {
        return r.rule.filters.from(pit)
      }
      return true;
    })
    .each(function(r) {
      var rule = r.rule;

      query.query.filtered.filter.bool.must = [
        {
          term: {
            type: rule.types.to
          }
        },
        {
          term: {
            sourceid: r.sourceid
          }
        }
      ];

      if (rule.geoDistance) {
        query.query.filtered.filter.bool.must.push({
          geo_shape: {
            geometry: {
              shape: {
                type : 'circle',
                coordinates : centroid,
                radius : util.format('%dm', rule.geoDistance)
              }
            }
          }
        });
      }

      var name;
      if (isFunction(rule.name)) {
        name = rule.name(pit);
      } else if (rule.name) {
        name = rule.name;
      } else {
        name = pit.name;
      }

      var textDistance = rule.textDistance ? rule.textDistance : 0;
      query.query.filtered.query.query_string.query = util.format('%s~%d', name, textDistance);

      console.log(JSON.stringify(query))

      client.search({
        index: config.elasticsearch.index,
        type: 'pit',
        body: query
      }).then(function(resp) {
        if (resp.hits.hits.length > 0) {
          var hit = resp.hits.hits[0];
          console.log(util.format('Relation: %s -> %s', pit.name, hit._source.name).green);
          var relation = {
            from: pit.id,
            to: hit._source.hgid,
            label: rule.relation
          };
          console.log(JSON.stringify(relation));
        } else {
          console.log(util.format('No relation found: %s (id: %s)', pit.name, pit.id).red);
        }
      },

      function(err) {
        // console.log(err);
      });


      // console.log(pit)
      // voer rule uit!
      // zoek in ES naar relevante pits
      //   met name = pit.name~rule.textDistance
      //   en type IN rule.types.to
      //   en afstand pit.geom.centroid < rule.geoDistance
      //   turf.centroid(poly);
      // output relaties! (Naar bestand of naar neo!)
      //console.log(rules, pit);

    })
}

function hasLength(str) {
  return str.length > 0;
}

// TODO: remove async, use highland only
async.eachSeries(sources, function(source, callback) {
  var through = _.pipeline(
    _.split(),
    _.filter(hasLength),
    _.map(JSON.parse)
  );

  var rules = require('./' + source + '.rules');
  var filename = path.join('..', 'data', 'bag', 'bag.place.pits.ndjson');
  // Dit is goede:
  // var filename = path.join(config.api.dataDir, 'sources', source, 'current', 'pits.ndjson');
  fs.createReadStream(filename, {encoding: 'utf8'})
      .pipe(through)
      .each(_.curry(infer, rules))
      .done(callback);
});

