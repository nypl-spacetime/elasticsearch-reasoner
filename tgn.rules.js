module.exports = {

  // geonames: [
  //   {
  //     types: {
  //       from: 'hg:Place',
  //       to: 'hg:Place'
  //     },
  //     geoDistance: 4000,
  //     textDistance: 2,
  //     relation: 'hg:sameHgConcept',
  //     filters: {
  //       from: function(pit) {
  //         // Do not use TGN term objects
  //         return pit.uri.indexOf('term') == -1;
  //       },
  //       to: null
  //     }
  //   },
  //
  //   {
  //     types: {
  //       from: 'hg:Province',
  //       to: 'hg:Province'
  //     },
  //     textDistance: 1,
  //     relation: 'hg:sameHgConcept',
  //     filters: {
  //       from: function(pit) {
  //         // Do not use TGN term objects
  //         return pit.uri.indexOf('term') == -1;
  //       },
  //       to: null
  //     }
  //   },
  //
  //   {
  //     types: {
  //       from: 'hg:Country',
  //       to: 'hg:Country'
  //     },
  //     textDistance: 0,
  //     name: function(pit) {
  //       return 'Kingdom of the Netherlands';
  //     },
  //     relation: 'hg:sameHgConcept',
  //     filters: {
  //       from: function(pit) {
  //         // Do not use TGN term objects
  //         return pit.uri.indexOf('term') == -1;
  //       },
  //       to: null
  //     }
  //   }
  //
  // ]
}