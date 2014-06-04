var port = process.env.PORT || 6461
var fs = require('fs')
var Dat = require('dat')
var bcsv = require('binary-csv')

require('./geocoder')(function(err, geocoderStream) {
  if (err) throw err
  
  var dat = new Dat('./data', function ready(err) {
    if (err) throw err
    load()
    
    dat.listen(port, function(err) {
      console.log('listening on', port)
    })
  })
  
  function load() {
    console.log(JSON.stringify({
      "starting": new Date(),
      "message": "Geocoding CSV and loading into Dat"
    }))
    
    var csv2json = bcsv({ json: true })
    
    var geocoder = geocoderStream(function formatter(obj) {
      var addr = obj['ADDR']
      console.log("ADDR:", addr)
      return addr
    })
    
    var writeStream = dat.createWriteStream({
      objects: true
    })
    
    var input = fs.createReadStream('./missAddr_140331.csv')
    
    input.pipe(csv2json).pipe(geocoder).pipe(writeStream)
    
    input.on('error', function(e) {
      console.log({'HTTPERR': e})
    })
    
    writeStream.on('end', function() {
      console.log(JSON.stringify({
        "finished": new Date()
      }))
    })
  }
})
