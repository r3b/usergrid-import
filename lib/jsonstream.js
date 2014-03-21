var util = require('util');
var StringDecoder = require('string_decoder').StringDecoder;
var Transform = require('stream').Transform;
util.inherits(JSONParseStream, Transform);

function JSONParseStream(options) {
  if (!(this instanceof JSONParseStream))
    return new JSONParseStream(options);

  Transform.call(this, options);
  this._writableState.objectMode = false;
  this._readableState.objectMode = true;
  this._buffer = '';
  this._decoder = new StringDecoder('utf8');
}

JSONParseStream.prototype._transform = function(chunk, encoding, cb) {
  this._buffer += this._decoder.write(chunk);
  // split on newlines
  var lines = this._buffer.split(/\r?\n/);
  // keep the last partial line buffered
  this._buffer = lines.pop();
  for (var l = 0; l < lines.length; l++) {
    var line = lines[l];
    try {
      var obj = JSON.parse(line);
      // push the parsed object out to the readable consumer
      this.push(obj);
    } catch (er) {
      this.buffer='';
    }
  }
  cb();
};

JSONParseStream.prototype._flush = function(cb) {
  var rem = this._buffer.trim();
  if (rem) {
    try {
      var obj = JSON.parse(rem);
      this.push(obj);
    } catch (er) {
      console.error("unable to parse remaining buffer after flush");
    }
  }
  cb();
};
module.exports=JSONParseStream;
