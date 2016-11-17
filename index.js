/* 
Copyright (C) 2016 Jason Lin infwonder<AT>gmail<DOT>com

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

'use strict';

const fs = require('fs');
const protobuf = require('protocol-buffers');
const C = require('crypto');
const lupus = require('lupus');
const mkdirp = require('mkdirp');

module.exports = 
{
  protofile: __dirname + '/HaaS.proto',
  chunkdir: undefined,
  metadir: undefined,
  outdir: undefined,
  schemas: undefined,
  cfgobj: undefined,

  load_schemas: function() 
  {
    // still need to add cfgid checking against cfgobj
    if (module.exports.cfgobj === undefined || module.exports.cfgobj.isa !== 'evenly-configs') throw "Need to obtain evenly-configs object";
    if (module.exports.cfgobj.configs === undefined) module.exports.cfgobj.load_config();

    module.exports.chunkdir = module.exports.cfgobj.chunkdir;
    module.exports.metadir = module.exports.cfgobj.metadir;
    module.exports.outdir = module.exports.cfgobj.outdir;

    var path = module.exports.protofile;
    module.exports.schemas = protobuf(fs.readFileSync(path));
  },

  inspect_pbuf_file: function(path) // utility function for debugging
  {
    if (module.exports.schemas === undefined) {
      console.log("schemas not found, loading " + module.exports.protofile + "...");
      module.exports.load_schemas(module.exports.protofile); 
    };

    fs.readFile(path, (err, buf) => {
      if (err) throw err;

      var obj = module.exports.schemas.haasmesg.decode(buf);
      console.log(obj); // dump to screen
    });
  },

  dump_pbuf_on_screen: function(fhash, chash, pbuf) // utility function for debugging
  {
    if (module.exports.schemas === undefined) {
      console.log("schemas not found, loading " + module.exports.protofile + "...");
      module.exports.load_schemas(module.exports.protofile); 
    };

    console.log(module.exports.schemas.haasmesg.decode(pbuf));
  },

  dump_pbuf_as_chunk: function(fhash, chash, pbuf) 
  {
    if (module.exports.schemas === undefined) {
      console.log("schemas not found, loading " + module.exports.protofile + "...");
      module.exports.load_schemas(module.exports.protofile); 
    };

    mkdirp(module.exports.chunkdir + '/' + fhash, (err) => {
      if (err) throw err;
      var path = module.exports.chunkdir + '/' + fhash + '/' + chash;
      fs.writeFile(path, pbuf, (err) => {if (err) throw err});
    });
  },

  where_to: function(cpath) // still need to add cfgid checking against cfgobj
  {
    if (cpath === undefined) throw "Need to provide chunk hash";
    if (module.exports.cfgobj === undefined || module.exports.cfgobj.isa !== 'evenly-configs') throw "Need to obtain evenly-configs object";

    var cfgobj = module.exports.cfgobj;
    var [cfgid, hid, chash] = cpath.split('-');
    if (cfgobj.cfgid !== cfgid) throw "mismaching config id... abort";
    var target = (cfgobj.hostlist.filter((h) => { return cfgobj.nodeparts[h].indexOf(hid) !== -1 }))[0];

    return {[cpath]: target};
  },

  bucket_path: function(chash) // still need to add cfgid checking against cfgobj
  {
    if (chash === undefined) throw "Need to provide chunk hash";
    if (module.exports.cfgobj.isa !== 'evenly-configs') throw "Need to obtain evenly-configs object";
    if (module.exports.cfgobj.configs === undefined) module.exports.cfgobj.load_config();

    var cfgobj = module.exports.cfgobj;
    var hid = cfgobj.hashs[chash.substr(0,cfgobj.configs.ringsize)];
    var cpath = cfgobj.cfgid + '-' + hid + '-' + chash;
    return cpath; 
  },

  chunk_file: function(path, chunk_size, store) 
  {
    store = (store == 'true'); // http://heyjavascript.com/javascript-string-to-boolean/

    if (module.exports.schemas === undefined) {
      console.log("schemas not found, loading " + module.exports.protofile + "...");
      module.exports.load_schemas(module.exports.protofile); 
    };

    if (module.exports.cfgobj.isa !== 'evenly-configs') throw "Need to obtain evenly-configs object";
    if (module.exports.cfgobj.configs === undefined) module.exports.cfgobj.load_config();
    var cfgobj = module.exports.cfgobj;

    chunk_size = chunk_size || 512; // default

    var stats = fs.statSync(path);

    // Calculating whole file chekcsum for chunk meta
    var fstream = fs.createReadStream(path);
    var fhash   = C.createHash('md5');
    var filemd5;

    fstream.pipe(fhash);

    fhash.on('readable', () => {
      var data = fhash.read();
      if (data) {
        filemd5 = data.toString('hex');
        console.log('Check sum: ' + filemd5);
        var buff = Buffer.alloc(chunk_size);
    
        // start building chunks here to make sure it is only done after filemd5 is create!
        // Chopping file into chunks and fit them in protobuf messages
        fs.open(path, 'r', (err, fd) => 
        {
          if (err) throw err;
      
          function readchunk(count, meta) 
          {
            fs.read(fd, buff, 0, chunk_size, null, (err, nread, buff) =>
            {
              if (err) throw err;
      
              if (nread === 0)
              {
                // Done reading from fd. Close it.
                fs.close(fd, (err) => { if (err) throw err });
         
                // Finalize and store meta
                meta.count = count-1;
                var pbuf = module.exports.schemas.haasmesg.encode(meta);
                fs.writeFile(module.exports.metadir + '/' + filemd5 + ".meta", pbuf, (err) => { if (err) throw err } );
                console.log("meta data " + filemd5 + ".meta written.");
        
                return;
              }
        
              var data;
        
              if (nread < chunk_size)
              {
                data = buff.slice(0, nread);
              } else {
                data = buff;
              }
        
              var chunksum = C.createHash('md5').update(data).digest('hex');
              var hid = cfgobj.hashs[chunksum.substr(0,cfgobj.configs.ringsize)];

              meta.piece.push({ part: count, size: nread, hash: chunksum });
        
              if (store === false) { // this is for directly sending result to network. Thus all header are attached and ready to send
                var pbuf = module.exports.schemas.haasmesg.encode({
                  type: 'file',
                  name: path,
                  hash: filemd5,
                  size: stats.size,
                 piece: [ {part: count, size: nread, hash: chunksum, data: data} ]
                });

                fs.writeFile(module.exports.chunkdir + '/' + filemd5 + '-' + chunksum, pbuf, (err) => { if (err) throw err });
              } else { // this is for actual storage that allows proper data deduplication 
                fs.writeFile(module.exports.chunkdir + '/data/' + chunksum, data, (err) => { if (err) throw err });

                var pbJSON = {
                  type: 'file',
                  name: path,
                  hash: filemd5,
                  size: stats.size,
                 piece: [ {part: count, size: nread, hash: chunksum} ] 
                };

                fs.writeFile(module.exports.chunkdir + '/head/' + filemd5 + '-' + chunksum, JSON.stringify(pbJSON), (err) => { if (err) throw err });
              }

              var cpath = cfgobj.cfgid + '-' + hid + '-' + chunksum;

              console.log("Chunk: ", module.exports.where_to(cpath));

              count = count + 1;
              readchunk(count, meta);
            });
          }
        
          // Preparing protobuf message for readchunk
          var meta = { type: 'meta', name: path, hash: filemd5, size: stats.size, csize: chunk_size, piece: [] };
          readchunk(1, meta);
        });
        
        console.log("Awaiting results from chunking " + path + "...");
      }
    });
  },

  mobilize_chunk: function(fhash, chash, callback) {
    if (fhash === undefined || chash === undefined) throw "Need to specify both file hash and chunk hash... ";

    if (module.exports.schemas === undefined) {
      console.log("schemas not found, loading " + module.exports.protofile + "...");
      module.exports.load_schemas(module.exports.protofile); 
    };

    fs.readFile(module.exports.chunkdir + '/data/' + chash, (err, data) => {
      if (err) throw err;

      var pbJSON = JSON.parse(fs.readFileSync(module.exports.chunkdir + '/head/' + fhash + '-' + chash));

      pbJSON.piece[0].data = data;
      var pbuf = module.exports.schemas.haasmesg.encode(pbJSON);
      
      return callback(fhash, chash, pbuf);
    });
  },

  join_chunks: function(metapath) // join from just downloaded chunks with headers... this should be more commonly used.
  {
    if (module.exports.schemas === undefined) {
      console.log("schemas not found, loading " + module.exports.protofile + "...");
      module.exports.load_schemas(module.exports.protofile); 
    };
   
    fs.readFile(module.exports.metadir + '/' + metapath, (err, meta) => {
      if (err) throw err;
    
      var mobj  = module.exports.schemas.haasmesg.decode(meta);
      var bsize = mobj.size;
      var buff  = Buffer.alloc(bsize);
      var name  = mobj.hash;
      var totalcount = mobj.count;
      var offset;
    
      fs.writeFile(module.exports.outdir + '/_b_' + name, buff, (err) => {
        if (err) throw err;
    
        var fd = fs.openSync(module.exports.outdir + '/_b_' + name, 'r+');
        var actualcount = totalcount - 1;
        var bytewritten = 0;
    
        lupus (0, totalcount, (i) => {
          var obj = mobj.piece[i];
          var chash = obj.hash;
          var offset = i * mobj.csize;
          var csize = obj.size;
    
          fs.readFile(module.exports.chunkdir + '/' + name + '/' + chash, (err, cbuf) => {
            if (err) throw err;

            var data = module.exports.schemas.haasmesg.decode(cbuf);
    
            fs.write(fd, data.piece[0].data, 0, csize, offset, (err, written) => {
              if (err) throw err;
              console.log( written + " bytes written for chunk " + i + "/" + actualcount);
              bytewritten = bytewritten + written;
              if (bytewritten === bsize) fs.close(fd);
            }); // End of fs.write (data)
          });   // End of fs.readFile (chunk)
        });     // End of lupus call
      });       // End of fs.writeFile (buff)
    });         // End of fs.readFile (metapath)
  }, // Callback hell version, but it's good for now
};
