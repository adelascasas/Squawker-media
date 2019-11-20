const mongoose = require('mongoose');
const cassandra = require('cassandra-driver');




mongoose.connect("mongodb://130.245.168.250/squawker",{ useNewUrlParser: true, useUnifiedTopology: true});
const db = mongoose.connection;

const casClient = new cassandra.Client({ contactPoints: ['192.168.122.30'], keyspace: 'media',localDataCenter:'datacenter1'});
casClient.connect(function (err) {
  if(err) { console.log(err);}
  else{
      console.log('connected');
  }
});
  
db.once('open', () => { 
    mongoose.model("users",mongoose.Schema({
           _id: String,
           username: String,
           password: String,
           email:String,
           verified: Boolean, 
           key: String,
           followers: [{ type: String}],
           following: [ { type: String}],
           media: [{type: String}]
       }),"users"); 
    mongoose.model('blacklist', mongoose.Schema({
           _id: String,
           token: String
       }),'blacklist');
});

//Add media
const addMedia = (id,blob,extension) => {
    const query = 'INSERT INTO media (id, content,extension) VALUES (?, ?, ?) USING TTL ?';
    const params = [id,blob,extension,1200];
    return casClient.execute(query, params, { prepare: true });
}

//Delete media
const delMedia = (id) => {
    const query = 'DELETE FROM media WHERE id = ?';
    const param = [id];
    return casClient.execute(query,param,{prepare: true});
}

//Get media 
const getMedia = (id) => {
    const query = 'SELECT content,extension FROM media WHERE id = ?';
    return casClient.execute(query, [id],{prepare: true});
} 

module.exports = {
    addMedia,
    getMedia,
    delMedia
};
