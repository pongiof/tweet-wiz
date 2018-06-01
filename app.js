const Datastore = require('@google-cloud/datastore');
const language = require('@google-cloud/language');
const Twitter = require('twitter');
const config = require('./config.json');

const language_client = new language.LanguageServiceClient({
  keyFilename: './datastore.json'
});

const datastore = new Datastore({
  projectId: config.project_id,
  keyFilename: './datastore.json'
});



const client = new Twitter({
  consumer_key: config.twitter_consumer_key,
  consumer_secret: config.twitter_consumer_secret,
  access_token_key: config.twitter_access_key,
  access_token_secret: config.twitter_access_secret
});

client.stream('statuses/filter', {track: config.searchTerms, language: 'it'}, function(stream) {
  stream.on('data', function(event) {
    processTweet(event);
  });
  stream.on('ERROR', function(error) {
    console.log('Twitter api error: ' + error)
  });
});

function processTweet(event) {
  if ((event.text != undefined) && (event.text.substring(0,2) != 'RT')) {
    analyzeTweet(event);
  }
}

function analyzeTweet(event) {
  const document = {
    content: event.text,
    type: 'PLAIN_TEXT',
  };
  language_client.analyzeSentiment({document: document}).then(results => {
    const sentiment = results[0].documentSentiment || null;
    language_client.analyzeEntities({document: document}).then(results => {
      const entities = results[0].entities.map(function(e) {
        return {
          name: e.name,
          type: e.type,
          salience: e.salience};
      });
      const task = {
        key: datastore.key(['Tweet', event.id]),
        data: {
          created_at: event.created_at,
          id: event.id,
          text: event.text,
          geo: event.geo,
          coordinates: event.coordinates,
          place: event.place,
          sentiment_score: sentiment.score || 0,
          sentiment_magnitude: sentiment.magnitude || 0,
          entities: entities
        },
      };
      saveTask(task);
    }).catch(err => {
      console.error('ERROR:', err);
    })
  }).catch(err => {
    console.error('ERROR:', err);
  })
}

function saveTask(task) {
  datastore.save(task).then(() => {
    console.log(task.data);
  }).catch(err => {
    console.error('ERROR:', err);
  });
}
