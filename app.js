const BigQuery = require('@google-cloud/bigquery');
const Datastore = require('@google-cloud/datastore');
const language = require('@google-cloud/language');
const Twitter = require('twitter');
const config = require('./config.json');

const language_client = new language.LanguageServiceClient({
  keyFilename: './datastore.json'
});

const bigquery = new BigQuery({
  projectId: config.project_id,
  keyFilename: './datastore.json'
});
const table = bigquery
  .dataset(config.bigquery_dataset)
  .table(config.bigquery_table);

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
      const entity = results[0].entities.sort(function(a, b) {
        return b.salience - a.salience;
      })[0];

      const tweet = {
        created_at: event.created_at,
        id: event.id,
        text: event.text,
        geo: event.geo,
        coordinates: event.coordinates,
        place: event.place,
        sentiment_score: sentiment.score || 0,
        sentiment_magnitude: sentiment.magnitude || 0,
        entity: entity.name,
        entity_type: entity.type,
        entity_salience: entity.salience,
      };
      saveTask(tweet);
    }).catch(err => {
      console.error('ERROR:', err);
    })
  }).catch(err => {
    console.error('ERROR:', err);
  })
}

function saveTask(tweet) {
  table.insert(tweet)
  .then(function(data) {
    var apiResponse = data[0];
    console.log(apiResponse);
    console.log(tweet);
  })
  .catch(function(err) {
    console.log(err);
  });
}
