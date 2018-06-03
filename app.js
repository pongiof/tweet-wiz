const BigQuery = require('@google-cloud/bigquery');
const Datastore = require('@google-cloud/datastore');
const language = require('@google-cloud/language');
const Twitter = require('twitter');
const config = require('./config.json');

const language_client = new language.LanguageServiceClient({
  keyFilename: './key.json'
});

const bigquery = new BigQuery({
  projectId: config.project_id,
  keyFilename: './key.json'
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

const allFollows = [...config.followLega,
    ...config.followM5S,
    ...config.followPD,
    ...config.followForzaItalia,
    ...config.followManganelliDitalia];

const allIdsRequests = allFollows.map(async (item) => {
       response = await client.get('users/show', {screen_name:item});
       return response.id;
     });

Promise.all(allIdsRequests)
  .then(ids => {
    const filter = ids.join(',');
    client.stream('statuses/filter', {follow: filter, language: 'it'}, function(stream) {
      stream.on('data', function(event) {
        processTweet(ids, event);
      });
      stream.on('ERROR', function(error) {
        console.log('Twitter api error: ' + error)
      });
    });
  });


function processTweet(ids, event) {
  if ((event.text == undefined) || (event.text.substring(0,2) == 'RT')) return;
  analyzeTweet(event);
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
        created_ms: Math.round(event.timestamp_ms / 1000),
        id: event.id,
        text: event.text,
        in_reply_to_screen_name: event.in_reply_to_screen_name,
        screen_name: event.user.screen_name,
        retweet_count: event.retweet_count,
        reply_count: event.reply_count,
        favorite_count: event.favorite_count,
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
    console.log(tweet);
  })
  .catch(function(err) {
    if (err.name === 'PartialFailureError') {
      console.log(err.errors[0].errors)
    }
    console.log('ERRORE');
    console.log(err);
  });
}
