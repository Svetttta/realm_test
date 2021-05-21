exports = async function() {
  console.log("Move inactive listings to history");
  const mongodb = context.services.get("mongodb-atlas");

  try {
    await archiveInActiveListings(mongodb);
  } catch(ex) {
    console.log(`Retry to move documents to history. The first try returned error: ${ex}`);
    await sleep(getContextValue("retry_sec"));
    await archiveInActiveListings(mongodb);
  }
}

async function archiveInActiveListings(mongodb){
    // Get collections
  const historyCollectionName = getContextValue("history_collection");
  const listingsCollectionName = getContextValue("listings_collection");
  const statusCollectionName = getContextValue("status_collection");
  
  const DB = await mongodb.db(getContextValue("db_name"));
  const historyCollection = await DB.collection(historyCollectionName);
  const listingsCollection = await DB.collection(listingsCollectionName);
  const statusCollection = await DB.collection(statusCollectionName);

  // Find all inactive listing keys
  const result = (await statusCollection.find({"is_active": false, "IdType": "ListingID"}, {"ListingKey" : 1 }).toArray())
  const inactive_listing_keys = result.map(item => item.ListingKey);

  const keys_amount = inactive_listing_keys.length;
  console.log(`${keys_amount} was/were found in the status collection: ${statusCollectionName}.`);
  if(keys_amount <= 0) {
    return;
  }
  
  // Find all inactive listings
  const inactive_listings = await listingsCollection.find({"ListingKey": {"$in": inactive_listing_keys}}).toArray();
  const docs_amount = inactive_listings.length;
  console.log(`${docs_amount} document(s) was/were found in listing collection: ${listingsCollectionName}.`);
  if(docs_amount <= 0) {
    return;
  }

  let ids = []
  for(let index = 0; index < docs_amount; index++) {
    ids.push(inactive_listings[index]._id);
  }

  // Move all inactive listings
  const session = mongodb.startSession();
  const transactionOptions = {
    readPreference: 'primary',
    readConcern: { level: 'local' },
    writeConcern: { w: 'majority' }
  };
  try {
    const transactionResults = await session.withTransaction(async () => {
      
      const bulkData = inactive_listings.map(item => (
        {
          replaceOne: {
            upsert: true,
            filter: {_id: item._id},
            replacement: item
          }
        }
      ));
      const bulkWriteResult = await historyCollection.bulkWrite(bulkData, {session});

      const deleteResult = await listingsCollection.deleteMany({_id: {$in: ids}}, {session});

    }, transactionOptions)

 
    console.log("The listings were successfully moved.");
  } catch(e){
    await session.abortTransaction();
    console.log("The transaction was aborted due to an unexpected error: " + e);
  } finally {
    await session.endSession();
  }

}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getContextValue(valueName) {
    const value = context.values.get(valueName);
    if(value === undefined || value === null) {
        throw new Error(`Cannot fetch required value ${valueName} from realm context`);
    }
    return value
}
