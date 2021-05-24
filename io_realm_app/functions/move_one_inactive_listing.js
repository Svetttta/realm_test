exports = async function(changeEvent) {
  event_id = changeEvent.fullDocument['_id']
  listingKey = changeEvent.fullDocument['ListingKey']
  console.log(`Move inactive listing to history. Event ID: ${event_id} ListingKey: ${listingKey}`);
  const mongodb = context.services.get("mongodb-atlas");

  try {
    await archiveInActiveListing(mongodb);
  } catch(ex) {
    console.log("Retry to move documents to history. The first try returned error:" + ex);
    await sleep(getContextValue("retry_sec"));
    await archiveInActiveListing(mongodb);
  }
}

async function archiveInActiveListing(mongodb, listingKey){
    // Get collections
  const historyCollectionName = getContextValue("history_collection");
  const listingsCollectionName = getContextValue("listings_collection");
  
  const DB = await mongodb.db(getContextValue("db_name"));
  const historyCollection = await DB.collection(historyCollectionName);
  const listingsCollection = await DB.collection(listingsCollectionName);

  // Find inactive listing
  const listing = await listingsCollection.findOne({"ListingKey": listingKey});
  if ( typeof listing !== 'undefined' ) {
    console.log(`Listing wasn't found in listing collection: ${listingsCollectionName}.`);
    return;
  }
  
  // Move listing
  const session = mongodb.startSession();
  const transactionOptions = {
    readPreference: 'primary',
    readConcern: { level: 'local' },
    writeConcern: { w: 'majority' }
  };
  try {
    const transactionResults = await session.withTransaction(async () => {
      
      console.log(`Mongo Listing ID ${listing._id}`);
      await historyCollection.replaceOne({_id: listing._id}, listing, {"upsert": true, "session": session});
      await listingsCollection.deleteOne({_id: listing._id}, {"session": session});

    }, transactionOptions)

 
    console.log("The listing was successfully moved.");
  } catch(e){
    await session.abortTransaction();
    throw `The transaction was aborted due to an unexpected error: ${e}`;
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
