module AWS.Dynamo
    ( invoke
    , DYNAMO
    , StreamPayload(..)
    , StreamEvent(..)
    , DynamoDBRecord(..)
    , PutParams
    , put
    , QueryParams
    , defaultQuery
    , QueryResult(..)
    , query
    , Dynaff
    , module AWS.Dynamo.Internal
    )
where

import Prelude
import AWS.Dynamo.Internal
import AWS.Dynamo.Classes (dynamoRead, class FromDynamo)
import Control.Monad.Aff (Aff, makeAff)
import Control.Monad.Eff (Eff)
import Control.Monad.Eff.Exception (Error, error)
import Control.Monad.Except (runExcept, withExcept)
import Data.Either (Either, either)
import Data.Foreign (Foreign, writeObject)
import Data.Foreign.Class (class AsForeign, class IsForeign, read, write, writeProp, readProp)
import Data.Foreign.Generic (defaultOptions, readGeneric)
import Data.Foreign.Undefined (Undefined)
import Data.Function.Uncurried (Fn4, runFn4)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe(..), maybe)

foreign import data DYNAMO :: !

foreign import invokeForeign
    :: forall eff. Fn4 String
                       Foreign
                       (Error -> Eff (dynamo :: DYNAMO | eff) Unit)
                       (Foreign -> Eff (dynamo :: DYNAMO | eff) Unit)
                       (Eff (dynamo :: DYNAMO |eff) Unit)

type Dyneff eff = Eff (dynamo :: DYNAMO | eff) Unit

invoke' :: forall a b eff . (AsForeign a, IsForeign b)
    => String -> a -> (Error -> Dyneff eff) -> (b -> Dyneff eff) -> Dyneff eff
invoke' name params errcb cb = runFn4 invokeForeign name (write params) errcb (either errcb cb <<< decode)
    where
        decode :: forall f. (IsForeign f) => Foreign -> Either Error f
        decode f = runExcept $ withExcept (error <<< renderErrors) $ read f

type Dynaff eff r = Aff (dynamo :: DYNAMO | eff) r

invoke :: forall a b eff. (AsForeign a, IsForeign b) => String -> a -> Dynaff eff b
invoke methodName = makeAff <<< invoke' methodName


newtype DynamoDBRecord a = DynamoDBRecord
    { creationTimeStamp :: Number
    , keys :: Foreign
    , newImage :: a
    , sequenceNumber :: String
    , sizeBytes :: Int
    }


instance dynamoDbRecordIsForeign :: (FromDynamo a) => IsForeign (DynamoDBRecord a) where
    read value = do
        creationTimeStamp <- readProp "ApproximateCreationDateTime" value
        keys <- readProp "Keys" value
        newImage <- dynamoRead =<< readProp "NewImage" value
        sequenceNumber <- readProp "SequenceNumber" value
        sizeBytes <- readProp "SizeBytes" value
        pure $ DynamoDBRecord
            { creationTimeStamp
            , keys
            , newImage
            , sequenceNumber
            , sizeBytes
            }

newtype StreamEvent a = StreamEvent
    { eventID :: String
    , eventName :: String -- INSERT
    , eventVersion :: String
    , eventSource :: String -- aws:dynamodb
    , awsRegion :: String -- us-west-2
    , dynamodb :: DynamoDBRecord a
    , eventSourceARN :: String
    }
derive instance genericStreamEvent :: Generic (StreamEvent a) _

instance isForeignStreamEvent :: (FromDynamo a) => IsForeign (StreamEvent a) where
    read = readGeneric defaultOptions{unwrapSingleConstructors=true}

newtype StreamPayload a = StreamPayload (Array (StreamEvent a))

instance streamPayloadIsForeign :: (FromDynamo a) => IsForeign (StreamPayload a) where
    read value = StreamPayload <$> readProp "Records" value

type PutParams a =
    { tablename :: Tablename a
    , item :: a
    -- TODO add ConditionExpression, Return{Values,ConsumedCapacity,ItemCollectionMetrics}
    }
newtype PutParams' a = PutParams' (PutParams a)

instance putParamsAsForeign :: (AsForeign a) => AsForeign (PutParams' a) where
    write (PutParams' {tablename, item}) = writeObject [ writeProp "TableName" tablename
                                                       , writeProp "Item" item]

voidForeign :: forall f. (Functor f) => f Foreign -> f Unit
voidForeign = void

put :: forall eff a. (AsForeign a) => PutParams a -> Dynaff eff Unit
put = voidForeign <<< invoke "put" <<< PutParams'


type QueryParams a hk sk =
    { tablename :: Tablename a
    , limit :: Maybe Int
    , order :: Order
    , keyCondition :: KeyExpr a hk sk
    , attrVals :: { keyAttr:: AttrVal a hk
                  , sortAttr :: Maybe (AttrVal a sk) }
    -- TODO add ProjectionExpression, ConsistentRead, FilterExpression, ExclusiveStartKey, ReturnConsumedCapacity, ExpressionAttributeNames
    }

defaultQuery :: forall a hk sk. (Table a hk sk) -> hk -> QueryParams a hk sk
defaultQuery table keyValue =
    { tablename : table.name
    , limit : Nothing
    , order : Ascending
    , keyCondition : KeyEq table.hashkey keyAttr Nothing
    , attrVals : {keyAttr, sortAttr : Nothing}
    }
    where
        keyAttr = hashAttr table.hashkey keyValue

newtype QueryParams' a hk sk = QueryParams' (QueryParams a hk sk)

instance queryParamsAsForeign :: (AsForeign hk, AsForeign sk) => AsForeign (QueryParams' a hk sk) where
    write (QueryParams' {tablename, limit, order, keyCondition, attrVals})
        = writeObject $ [ writeProp "TableName" tablename
                        , writeProp "ScanIndexForward" order
                        , writeProp "KeyConditionExpression" $ compileCondition keyCondition
                        , writeProp "ExpressionAttributeValues" $ attrValsToForeign attrVals
                        ] <> maybe [] (\l -> [writeProp "Limit" l]) limit


data QueryResult a = QueryResult
    { items :: Array a
    , count :: Int
    , scannedCount :: Int
    , lastEvaluatedKey :: Undefined a
    }

instance queryResultIsForeign :: (IsForeign a) => IsForeign (QueryResult a) where
    read value = do
        items <- readProp "Items" value
        count <- readProp "Count" value
        scannedCount <- readProp "ScannedCount" value
        lastEvaluatedKey <- readProp "LastEvaluatedKey" value
        pure $ QueryResult
            { items
            , count
            , scannedCount
            , lastEvaluatedKey
            }

query :: forall eff a hk sk. (IsForeign a, AsForeign hk, AsForeign sk) => QueryParams a hk sk -> Dynaff eff (QueryResult a)
query = invoke "query" <<< QueryParams'



